from __future__ import annotations

import asyncio
import logging
import re
from datetime import UTC, date, datetime
from typing import Any

from telegram import (
    BotCommandScopeAllPrivateChats,
    BotCommandScopeChat,
    CopyTextButton,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    Update,
)
from telegram.constants import ChatType
from telegram.error import RetryAfter
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackContext,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    Defaults,
    MessageHandler,
    filters,
)

from .config import Settings, load_settings
from .db import Customer, Database, Receipt
from .logic import (
    DEFAULT_PRICE_PER_DEVICE_RUB,
    RelativeExpiryPeriod,
    build_payment_amount_text,
    build_reminder_text,
    build_payment_details_text,
    calculate_payment_amount,
    format_date,
    format_relative_expiry,
    format_device_count,
    format_expiry_status,
    make_client_code,
    parse_client_code,
    parse_expiry_input,
    parse_relative_expiry,
)


LOGGER = logging.getLogger(__name__)
CALLBACK_RE = re.compile(r"^receipt:(?P<receipt_id>\d+):(?P<action>[a-z_]+)(?::(?P<value>\d+))?$")
ADMIN_PENDING_KEY = "admin_pending_action"
USER_AWAITING_RECEIPT_KEY = "awaiting_receipt"
LINK_CARD_SHOWN_KEY = "link_card_shown"


class VPNPaymentBot:
    def __init__(self, settings: Settings, database: Database) -> None:
        self.settings = settings
        self.db = database

    def build_application(self) -> Application:
        defaults = Defaults(tzinfo=self.settings.tzinfo)
        application = (
            ApplicationBuilder()
            .token(self.settings.bot_token)
            .connect_timeout(20.0)
            .read_timeout(30.0)
            .write_timeout(30.0)
            .pool_timeout(20.0)
            .get_updates_connect_timeout(20.0)
            .get_updates_read_timeout(30.0)
            .get_updates_write_timeout(30.0)
            .get_updates_pool_timeout(20.0)
            .defaults(defaults)
            .post_init(self.post_init)
            .post_shutdown(self.post_shutdown)
            .build()
        )

        application.add_handler(CommandHandler("start", self.start_command))
        application.add_handler(CommandHandler("help", self.help_command))
        application.add_handler(CommandHandler("status", self.status_command))
        application.add_handler(CommandHandler("id", self.id_command))
        application.add_handler(CommandHandler("price", self.price_command))
        application.add_handler(CommandHandler("pay", self.pay_command))
        application.add_handler(CommandHandler("whoami", self.whoami_command))
        application.add_handler(CommandHandler("admin", self.admin_command))
        application.add_handler(CommandHandler("clients", self.clients_command))
        application.add_handler(CommandHandler("client", self.client_command))
        application.add_handler(CommandHandler("pending", self.pending_command))
        application.add_handler(CommandHandler("link", self.link_command))
        application.add_handler(CommandHandler("setexpiry", self.setexpiry_command))
        application.add_handler(CommandHandler("extend", self.extend_command))
        application.add_handler(CommandHandler("setdevices", self.setdevices_command))
        application.add_handler(CommandHandler("setname", self.setname_command))
        application.add_handler(CommandHandler("mutereceipts", self.mute_receipts_command))
        application.add_handler(CommandHandler("deleteclient", self.delete_client_command))
        application.add_handler(CommandHandler("broadcast", self.broadcast_command))
        application.add_handler(CommandHandler("reject", self.reject_command))
        application.add_handler(CommandHandler("cancel", self.cancel_command))
        application.add_handler(CommandHandler("runreminders", self.run_reminders_command))
        application.add_handler(CallbackQueryHandler(self.receipt_callback, pattern=CALLBACK_RE))
        application.add_handler(
            MessageHandler(
                filters.TEXT & (~filters.COMMAND) & filters.ChatType.PRIVATE,
                self.private_text_message,
            )
        )
        application.add_handler(
            MessageHandler(
                (filters.PHOTO | filters.Document.ALL) & filters.ChatType.PRIVATE,
                self.receipt_message,
            )
        )
        application.add_error_handler(self.error_handler)

        return application

    async def post_init(self, application: Application) -> None:
        self.schedule_reminders(application)
        await self.sync_command_scopes(application)

    async def post_shutdown(self, _: Application) -> None:
        self.db.close()

    def schedule_reminders(self, application: Application) -> None:
        job_queue = application.job_queue
        if job_queue is None:
            LOGGER.warning("Job queue is unavailable; reminders will not run.")
            return

        job_queue.run_daily(
            self.reminder_job,
            time=self.settings.daily_reminder_time,
            name="daily-reminders",
        )
        job_queue.run_once(self.reminder_job, when=10, name="startup-reminder-scan")

    def resolve_admin_chat_id(self) -> int | None:
        if self.settings.admin_chat_id is not None:
            return self.settings.admin_chat_id

        raw = self.db.get_setting("admin_chat_id")
        return None if raw is None else int(raw)

    def is_admin_chat(self, chat_id: int | None) -> bool:
        admin_chat_id = self.resolve_admin_chat_id()
        return admin_chat_id is not None and admin_chat_id == chat_id

    def today(self) -> date:
        return datetime.now(self.settings.tzinfo).date()

    def client_code(self, user_id: int) -> str:
        return make_client_code(user_id)

    def is_customer_linked(self, customer: Customer | None) -> bool:
        return (
            customer is not None
            and customer.is_active
            and customer.subscription_expires_on is not None
        )

    def link_id_markup(self, client_code: str) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [[InlineKeyboardButton("Скопировать ID", copy_text=CopyTextButton(client_code))]]
        )

    async def sync_command_scopes(self, application: Application) -> None:
        await application.bot.set_my_commands(
            [
                ("start", "Открыть бота"),
                ("id", "Показать ID для привязки"),
            ],
            scope=BotCommandScopeAllPrivateChats(),
        )

        admin_chat_id = self.resolve_admin_chat_id()
        if admin_chat_id is not None:
            await application.bot.set_my_commands(
                [
                    ("clients", "Список клиентов"),
                    ("client", "Карточка клиента"),
                    ("pending", "Чеки на проверке"),
                    ("link", "Привязать клиента"),
                    ("setexpiry", "Поставить дату"),
                    ("extend", "Продлить подписку"),
                    ("setdevices", "Изменить число устройств клиента"),
                    ("setname", "Имя для админа"),
                    ("mutereceipts", "Отключить приём чеков"),
                    ("deleteclient", "Удалить клиента"),
                    ("broadcast", "Разослать сообщение всем"),
                    ("reject", "Отклонить чек"),
                    ("cancel", "Отменить ввод срока"),
                    ("runreminders", "Запустить напоминания"),
                ],
                scope=BotCommandScopeChat(admin_chat_id),
            )

    async def sync_customer_commands(self, application: Application, customer: Customer | None) -> None:
        if customer is None or customer.chat_id is None:
            return

        if not self.is_customer_linked(customer):
            await application.bot.delete_my_commands(scope=BotCommandScopeChat(customer.chat_id))
            return

        await application.bot.set_my_commands(
            [
                ("status", "Показать срок подписки"),
                ("price", "Количество устройств и цена"),
                ("pay", "Оплатить и отправить чек"),
                ("id", "Показать ID клиента"),
                ("whoami", "Показать user_id и client_id"),
            ],
            scope=BotCommandScopeChat(customer.chat_id),
        )

    async def send_link_id_card(
        self,
        message: Message,
        user_id: int,
        *,
        lead_text: str | None = None,
    ) -> None:
        client_code = self.client_code(user_id)
        parts = []
        if lead_text:
            parts.append(lead_text)
        parts.append(f"ID для привязки:\n{client_code}")
        await message.reply_text(
            "\n\n".join(parts),
            reply_markup=self.link_id_markup(client_code),
        )

    def sync_existing_customer(self, *, user_id: int, chat_id: int, username: str | None, full_name: str) -> Customer:
        return self.db.upsert_customer_profile(
            telegram_user_id=user_id,
            chat_id=chat_id,
            username=username,
            full_name=full_name,
        )

    def resolve_user_reference(self, raw: str) -> int | None:
        from_code = parse_client_code(raw)
        if from_code is not None:
            return from_code

        try:
            return int(raw)
        except ValueError:
            return None

    def customer_label(self, customer: Customer) -> str:
        username_part = f"@{customer.username}" if customer.username else "без username"
        display_name = self.admin_customer_name(customer)
        telegram_name_suffix = ""
        if customer.admin_name and customer.admin_name != customer.full_name:
            telegram_name_suffix = f", telegram_name={customer.full_name}"
        return (
            f"{display_name} ({username_part}, "
            f"client_id={self.client_code(customer.telegram_user_id)}, "
            f"user_id={customer.telegram_user_id}{telegram_name_suffix})"
        )

    def admin_customer_name(self, customer: Customer) -> str:
        return customer.admin_name or customer.full_name

    def admin_customer_name_with_telegram(self, customer: Customer) -> str:
        if customer.admin_name and customer.admin_name != customer.full_name:
            return f"{customer.admin_name} (Telegram: {customer.full_name})"
        return customer.full_name

    def build_link_success_text(self, expires_on: date) -> str:
        days_left = (expires_on - self.today()).days
        return (
            "Готово, привязка выполнена ✅\n\n"
            f"Подписка активна до {format_date(expires_on)}.\n"
            f"Осталось: {self.day_count_text(days_left)}."
        )

    def build_payment_confirmed_text(self) -> str:
        return "Платёж подтверждён ✅\nПодписка продлена."

    def receipt_keyboard(self, receipt_id: int) -> InlineKeyboardMarkup:
        receipt = self.db.get_receipt_by_id(receipt_id)
        customer = (
            self.db.get_customer_by_user_id(receipt.telegram_user_id)
            if receipt is not None
            else None
        )
        receipts_muted = bool(customer and customer.receipts_muted)
        mute_label = "🔔 Размутить чеки" if receipts_muted else "🔇 Заглушить чеки"
        mute_action = "unmute_receipts" if receipts_muted else "mute_receipts"
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("✅ +30 дней", callback_data=f"receipt:{receipt_id}:extend:30"),
                    InlineKeyboardButton("✅ +90 дней", callback_data=f"receipt:{receipt_id}:extend:90"),
                ],
                [
                    InlineKeyboardButton("🗓 Свой срок", callback_data=f"receipt:{receipt_id}:custom"),
                    InlineKeyboardButton("❌ Отклонить", callback_data=f"receipt:{receipt_id}:reject"),
                ],
                [
                    InlineKeyboardButton(mute_label, callback_data=f"receipt:{receipt_id}:{mute_action}"),
                ],
            ]
        )

    def payment_summary(self, customer: Customer) -> str:
        amount = calculate_payment_amount(customer.device_count)
        return f"{amount}₽ ({format_device_count(customer.device_count)})"

    def username_label(self, username: str | None) -> str:
        return f"@{username}" if username else "без username"

    def date_or_period_hint(self) -> str:
        return "DD.MM.YYYY, +30, +30d или +1m"

    def relative_period_hint(self) -> str:
        return "30, 30d, +30 или 1m"

    def day_count_text(self, days: int) -> str:
        return format_relative_expiry(RelativeExpiryPeriod(amount=days, unit="days"))

    def format_timestamp(self, raw: str | None) -> str:
        if not raw:
            return "—"

        try:
            value = datetime.fromisoformat(raw)
        except ValueError:
            return raw

        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        local_value = value.astimezone(self.settings.tzinfo)
        return local_value.strftime("%d.%m.%Y %H:%M")

    def actor_details(self, update: Update) -> tuple[int | None, str | None, str | None]:
        actor = update.effective_user
        if actor is None:
            return None, None, None
        return actor.id, actor.username, actor.full_name

    def record_customer_audit(self, update: Update, telegram_user_id: int, *, action: str, details: str) -> None:
        actor_user_id, actor_username, actor_full_name = self.actor_details(update)
        self.db.record_customer_audit(
            telegram_user_id=telegram_user_id,
            action=action,
            details=details,
            actor_user_id=actor_user_id,
            actor_username=actor_username,
            actor_full_name=actor_full_name,
        )

    def audit_actor_label(self, actor_full_name: str | None, actor_username: str | None, actor_user_id: int | None) -> str:
        name = actor_full_name or "Неизвестный администратор"
        if actor_username:
            return f"{name} (@{actor_username})"
        if actor_user_id is not None:
            return f"{name} ({actor_user_id})"
        return name

    def reminder_template_context(self, customer: Customer, expires_on: date, days_left: int) -> dict[str, Any]:
        overdue_days = abs(days_left)
        amount = calculate_payment_amount(customer.device_count)
        return {
            "customer_name": customer.full_name,
            "admin_customer_name": self.admin_customer_name(customer),
            "expires_on": format_date(expires_on),
            "expires_on_iso": expires_on.isoformat(),
            "days_left": days_left,
            "days_left_text": self.day_count_text(days_left if days_left > 0 else 0),
            "overdue_days": overdue_days,
            "overdue_text": self.day_count_text(overdue_days),
            "payment_details": build_payment_amount_text(customer.device_count),
            "payment_amount_line": build_payment_amount_text(customer.device_count),
            "amount": amount,
            "device_count": customer.device_count,
            "device_count_label": format_device_count(customer.device_count),
            "client_code": self.client_code(customer.telegram_user_id),
            "telegram_user_id": customer.telegram_user_id,
            "username": self.username_label(customer.username),
            "status": format_expiry_status(expires_on, self.today()),
            "access_disable_warning": "Если оплата не поступит, доступ будет отключен.",
        }

    def render_template(self, template: str, fallback: str, **values: Any) -> str:
        try:
            return template.format(**values)
        except Exception:
            LOGGER.exception("Failed to render reminder template")
            return fallback

    def build_customer_reminder_message(self, customer: Customer, expires_on: date, days_left: int) -> str:
        fallback = build_reminder_text(customer.full_name, expires_on, days_left, customer.device_count)
        if days_left > 0:
            template = self.settings.reminder_before_due_template
        elif days_left == 0:
            template = self.settings.reminder_due_today_template
        else:
            template = self.settings.reminder_overdue_template
        if not template:
            return fallback
        return self.render_template(
            template,
            fallback,
            **self.reminder_template_context(customer, expires_on, days_left),
        )

    def receipt_duplicate_text(self, receipt: Receipt) -> str:
        if receipt.duplicate_of_receipt_id is None:
            return "нет"
        original = self.db.get_receipt_by_id(receipt.duplicate_of_receipt_id)
        if original is None:
            return f"чек #{receipt.duplicate_of_receipt_id}"
        return f"чек #{original.id} ({original.status}, {self.format_timestamp(original.created_at)})"

    def render_receipt_review_message(self, receipt: Receipt) -> str:
        customer = self.db.get_customer_by_user_id(receipt.telegram_user_id)
        customer_name = (
            self.admin_customer_name(customer)
            if customer is not None
            else f"Клиент {receipt.telegram_user_id}"
        )
        username = self.username_label(customer.username if customer else None)
        status = format_expiry_status(
            customer.subscription_expires_on if customer else None,
            self.today(),
        )
        comment = receipt.caption or "Без подписи."
        linked_state = "да" if customer and customer.subscription_expires_on is not None else "пока нет"
        payment = self.payment_summary(customer) if customer is not None else "нет данных"
        receipt_state = "выключен" if customer and customer.receipts_muted else "включен"

        lines = [
            f"Чек #{receipt.id}",
            f"Клиент: {customer_name}",
            f"ID клиента: {self.client_code(receipt.telegram_user_id)}",
            f"Username: {username}",
            f"Telegram ID: {receipt.telegram_user_id}",
            f"Привязан к подписке: {linked_state}",
            f"Текущий срок: {status}",
            f"Сумма к оплате: {payment}",
            f"Прием чеков: {receipt_state}",
            f"Дубликат файла: {self.receipt_duplicate_text(receipt)}",
            f"Статус чека: {receipt.status}",
            f"Отправлен: {self.format_timestamp(receipt.created_at)}",
            f"Комментарий: {comment}",
        ]
        if customer and customer.admin_name and customer.admin_name != customer.full_name:
            lines.insert(2, f"Имя в Telegram: {customer.full_name}")
        if receipt.reviewed_at:
            lines.append(f"Обработан: {self.format_timestamp(receipt.reviewed_at)}")
        if receipt.review_note:
            lines.append(f"Комментарий проверки: {receipt.review_note}")
        return "\n".join(lines)

    def build_admin_overdue_reminder_text(self, customer: Customer) -> str:
        assert customer.subscription_expires_on is not None
        telegram_name_line = ""
        if customer.admin_name and customer.admin_name != customer.full_name:
            telegram_name_line = f"\nИмя в Telegram: {customer.full_name}"
        fallback = (
            "⚠️💳 Просроченная подписка\n\n"
            f"Клиент: {self.admin_customer_name(customer)}"
            f"{telegram_name_line}\n"
            f"Username: {self.username_label(customer.username)}\n"
            f"ID клиента: {self.client_code(customer.telegram_user_id)}\n"
            f"Telegram ID: {customer.telegram_user_id}\n"
            f"Статус: {format_expiry_status(customer.subscription_expires_on, self.today())}"
        )
        template = self.settings.admin_overdue_reminder_template
        if not template:
            return fallback
        return self.render_template(
            template,
            fallback,
            **self.reminder_template_context(
                customer,
                customer.subscription_expires_on,
                -abs((customer.subscription_expires_on - self.today()).days),
            ),
        )

    async def reject_receipt(
        self,
        application: Application,
        *,
        receipt: Receipt,
        reason: str,
    ) -> None:
        self.db.mark_receipt_status(receipt.id, "rejected", reason)
        await self.refresh_receipt_review_message(application, receipt.id)

        customer = self.db.get_customer_by_user_id(receipt.telegram_user_id)
        if customer and customer.chat_id is not None:
            if reason == "Чек отклонен.":
                text = "К сожалению, чек отклонен 😕"
            else:
                text = f"К сожалению, чек отклонен 😕\n\nПричина: {reason}"
            await application.bot.send_message(chat_id=customer.chat_id, text=text)

    async def notify_customer_expiry(
        self,
        application: Application,
        customer: Customer,
        reason: str,
        *,
        include_status: bool = True,
    ) -> None:
        if customer.chat_id is None:
            return

        await self.sync_customer_commands(application, customer)
        text = reason
        if include_status:
            status = format_expiry_status(customer.subscription_expires_on, self.today())
            text = f"{reason}\n\n{status}"
        await application.bot.send_message(chat_id=customer.chat_id, text=text)

    async def apply_expiry_update(
        self,
        application: Application,
        *,
        user_id: int,
        new_expiry: date,
        customer_message: str,
        include_status: bool = True,
        receipt: Receipt | None = None,
        review_note: str | None = None,
    ) -> Customer:
        updated_customer = self.db.set_subscription_expiry(user_id, new_expiry)
        if receipt is not None and receipt.status == "pending":
            self.db.mark_receipt_status(receipt.id, "approved", review_note)
            await self.refresh_receipt_review_message(application, receipt.id)

        await self.notify_customer_expiry(
            application,
            updated_customer,
            customer_message,
            include_status=include_status,
        )
        return updated_customer

    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.effective_message
        user = update.effective_user
        chat = update.effective_chat
        if message is None or user is None or chat is None:
            return

        if self.is_admin_chat(chat.id):
            await message.reply_text(
                "Панель администратора активна."
            )
            return

        customer = self.sync_existing_customer(
            user_id=user.id,
            chat_id=chat.id,
            username=user.username,
            full_name=user.full_name,
        )
        await self.sync_customer_commands(context.application, customer)
        if not self.is_customer_linked(customer):
            context.user_data[LINK_CARD_SHOWN_KEY] = True
            await self.send_link_id_card(message, user.id)
            return

        await message.reply_text(format_expiry_status(customer.subscription_expires_on, self.today()))

    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.effective_message
        chat = update.effective_chat
        if message is None or chat is None:
            return

        if self.is_admin_chat(chat.id):
            await message.reply_text(
                "Команды администратора:\n"
                "/clients - показать привязанных клиентов\n"
                "/client <client_id|user_id> - подробная карточка клиента\n"
                "/pending - показать чеки на проверке\n"
                "/link <client_id> <DD.MM.YYYY|+30|+1m> - привязать клиента и задать дату или срок\n"
                "/setexpiry <client_id|user_id> <DD.MM.YYYY|+30|+1m> - поставить дату или срок\n"
                "/extend <client_id|user_id> <30|1m> - продлить подписку на дни или месяцы\n"
                "/setdevices <client_id|user_id> <count> - изменить число устройств клиента\n"
                "/setname <client_id|user_id> <имя|-> - задать имя клиента для админа\n"
                "/mutereceipts <client_id|user_id> <on|off> - включить или выключить прием чеков\n"
                "/deleteclient <client_id|user_id> - убрать клиента из активных\n"
                "/broadcast - ответом на сообщение разослать его всем пользователям\n"
                "/reject <причина> - ответом на карточку чека отклонить оплату\n"
                "/cancel - отменить ввод своего срока\n"
                "/runreminders - запустить проверку напоминаний"
            )
            return

        user = update.effective_user
        if user is None:
            return

        customer = self.sync_existing_customer(
            user_id=user.id,
            chat_id=chat.id,
            username=user.username,
            full_name=user.full_name,
        )
        await self.sync_customer_commands(context.application, customer)
        if not self.is_customer_linked(customer):
            await self.send_link_id_card(message, user.id)
            return

        await message.reply_text(
            "Команды клиента:\n"
            "/status - посмотреть срок подписки\n"
            "/price - посмотреть число устройств и сумму\n"
            "/pay - получить реквизиты и отправить чек\n"
            "/id - показать ID клиента\n"
            "/whoami - показать user_id и client_id"
        )

    async def status_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.effective_message
        user = update.effective_user
        chat = update.effective_chat
        if message is None or user is None or chat is None:
            return

        customer = self.sync_existing_customer(
            user_id=user.id,
            chat_id=chat.id,
            username=user.username,
            full_name=user.full_name,
        )
        await self.sync_customer_commands(context.application, customer)
        if not self.is_customer_linked(customer):
            await self.send_link_id_card(message, user.id)
            return

        await message.reply_text(format_expiry_status(customer.subscription_expires_on, self.today()))

    async def id_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.effective_message
        user = update.effective_user
        if message is None or user is None:
            return

        await self.send_link_id_card(message, user.id)

    async def price_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.effective_message
        user = update.effective_user
        chat = update.effective_chat
        if message is None or user is None or chat is None:
            return

        customer = self.sync_existing_customer(
            user_id=user.id,
            chat_id=chat.id,
            username=user.username,
            full_name=user.full_name,
        )
        await self.sync_customer_commands(context.application, customer)
        if not self.is_customer_linked(customer):
            await self.send_link_id_card(
                message,
                user.id,
                lead_text="Сначала отправьте этот ID администратору.",
            )
            return

        await message.reply_text(
            "Стоимость подписки:\n"
            f"Устройства: {format_device_count(customer.device_count)}\n"
            f"Итого: {calculate_payment_amount(customer.device_count)}₽\n"
            f"Тариф: {DEFAULT_PRICE_PER_DEVICE_RUB}₽ за 1 устройство.\n\n"
            "Для оплаты используйте /pay."
        )

    async def pay_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.effective_message
        user = update.effective_user
        chat = update.effective_chat
        if message is None or user is None or chat is None:
            return

        customer = self.sync_existing_customer(
            user_id=user.id,
            chat_id=chat.id,
            username=user.username,
            full_name=user.full_name,
        )
        await self.sync_customer_commands(context.application, customer)
        if not self.is_customer_linked(customer):
            context.user_data[LINK_CARD_SHOWN_KEY] = True
            await self.send_link_id_card(
                message,
                user.id,
                lead_text="Сначала отправьте этот ID администратору.",
            )
            return

        if customer.receipts_muted:
            context.user_data.pop(USER_AWAITING_RECEIPT_KEY, None)
            await message.reply_text("Прием чеков для вас временно отключен. Напишите администратору.")
            return

        context.user_data[USER_AWAITING_RECEIPT_KEY] = True
        await message.reply_text(
            "Реквизиты для оплаты:\n"
            f"{build_payment_details_text(customer.device_count, self.settings.payment_destination_text)}\n\n"
            "После перевода пришлите чек следующим сообщением."
        )

    async def whoami_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.effective_message
        chat = update.effective_chat
        user = update.effective_user
        if message is None or chat is None or user is None:
            return

        if self.is_admin_chat(chat.id):
            await message.reply_text("Команда доступна только клиенту.")
            return

        lines = [f"user_id={user.id}", f"client_id={self.client_code(user.id)}"]
        if user.username:
            lines.append(f"username=@{user.username}")
        await message.reply_text("\n".join(lines))

    async def admin_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.effective_message
        chat = update.effective_chat
        if message is None or chat is None:
            return

        if self.settings.admin_chat_id is not None:
            await message.reply_text("ADMIN_CHAT_ID уже задан в .env, этот чат менять не нужно.")
            return

        if chat.type != ChatType.PRIVATE:
            await message.reply_text("Назначать админ-чат нужно в личке с ботом.")
            return

        if not context.args:
            await message.reply_text("Использование: /admin <ADMIN_CODE>")
            return

        provided_code = context.args[0].strip()
        if provided_code != self.settings.admin_code:
            await message.reply_text("Неверный код администратора.")
            return

        self.db.set_setting("admin_chat_id", str(chat.id))
        await self.sync_command_scopes(context.application)
        await message.reply_text(f"Готово ✅\nТеперь этот чат админский.\nchat_id={chat.id}")

    async def clients_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.effective_message
        chat = update.effective_chat
        if message is None or chat is None or not self.is_admin_chat(chat.id):
            return

        customers = self.db.list_customers(limit=50)
        if not customers:
            await message.reply_text(
                "Пока нет привязанных клиентов.\n\n"
                "Попросите человека прислать свой ID клиента и выполните /link <client_id> <DD.MM.YYYY|+30|+1m>."
            )
            return

        today = self.today()
        lines = []
        for customer in customers:
            lines.append(
                f"- {self.admin_customer_name_with_telegram(customer)} | {self.client_code(customer.telegram_user_id)} | "
                f"{format_expiry_status(customer.subscription_expires_on, today)} | "
                f"{self.payment_summary(customer)} | "
                f"чеки {'выкл' if customer.receipts_muted else 'вкл'}"
            )
        await message.reply_text("\n".join(lines))

    async def client_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.effective_message
        chat = update.effective_chat
        if message is None or chat is None or not self.is_admin_chat(chat.id):
            return

        resolved = self.resolve_command_target(message, context.args)
        if resolved is None:
            await message.reply_text(
                "Использование: /client <client_id|user_id>\n"
                "Или ответом на карточку чека: /client"
            )
            return

        user_id, args, _ = resolved
        if args:
            await message.reply_text("Использование: /client <client_id|user_id>")
            return

        customer = self.db.get_customer_by_user_id(user_id)
        if customer is None:
            await message.reply_text("Клиент не найден.")
            return

        username = self.username_label(customer.username)
        state = "активен" if customer.is_active else "удален"
        receipts_state = "выключен" if customer.receipts_muted else "включен"
        lines = [
            f"Клиент: {self.admin_customer_name(customer)}",
            f"Username: {username}",
            f"ID клиента: {self.client_code(customer.telegram_user_id)}",
            f"Telegram ID: {customer.telegram_user_id}",
            f"Статус клиента: {state}",
            f"Срок: {format_expiry_status(customer.subscription_expires_on, self.today())}",
            f"Оплата: {self.payment_summary(customer)}",
            f"Прием чеков: {receipts_state}",
            f"Создан: {self.format_timestamp(customer.created_at)}",
            f"Обновлен: {self.format_timestamp(customer.updated_at)}",
        ]
        if customer.admin_name and customer.admin_name != customer.full_name:
            lines.insert(1, f"Имя в Telegram: {customer.full_name}")

        receipts = self.db.list_receipts_for_user(customer.telegram_user_id, limit=5)
        if receipts:
            lines.append("")
            lines.append("Последние чеки:")
            for receipt in receipts:
                duplicate_suffix = (
                    f" | дубль {self.receipt_duplicate_text(receipt)}"
                    if receipt.duplicate_of_receipt_id is not None
                    else ""
                )
                lines.append(
                    f"- #{receipt.id} | {receipt.status} | {self.format_timestamp(receipt.created_at)}{duplicate_suffix}"
                )
        else:
            lines.append("")
            lines.append("Последние чеки: нет")

        audit_entries = self.db.list_customer_audit_entries(customer.telegram_user_id, limit=5)
        if audit_entries:
            lines.append("")
            lines.append("Последние изменения:")
            for entry in audit_entries:
                lines.append(
                    f"- {self.format_timestamp(entry.created_at)} | "
                    f"{self.audit_actor_label(entry.actor_full_name, entry.actor_username, entry.actor_user_id)} | "
                    f"{entry.action} | {entry.details}"
                )
        else:
            lines.append("")
            lines.append("Последние изменения: нет")

        await message.reply_text("\n".join(lines))

    async def pending_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.effective_message
        chat = update.effective_chat
        if message is None or chat is None or not self.is_admin_chat(chat.id):
            return

        receipts = self.db.list_pending_receipts(limit=20)
        if not receipts:
            await message.reply_text("Чеков на проверке сейчас нет.")
            return

        lines = []
        for receipt in receipts:
            customer = self.db.get_customer_by_user_id(receipt.telegram_user_id)
            customer_name = (
                self.admin_customer_name_with_telegram(customer)
                if customer
                else f"Клиент {receipt.telegram_user_id}"
            )
            duplicate_suffix = (
                f" | дубль {self.receipt_duplicate_text(receipt)}"
                if receipt.duplicate_of_receipt_id is not None
                else ""
            )
            lines.append(
                f"- Чек #{receipt.id} | {customer_name} | "
                f"{self.client_code(receipt.telegram_user_id)} | {self.format_timestamp(receipt.created_at)}"
                f"{duplicate_suffix}"
            )
        await message.reply_text("\n".join(lines))

    def resolve_command_target(self, message: Message, args: list[str]) -> tuple[int, list[str], Receipt | None] | None:
        admin_chat_id = self.resolve_admin_chat_id()
        if admin_chat_id is None:
            return None

        if message.reply_to_message is not None:
            receipt = self.db.get_receipt_by_admin_message(admin_chat_id, message.reply_to_message.message_id)
            if receipt is not None:
                return receipt.telegram_user_id, args, receipt

        if not args:
            return None

        user_id = self.resolve_user_reference(args[0])
        if user_id is None:
            return None
        return user_id, args[1:], None

    async def link_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.effective_message
        chat = update.effective_chat
        if message is None or chat is None or not self.is_admin_chat(chat.id):
            return

        if len(context.args) != 2:
            await message.reply_text(
                f"Использование: /link <client_id> <{self.date_or_period_hint()}>"
            )
            return

        user_id = self.resolve_user_reference(context.args[0])
        if user_id is None:
            await message.reply_text("Не удалось распознать client_id. Пример: VPN-21I3V9")
            return

        customer = self.db.get_customer_by_user_id(user_id)
        try:
            new_expiry = parse_expiry_input(
                context.args[1],
                customer.subscription_expires_on if customer else None,
                self.today(),
            )
        except ValueError:
            await message.reply_text(
                f"Неверный формат. Используйте {self.date_or_period_hint()}."
            )
            return

        updated_customer = await self.apply_expiry_update(
            context.application,
            user_id=user_id,
            new_expiry=new_expiry,
            customer_message=self.build_link_success_text(new_expiry),
            include_status=False,
        )
        self.record_customer_audit(
            update,
            updated_customer.telegram_user_id,
            action="link",
            details=f"Клиент привязан, срок установлен до {format_date(new_expiry)}.",
        )
        await message.reply_text(
            f"Клиент привязан: {self.customer_label(updated_customer)}\n"
            f"Новый срок: {format_date(new_expiry)}"
        )

    async def setexpiry_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.effective_message
        chat = update.effective_chat
        if message is None or chat is None or not self.is_admin_chat(chat.id):
            return

        resolved = self.resolve_command_target(message, context.args)
        if resolved is None:
            await message.reply_text(
                f"Использование: /setexpiry <client_id|user_id> <{self.date_or_period_hint()}>\n"
                f"Или ответом на карточку чека: /setexpiry <{self.date_or_period_hint()}>"
            )
            return

        user_id, args, receipt = resolved
        if len(args) != 1:
            await message.reply_text(
                f"Использование: /setexpiry <client_id|user_id> <{self.date_or_period_hint()}>"
            )
            return

        customer = self.db.get_customer_by_user_id(user_id)
        try:
            new_expiry = parse_expiry_input(
                args[0],
                customer.subscription_expires_on if customer else None,
                self.today(),
            )
        except ValueError:
            await message.reply_text(
                f"Неверный формат. Используйте {self.date_or_period_hint()}."
            )
            return

        updated_customer = await self.apply_expiry_update(
            context.application,
            user_id=user_id,
            new_expiry=new_expiry,
            receipt=receipt,
            review_note=f"Установлена дата {format_date(new_expiry)}",
            customer_message="Срок подписки изменен ✅",
        )
        details = f"Срок подписки изменен на {format_date(new_expiry)}."
        if receipt is not None:
            details = f"{details} Через чек #{receipt.id}."
        self.record_customer_audit(
            update,
            updated_customer.telegram_user_id,
            action="setexpiry",
            details=details,
        )
        await message.reply_text(
            f"Срок обновлен: {self.customer_label(updated_customer)}\n"
            f"Новая дата: {format_date(new_expiry)}"
        )

    async def extend_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.effective_message
        chat = update.effective_chat
        if message is None or chat is None or not self.is_admin_chat(chat.id):
            return

        resolved = self.resolve_command_target(message, context.args)
        if resolved is None:
            await message.reply_text(
                f"Использование: /extend <client_id|user_id> <{self.relative_period_hint()}>\n"
                f"Или ответом на карточку чека: /extend <{self.relative_period_hint()}>"
            )
            return

        user_id, args, receipt = resolved
        if len(args) != 1:
            await message.reply_text(
                f"Использование: /extend <client_id|user_id> <{self.relative_period_hint()}>"
            )
            return

        raw_period = args[0].strip()
        normalized_period = raw_period if raw_period.startswith("+") else f"+{raw_period}"
        relative_period = parse_relative_expiry(normalized_period)
        if relative_period is None:
            await message.reply_text(
                f"Не понял срок. Используйте {self.relative_period_hint()}."
            )
            return

        if relative_period.amount <= 0:
            await message.reply_text("Срок должен быть больше нуля.")
            return

        customer = self.db.get_customer_by_user_id(user_id)
        new_expiry = parse_expiry_input(
            normalized_period,
            customer.subscription_expires_on if customer else None,
            self.today(),
        )
        updated_customer = await self.apply_expiry_update(
            context.application,
            user_id=user_id,
            new_expiry=new_expiry,
            receipt=receipt,
            review_note=f"Продлено на {format_relative_expiry(relative_period)}",
            customer_message=self.build_payment_confirmed_text(),
        )
        details = (
            f"Подписка продлена на {format_relative_expiry(relative_period)} "
            f"до {format_date(new_expiry)}."
        )
        if receipt is not None:
            details = f"{details} Через чек #{receipt.id}."
        self.record_customer_audit(
            update,
            updated_customer.telegram_user_id,
            action="extend",
            details=details,
        )

        await message.reply_text(
            f"Подписка продлена: {self.customer_label(updated_customer)}\n"
            f"Новая дата: {format_date(new_expiry)}"
        )

    async def setdevices_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.effective_message
        chat = update.effective_chat
        if message is None or chat is None or not self.is_admin_chat(chat.id):
            return

        resolved = self.resolve_command_target(message, context.args)
        if resolved is None:
            await message.reply_text(
                "Использование: /setdevices <client_id|user_id> <count>\n"
                "Или ответом на карточку чека: /setdevices <count>"
            )
            return

        user_id, args, _ = resolved
        if len(args) != 1:
            await message.reply_text("Использование: /setdevices <client_id|user_id> <count>")
            return

        try:
            device_count = int(args[0])
        except ValueError:
            await message.reply_text("Количество устройств должно быть числом.")
            return

        if device_count <= 0:
            await message.reply_text("Количество устройств должно быть больше нуля.")
            return

        previous_customer = self.db.get_customer_by_user_id(user_id)
        updated_customer = self.db.set_customer_device_count(user_id, device_count)
        previous_devices = previous_customer.device_count if previous_customer is not None else 1
        previous_amount = calculate_payment_amount(previous_devices)
        new_amount = calculate_payment_amount(updated_customer.device_count)
        self.record_customer_audit(
            update,
            updated_customer.telegram_user_id,
            action="setdevices",
            details=(
                f"Количество устройств: {previous_devices} -> {updated_customer.device_count}; "
                f"сумма: {previous_amount}₽ -> {new_amount}₽."
            ),
        )
        await message.reply_text(
            f"Количество устройств обновлено: {self.customer_label(updated_customer)}\n"
            f"Новая сумма: {self.payment_summary(updated_customer)}"
        )

    async def setname_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.effective_message
        chat = update.effective_chat
        if message is None or chat is None or not self.is_admin_chat(chat.id):
            return

        resolved = self.resolve_command_target(message, context.args)
        if resolved is None:
            await message.reply_text(
                "Использование: /setname <client_id|user_id> <имя>\n"
                "Или ответом на карточку чека: /setname <имя>\n"
                "Чтобы очистить имя, используйте /setname <client_id|user_id> -"
            )
            return

        user_id, args, _ = resolved
        if not args:
            await message.reply_text("Использование: /setname <client_id|user_id> <имя|->")
            return

        raw_name = " ".join(args).strip()
        previous_customer = self.db.get_customer_by_user_id(user_id)
        admin_name = None if raw_name in {"-", "clear", "none"} else raw_name
        updated_customer = self.db.set_customer_admin_name(user_id, admin_name)
        previous_name = previous_customer.admin_name if previous_customer and previous_customer.admin_name else "—"
        new_name = updated_customer.admin_name if updated_customer.admin_name else "—"
        self.record_customer_audit(
            update,
            updated_customer.telegram_user_id,
            action="setname",
            details=f"Имя для администратора: {previous_name} -> {new_name}.",
        )
        await message.reply_text(
            f"Имя для администратора обновлено: {self.customer_label(updated_customer)}"
        )

    def parse_toggle_value(self, raw: str) -> bool | None:
        value = raw.strip().lower()
        if value in {"on", "true", "1", "yes", "mute", "muted"}:
            return True
        if value in {"off", "false", "0", "no", "unmute", "unmuted"}:
            return False
        return None

    def retry_after_seconds(self, retry_after: Any) -> float:
        if hasattr(retry_after, "total_seconds"):
            return max(float(retry_after.total_seconds()), 1.0)
        return max(float(retry_after), 1.0)

    async def copy_message_with_retry(self, message: Message, *, chat_id: int, attempts: int = 3) -> None:
        for attempt in range(attempts):
            try:
                await message.copy(chat_id=chat_id)
                return
            except RetryAfter as exc:
                if attempt >= attempts - 1:
                    raise
                await asyncio.sleep(self.retry_after_seconds(exc.retry_after) + 0.5)

    async def mute_receipts_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.effective_message
        chat = update.effective_chat
        if message is None or chat is None or not self.is_admin_chat(chat.id):
            return

        resolved = self.resolve_command_target(message, context.args)
        if resolved is None:
            await message.reply_text(
                "Использование: /mutereceipts <client_id|user_id> <on|off>\n"
                "Или ответом на карточку чека: /mutereceipts <on|off>"
            )
            return

        user_id, args, _ = resolved
        if len(args) != 1:
            await message.reply_text("Использование: /mutereceipts <client_id|user_id> <on|off>")
            return

        muted = self.parse_toggle_value(args[0])
        if muted is None:
            await message.reply_text("Используйте on/off.")
            return

        previous_customer = self.db.get_customer_by_user_id(user_id)
        updated_customer = self.db.set_customer_receipts_muted(user_id, muted)
        state = "выключен" if updated_customer.receipts_muted else "включен"
        previous_state = (
            "выключен"
            if previous_customer is not None and previous_customer.receipts_muted
            else "включен"
        )
        self.record_customer_audit(
            update,
            updated_customer.telegram_user_id,
            action="mutereceipts",
            details=f"Прием чеков: {previous_state} -> {state}.",
        )
        await message.reply_text(
            f"Прием чеков обновлен: {self.customer_label(updated_customer)}\n"
            f"Новый статус: {state}."
        )

    async def delete_client_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.effective_message
        chat = update.effective_chat
        if message is None or chat is None or not self.is_admin_chat(chat.id):
            return

        resolved = self.resolve_command_target(message, context.args)
        if resolved is None:
            await message.reply_text(
                "Использование: /deleteclient <client_id|user_id>\n"
                "Или ответом на карточку чека: /deleteclient"
            )
            return

        user_id, args, _ = resolved
        if args:
            await message.reply_text("Использование: /deleteclient <client_id|user_id>")
            return

        updated_customer = self.db.deactivate_customer(user_id)
        if updated_customer is None:
            await message.reply_text("Клиент не найден.")
            return

        await self.sync_customer_commands(context.application, updated_customer)
        self.record_customer_audit(
            update,
            updated_customer.telegram_user_id,
            action="deleteclient",
            details="Клиент удален из активных, срок очищен.",
        )
        await message.reply_text(f"Клиент удален из активных: {self.customer_label(updated_customer)}")

    async def broadcast_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.effective_message
        chat = update.effective_chat
        if message is None or chat is None or not self.is_admin_chat(chat.id):
            return

        source_message = message.reply_to_message
        if source_message is None:
            await message.reply_text(
                "Ответьте командой /broadcast на сообщение в админ-чате, и бот разошлет его всем пользователям."
            )
            return

        recipients = self.db.list_broadcast_recipients(exclude_chat_id=chat.id)
        if not recipients:
            await message.reply_text(
                "Для рассылки пока нет получателей. Бот может писать только тем, кто уже открывал с ним диалог."
            )
            return

        sent_count = 0
        failed_count = 0
        failed_labels: list[str] = []
        for customer in recipients:
            if customer.chat_id is None:
                continue

            try:
                await self.copy_message_with_retry(source_message, chat_id=customer.chat_id)
                sent_count += 1
            except Exception as exc:
                failed_count += 1
                failed_labels.append(f"{self.client_code(customer.telegram_user_id)} ({exc})")
                LOGGER.exception(
                    "Failed to broadcast message to customer %s",
                    customer.telegram_user_id,
                )

        lines = [
            "Рассылка завершена.",
            f"Получателей: {len(recipients)}",
            f"Успешно: {sent_count}",
            f"Ошибок: {failed_count}",
        ]
        if failed_labels:
            lines.append("Не доставлено: " + ", ".join(failed_labels[:10]))
            if len(failed_labels) > 10:
                lines.append(f"И еще ошибок: {len(failed_labels) - 10}")
        await message.reply_text("\n".join(lines))

    async def reject_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.effective_message
        chat = update.effective_chat
        if message is None or chat is None or not self.is_admin_chat(chat.id):
            return

        if message.reply_to_message is None:
            await message.reply_text("Ответьте этой командой на карточку чека: /reject <причина>")
            return

        admin_chat_id = self.resolve_admin_chat_id()
        if admin_chat_id is None:
            return

        receipt = self.db.get_receipt_by_admin_message(admin_chat_id, message.reply_to_message.message_id)
        if receipt is None:
            await message.reply_text("Не нашел чек, к которому относится это сообщение.")
            return

        if receipt.status != "pending":
            await message.reply_text("Этот чек уже обработан.")
            return

        reason = " ".join(context.args).strip() or "Чек отклонен."
        await self.reject_receipt(context.application, receipt=receipt, reason=reason)
        await message.reply_text("Чек отклонен.")

    async def cancel_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.effective_message
        chat = update.effective_chat
        if message is None or chat is None or not self.is_admin_chat(chat.id):
            return

        if context.chat_data.pop(ADMIN_PENDING_KEY, None) is None:
            await message.reply_text("Сейчас нет действия, которое нужно отменять.")
            return

        await message.reply_text("Окей, отменил ввод срока.")

    async def run_reminders_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.effective_message
        chat = update.effective_chat
        if message is None or chat is None or not self.is_admin_chat(chat.id):
            return

        sent_count = await self.process_reminders(context.application)
        await message.reply_text(f"Готово. Отправлено напоминаний: {sent_count}.")

    async def private_text_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.effective_message
        user = update.effective_user
        chat = update.effective_chat
        if message is None or user is None or chat is None:
            return

        if self.is_admin_chat(chat.id):
            pending = context.chat_data.get(ADMIN_PENDING_KEY)
            if pending is not None:
                await self.handle_pending_admin_action(update, context, pending)
            return

        customer = self.sync_existing_customer(
            user_id=user.id,
            chat_id=chat.id,
            username=user.username,
            full_name=user.full_name,
        )
        if context.user_data.get(USER_AWAITING_RECEIPT_KEY):
            if not self.is_customer_linked(customer):
                context.user_data.pop(USER_AWAITING_RECEIPT_KEY, None)
                context.user_data[LINK_CARD_SHOWN_KEY] = True
                await self.send_link_id_card(
                    message,
                    user.id,
                    lead_text="Сначала отправьте этот ID администратору.",
                )
                return
            await self.receipt_message(update, context)
            return

        if not self.is_customer_linked(customer):
            if context.user_data.get(LINK_CARD_SHOWN_KEY):
                return
            context.user_data[LINK_CARD_SHOWN_KEY] = True
            await self.send_link_id_card(message, user.id)

    async def handle_pending_admin_action(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        pending: dict[str, Any],
    ) -> None:
        message = update.effective_message
        if message is None:
            return

        receipt = self.db.get_receipt_by_id(int(pending["receipt_id"]))
        if receipt is None:
            context.chat_data.pop(ADMIN_PENDING_KEY, None)
            await message.reply_text("Чек больше не найден.")
            return

        if receipt.status != "pending":
            context.chat_data.pop(ADMIN_PENDING_KEY, None)
            await message.reply_text("Этот чек уже обработан.")
            return

        action = str(pending.get("action") or "")
        if action == "custom_expiry":
            customer = self.db.get_customer_by_user_id(receipt.telegram_user_id)
            try:
                new_expiry = parse_expiry_input(
                    message.text or "",
                    customer.subscription_expires_on if customer else None,
                    self.today(),
                )
            except ValueError:
                await message.reply_text(
                    f"Не понял формат. Пришлите {self.date_or_period_hint()}. Для отмены используйте /cancel."
                )
                return

            updated_customer = await self.apply_expiry_update(
                context.application,
                user_id=receipt.telegram_user_id,
                new_expiry=new_expiry,
                receipt=receipt,
                review_note=f"Установлено вручную: {format_date(new_expiry)}",
                customer_message="Срок подписки изменен ✅",
            )
            self.record_customer_audit(
                update,
                updated_customer.telegram_user_id,
                action="setexpiry",
                details=(
                    f"Срок подписки изменен на {format_date(new_expiry)} "
                    f"через чек #{receipt.id}."
                ),
            )
            context.chat_data.pop(ADMIN_PENDING_KEY, None)
            await message.reply_text(
                f"Готово. Новый срок для {self.customer_label(updated_customer)}: {format_date(new_expiry)}."
            )
            return

        if action == "reject_receipt":
            raw_reason = (message.text or "").strip()
            if not raw_reason:
                await message.reply_text(
                    "Пришлите комментарий к отклонению. Если комментарий не нужен, отправьте -."
                )
                return

            reason = "Чек отклонен." if raw_reason == "-" else raw_reason
            await self.reject_receipt(context.application, receipt=receipt, reason=reason)
            context.chat_data.pop(ADMIN_PENDING_KEY, None)
            await message.reply_text("Чек отклонен.")
            return

        context.chat_data.pop(ADMIN_PENDING_KEY, None)
        await message.reply_text("Неизвестное действие отменено.")

    async def receipt_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.effective_message
        user = update.effective_user
        chat = update.effective_chat
        if message is None or user is None or chat is None:
            return

        if self.is_admin_chat(chat.id):
            return

        admin_chat_id = self.resolve_admin_chat_id()
        if admin_chat_id is None:
            await message.reply_text("Админ-чат пока не настроен. Попробуйте чуть позже.")
            return

        customer = self.sync_existing_customer(
            user_id=user.id,
            chat_id=chat.id,
            username=user.username,
            full_name=user.full_name,
        )
        await self.sync_customer_commands(context.application, customer)
        if not self.is_customer_linked(customer):
            context.user_data.pop(USER_AWAITING_RECEIPT_KEY, None)
            context.user_data[LINK_CARD_SHOWN_KEY] = True
            await self.send_link_id_card(
                message,
                user.id,
                lead_text="Сначала отправьте этот ID администратору.",
            )
            return

        if customer.receipts_muted:
            context.user_data.pop(USER_AWAITING_RECEIPT_KEY, None)
            await message.reply_text("Прием чеков для вас временно отключен. Напишите администратору.")
            return

        kind = "text"
        file_id = None
        file_unique_id = None
        caption = message.caption or message.text

        if message.photo:
            kind = "photo"
            file_id = message.photo[-1].file_id
            file_unique_id = message.photo[-1].file_unique_id
        elif message.document:
            kind = "document"
            file_id = message.document.file_id
            file_unique_id = message.document.file_unique_id
        elif not context.user_data.get(USER_AWAITING_RECEIPT_KEY):
            return

        receipt = self.db.create_receipt(
            telegram_user_id=user.id,
            customer_id=customer.id,
            source_chat_id=chat.id,
            source_message_id=message.message_id,
            kind=kind,
            caption=caption,
            file_id=file_id,
            file_unique_id=file_unique_id,
        )

        copied = await message.copy(chat_id=admin_chat_id)
        review_message = await context.application.bot.send_message(
            chat_id=admin_chat_id,
            text=self.render_receipt_review_message(receipt),
            reply_to_message_id=copied.message_id,
            reply_markup=self.receipt_keyboard(receipt.id),
        )
        self.db.attach_admin_messages(
            receipt_id=receipt.id,
            admin_chat_id=admin_chat_id,
            admin_copy_message_id=copied.message_id,
            admin_control_message_id=review_message.message_id,
        )

        context.user_data.pop(USER_AWAITING_RECEIPT_KEY, None)
        duplicate_notice = ""
        if receipt.duplicate_of_receipt_id is not None:
            duplicate_notice = (
                f"\n\nПохожий файл уже отправлялся раньше: {self.receipt_duplicate_text(receipt)}."
            )
        await message.reply_text(f"Чек отправлен администратору на проверку.{duplicate_notice}")

    async def receipt_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        query = update.callback_query
        chat = update.effective_chat
        if query is None or chat is None:
            return

        if not self.is_admin_chat(chat.id):
            await query.answer("Только для администратора.", show_alert=True)
            return

        match = CALLBACK_RE.fullmatch(query.data or "")
        if match is None:
            await query.answer()
            return

        receipt_id = int(match.group("receipt_id"))
        action = match.group("action")
        value = match.group("value")
        receipt = self.db.get_receipt_by_id(receipt_id)
        if receipt is None:
            await query.answer("Чек не найден.", show_alert=True)
            return

        if receipt.status != "pending":
            await query.answer("Этот чек уже обработан.", show_alert=True)
            return

        if action == "extend" and value is not None:
            customer = self.db.get_customer_by_user_id(receipt.telegram_user_id)
            new_expiry = parse_expiry_input(
                f"+{value}",
                customer.subscription_expires_on if customer else None,
                self.today(),
            )
            await self.apply_expiry_update(
                context.application,
                user_id=receipt.telegram_user_id,
                new_expiry=new_expiry,
                receipt=receipt,
                review_note=f"Продлено на {value} дней",
                customer_message=self.build_payment_confirmed_text(),
            )
            self.record_customer_audit(
                update,
                receipt.telegram_user_id,
                action="extend",
                details=f"Подписка продлена на {value} дней до {format_date(new_expiry)} через чек #{receipt.id}.",
            )
            await query.answer("Готово.")
            return

        if action == "custom":
            context.chat_data[ADMIN_PENDING_KEY] = {"action": "custom_expiry", "receipt_id": receipt.id}
            await query.answer()
            await query.message.reply_text(
                f"Пришлите новый срок для чека #{receipt.id} в формате {self.date_or_period_hint()}.\n"
                "Если передумали, используйте /cancel."
            )
            return

        if action == "reject":
            context.chat_data[ADMIN_PENDING_KEY] = {"action": "reject_receipt", "receipt_id": receipt.id}
            await query.answer()
            await query.message.reply_text(
                f"Пришлите комментарий к отклонению чека #{receipt.id}.\n"
                "Если комментарий не нужен, отправьте -.\n"
                "Если передумали, используйте /cancel."
            )
            return

        if action in {"mute_receipts", "unmute_receipts"}:
            muted = action == "mute_receipts"
            previous_customer = self.db.get_customer_by_user_id(receipt.telegram_user_id)
            updated_customer = self.db.set_customer_receipts_muted(receipt.telegram_user_id, muted)
            previous_state = (
                "выключен"
                if previous_customer is not None and previous_customer.receipts_muted
                else "включен"
            )
            await self.refresh_receipt_review_message(context.application, receipt.id)
            state = "выключен" if updated_customer.receipts_muted else "включен"
            self.record_customer_audit(
                update,
                updated_customer.telegram_user_id,
                action="mutereceipts",
                details=f"Прием чеков: {previous_state} -> {state}. Через чек #{receipt.id}.",
            )
            await query.answer(f"Прием чеков: {state}.")
            return

        await query.answer()

    async def refresh_receipt_review_message(self, application: Application, receipt_id: int) -> None:
        receipt = self.db.get_receipt_by_id(receipt_id)
        if receipt is None or receipt.admin_chat_id is None or receipt.admin_control_message_id is None:
            return

        reply_markup = self.receipt_keyboard(receipt.id) if receipt.status == "pending" else None
        await application.bot.edit_message_text(
            chat_id=receipt.admin_chat_id,
            message_id=receipt.admin_control_message_id,
            text=self.render_receipt_review_message(receipt),
            reply_markup=reply_markup,
        )

    async def reminder_job(self, context: CallbackContext) -> None:
        await self.process_reminders(context.application)

    async def process_reminders(self, application: Application) -> int:
        today = self.today()
        candidates = self.db.list_due_reminders(
            today=today,
            reminder_days_before=self.settings.reminder_days_before,
            overdue_interval_days=max(1, self.settings.overdue_reminder_interval_days),
        )

        sent_count = 0
        for candidate in candidates:
            customer = candidate.customer
            expires_on = customer.subscription_expires_on
            if expires_on is None:
                continue

            delivered = False
            if customer.chat_id is not None:
                try:
                    await application.bot.send_message(
                        chat_id=customer.chat_id,
                        text=self.build_customer_reminder_message(
                            customer,
                            expires_on,
                            candidate.decision.days_left,
                        ),
                    )
                    delivered = True
                except Exception:
                    LOGGER.exception("Failed to send reminder to customer %s", customer.telegram_user_id)

            if candidate.decision.reminder_type == "overdue":
                admin_chat_id = self.resolve_admin_chat_id()
                if admin_chat_id is not None:
                    try:
                        await application.bot.send_message(
                            chat_id=admin_chat_id,
                            text=self.build_admin_overdue_reminder_text(customer),
                        )
                        delivered = True
                    except Exception:
                        LOGGER.exception(
                            "Failed to send overdue reminder to admin for customer %s",
                            customer.telegram_user_id,
                        )

            if not delivered:
                continue

            self.db.record_reminder(
                customer_id=customer.id,
                reminder_key=candidate.decision.reminder_key,
                reminder_type=candidate.decision.reminder_type,
            )
            sent_count += 1

        return sent_count

    async def error_handler(self, update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
        LOGGER.exception("Unhandled error while processing update: %s", update, exc_info=context.error)


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def main() -> None:
    settings = load_settings()
    configure_logging(settings.log_level)

    database = Database(settings.database_path)
    database.init()

    app = VPNPaymentBot(settings, database).build_application()
    app.run_polling(allowed_updates=Update.ALL_TYPES, bootstrap_retries=3)
