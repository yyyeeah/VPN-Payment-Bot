import asyncio
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

from vpn_payment_bot.bot import VPNPaymentBot
from vpn_payment_bot.config import Settings
from vpn_payment_bot.db import Database


def make_settings(database_path: Path) -> Settings:
    return Settings(
        bot_token="123:ABC",
        database_path=database_path,
        timezone_name="Asia/Yekaterinburg",
        admin_chat_id=1,
        admin_code=None,
        payment_destination_text="test-bank-details",
        reminder_days_before=(1,),
        overdue_reminder_interval_days=1,
        daily_reminder_time_raw="10:00",
        log_level="INFO",
        reminder_before_due_template=None,
        reminder_due_today_template=None,
        reminder_overdue_template=None,
        admin_overdue_reminder_template=None,
    )


def test_first_contact_is_stored_but_hidden_from_clients_list(tmp_path: Path) -> None:
    database_path = tmp_path / "bot.sqlite3"
    db = Database(database_path)
    db.init()
    bot = VPNPaymentBot(make_settings(database_path), db)

    customer = bot.sync_existing_customer(
        user_id=123456,
        chat_id=777,
        username="alice",
        full_name="Alice Example",
    )

    assert customer.chat_id == 777
    assert customer.subscription_expires_on is None
    assert customer.admin_name is None
    assert customer.device_count == 1
    assert customer.receipts_muted is False
    assert db.list_customers() == []

    updated = db.set_subscription_expiry(customer.telegram_user_id, bot.today())
    assert updated.chat_id == 777
    assert len(db.list_customers()) == 1

    db.close()


def test_customer_settings_can_be_changed_individually(tmp_path: Path) -> None:
    database_path = tmp_path / "bot.sqlite3"
    db = Database(database_path)
    db.init()
    bot = VPNPaymentBot(make_settings(database_path), db)

    customer = bot.sync_existing_customer(
        user_id=123456,
        chat_id=777,
        username="alice",
        full_name="Alice Example",
    )

    updated_devices = db.set_customer_device_count(customer.telegram_user_id, 4)
    updated_muted = db.set_customer_receipts_muted(customer.telegram_user_id, True)

    assert updated_devices.device_count == 4
    assert updated_muted.receipts_muted is True

    db.close()


def test_customer_can_be_deactivated_and_reactivated(tmp_path: Path) -> None:
    database_path = tmp_path / "bot.sqlite3"
    db = Database(database_path)
    db.init()
    bot = VPNPaymentBot(make_settings(database_path), db)

    customer = bot.sync_existing_customer(
        user_id=123456,
        chat_id=777,
        username="alice",
        full_name="Alice Example",
    )
    db.set_subscription_expiry(customer.telegram_user_id, date(2026, 4, 10))

    deactivated = db.deactivate_customer(customer.telegram_user_id)
    assert deactivated is not None
    assert deactivated.is_active is False
    assert deactivated.subscription_expires_on is None
    assert db.list_customers() == []

    reactivated = db.set_subscription_expiry(customer.telegram_user_id, date(2026, 4, 25))
    assert reactivated.is_active is True
    assert reactivated.subscription_expires_on == date(2026, 4, 25)
    assert len(db.list_customers()) == 1

    db.close()


def test_receipt_timestamp_is_rendered_in_local_timezone(tmp_path: Path) -> None:
    database_path = tmp_path / "bot.sqlite3"
    db = Database(database_path)
    db.init()
    bot = VPNPaymentBot(make_settings(database_path), db)

    assert bot.format_timestamp("2026-03-26T17:36:17.340829+00:00") == "26.03.2026 22:36"

    db.close()


def test_link_success_text_uses_correct_russian_plural_form(tmp_path: Path) -> None:
    database_path = tmp_path / "bot.sqlite3"
    db = Database(database_path)
    db.init()
    bot = VPNPaymentBot(make_settings(database_path), db)

    text = bot.build_link_success_text(bot.today() + timedelta(days=2))

    assert "Осталось: 2 дня." in text

    db.close()


def test_admin_overdue_reminder_does_not_include_payment_details(tmp_path: Path) -> None:
    database_path = tmp_path / "bot.sqlite3"
    db = Database(database_path)
    db.init()
    bot = VPNPaymentBot(make_settings(database_path), db)

    customer = bot.sync_existing_customer(
        user_id=123456,
        chat_id=777,
        username="alice",
        full_name="Alice Example",
    )
    customer = db.set_subscription_expiry(customer.telegram_user_id, date(2026, 3, 20))
    customer = db.set_customer_device_count(customer.telegram_user_id, 2)

    text = bot.build_admin_overdue_reminder_text(customer)

    assert text.startswith("⚠️💳 Просроченная подписка")
    assert "test-bank-details" not in text
    assert "К оплате" not in text

    db.close()


def test_sync_customer_commands_for_linked_customer_omits_start(tmp_path: Path) -> None:
    database_path = tmp_path / "bot.sqlite3"
    db = Database(database_path)
    db.init()
    bot = VPNPaymentBot(make_settings(database_path), db)

    customer = bot.sync_existing_customer(
        user_id=123456,
        chat_id=777,
        username="alice",
        full_name="Alice Example",
    )
    customer = db.set_subscription_expiry(customer.telegram_user_id, date(2026, 4, 10))

    application = SimpleNamespace(bot=AsyncMock())
    asyncio.run(bot.sync_customer_commands(application, customer))

    application.bot.set_my_commands.assert_awaited_once()
    commands = application.bot.set_my_commands.await_args.args[0]
    assert ("start", "Открыть бота") not in commands
    assert ("status", "Показать срок подписки") in commands
    assert ("price", "Количество устройств и цена") in commands
    assert ("whoami", "Показать user_id и client_id") in commands
    application.bot.delete_my_commands.assert_not_called()

    db.close()


def test_sync_command_scopes_for_admin_omits_whoami_and_updates_setdevices_description(tmp_path: Path) -> None:
    database_path = tmp_path / "bot.sqlite3"
    db = Database(database_path)
    db.init()
    bot = VPNPaymentBot(make_settings(database_path), db)

    application = SimpleNamespace(bot=AsyncMock())
    asyncio.run(bot.sync_command_scopes(application))

    admin_commands = application.bot.set_my_commands.await_args_list[1].args[0]
    assert ("whoami", "Показать chat_id и user_id") not in admin_commands
    assert ("setdevices", "Изменить число устройств клиента") in admin_commands

    db.close()


def test_sync_customer_commands_clears_chat_scope_for_unlinked_customer(tmp_path: Path) -> None:
    database_path = tmp_path / "bot.sqlite3"
    db = Database(database_path)
    db.init()
    bot = VPNPaymentBot(make_settings(database_path), db)

    customer = bot.sync_existing_customer(
        user_id=123456,
        chat_id=777,
        username="alice",
        full_name="Alice Example",
    )

    application = SimpleNamespace(bot=AsyncMock())
    asyncio.run(bot.sync_customer_commands(application, customer))

    application.bot.delete_my_commands.assert_awaited_once()
    application.bot.set_my_commands.assert_not_called()

    db.close()


def test_pay_command_sends_payment_details_and_waits_for_receipt(tmp_path: Path) -> None:
    database_path = tmp_path / "bot.sqlite3"
    db = Database(database_path)
    db.init()
    bot = VPNPaymentBot(make_settings(database_path), db)

    customer = bot.sync_existing_customer(
        user_id=123456,
        chat_id=777,
        username="alice",
        full_name="Alice Example",
    )
    customer = db.set_subscription_expiry(customer.telegram_user_id, date(2026, 4, 10))
    customer = db.set_customer_device_count(customer.telegram_user_id, 2)

    message = SimpleNamespace(reply_text=AsyncMock())
    update = SimpleNamespace(
        effective_message=message,
        effective_user=SimpleNamespace(id=customer.telegram_user_id, username="alice", full_name="Alice Example"),
        effective_chat=SimpleNamespace(id=customer.chat_id),
    )
    context = SimpleNamespace(application=SimpleNamespace(bot=AsyncMock()), user_data={})

    asyncio.run(bot.pay_command(update, context))

    message.reply_text.assert_awaited_once()
    text = message.reply_text.await_args.args[0]
    assert "Реквизиты для оплаты:" in text
    assert "test-bank-details" in text
    assert "300₽ (2 устройства)" in text
    assert "После перевода пришлите чек следующим сообщением." in text
    assert "/pay" not in text
    assert context.user_data["awaiting_receipt"] is True

    db.close()


def test_price_command_shows_devices_and_total_without_payment_details(tmp_path: Path) -> None:
    database_path = tmp_path / "bot.sqlite3"
    db = Database(database_path)
    db.init()
    bot = VPNPaymentBot(make_settings(database_path), db)

    customer = bot.sync_existing_customer(
        user_id=123456,
        chat_id=777,
        username="alice",
        full_name="Alice Example",
    )
    customer = db.set_subscription_expiry(customer.telegram_user_id, date(2026, 4, 10))
    customer = db.set_customer_device_count(customer.telegram_user_id, 2)

    message = SimpleNamespace(reply_text=AsyncMock())
    update = SimpleNamespace(
        effective_message=message,
        effective_user=SimpleNamespace(id=customer.telegram_user_id, username="alice", full_name="Alice Example"),
        effective_chat=SimpleNamespace(id=customer.chat_id),
    )
    context = SimpleNamespace(application=SimpleNamespace(bot=AsyncMock()), user_data={})

    asyncio.run(bot.price_command(update, context))

    message.reply_text.assert_awaited_once()
    text = message.reply_text.await_args.args[0]
    assert "Стоимость подписки:" in text
    assert "Устройства: 2 устройства" in text
    assert "Итого: 300₽" in text
    assert "Тариф: 150₽ за 1 устройство." in text
    assert "/pay" in text
    assert "test-bank-details" not in text
    assert "awaiting_receipt" not in context.user_data

    db.close()


def test_whoami_command_for_customer_omits_chat_id(tmp_path: Path) -> None:
    database_path = tmp_path / "bot.sqlite3"
    db = Database(database_path)
    db.init()
    bot = VPNPaymentBot(make_settings(database_path), db)

    message = SimpleNamespace(reply_text=AsyncMock())
    update = SimpleNamespace(
        effective_message=message,
        effective_user=SimpleNamespace(id=123456, username="alice", full_name="Alice Example"),
        effective_chat=SimpleNamespace(id=777),
    )

    asyncio.run(bot.whoami_command(update, SimpleNamespace()))

    text = message.reply_text.await_args.args[0]
    assert "user_id=123456" in text
    assert "client_id=" in text
    assert "chat_id=" not in text

    db.close()


def test_whoami_command_is_disabled_for_admin(tmp_path: Path) -> None:
    database_path = tmp_path / "bot.sqlite3"
    db = Database(database_path)
    db.init()
    bot = VPNPaymentBot(make_settings(database_path), db)

    message = SimpleNamespace(reply_text=AsyncMock())
    update = SimpleNamespace(
        effective_message=message,
        effective_user=SimpleNamespace(id=999, username="admin", full_name="Admin Example"),
        effective_chat=SimpleNamespace(id=1),
    )

    asyncio.run(bot.whoami_command(update, SimpleNamespace()))

    message.reply_text.assert_awaited_once_with("Команда доступна только клиенту.")

    db.close()


def test_setdevices_command_records_audit_log(tmp_path: Path) -> None:
    database_path = tmp_path / "bot.sqlite3"
    db = Database(database_path)
    db.init()
    bot = VPNPaymentBot(make_settings(database_path), db)

    customer = bot.sync_existing_customer(
        user_id=123456,
        chat_id=777,
        username="alice",
        full_name="Alice Example",
    )
    db.set_subscription_expiry(customer.telegram_user_id, date(2026, 4, 10))

    message = SimpleNamespace(reply_text=AsyncMock(), reply_to_message=None)
    update = SimpleNamespace(
        effective_message=message,
        effective_chat=SimpleNamespace(id=1),
        effective_user=SimpleNamespace(id=999, username="admin", full_name="Admin Example"),
    )
    context = SimpleNamespace(args=[bot.client_code(customer.telegram_user_id), "3"])

    asyncio.run(bot.setdevices_command(update, context))

    entries = db.list_customer_audit_entries(customer.telegram_user_id, limit=1)
    assert len(entries) == 1
    assert entries[0].action == "setdevices"
    assert "1 -> 3" in entries[0].details
    assert entries[0].actor_username == "admin"

    db.close()


def test_setname_command_updates_admin_only_name_and_admin_views(tmp_path: Path) -> None:
    database_path = tmp_path / "bot.sqlite3"
    db = Database(database_path)
    db.init()
    bot = VPNPaymentBot(make_settings(database_path), db)

    customer = bot.sync_existing_customer(
        user_id=123456,
        chat_id=777,
        username="alice",
        full_name="Alice Example",
    )
    customer = db.set_subscription_expiry(customer.telegram_user_id, date(2026, 4, 10))

    message = SimpleNamespace(reply_text=AsyncMock(), reply_to_message=None)
    update = SimpleNamespace(
        effective_message=message,
        effective_chat=SimpleNamespace(id=1),
        effective_user=SimpleNamespace(id=999, username="admin", full_name="Admin Example"),
    )
    context = SimpleNamespace(args=[bot.client_code(customer.telegram_user_id), "VIP", "Alice"])

    asyncio.run(bot.setname_command(update, context))

    updated = db.get_customer_by_user_id(customer.telegram_user_id)
    assert updated is not None
    assert updated.admin_name == "VIP Alice"
    assert updated.full_name == "Alice Example"

    reminder_text = bot.build_customer_reminder_message(updated, date(2026, 3, 20), -2)
    review_receipt = db.create_receipt(
        telegram_user_id=updated.telegram_user_id,
        customer_id=updated.id,
        source_chat_id=updated.chat_id or 777,
        source_message_id=10,
        kind="text",
        caption="Чек",
        file_id=None,
        file_unique_id=None,
    )
    review_text = bot.render_receipt_review_message(review_receipt)
    audit_entry = db.list_customer_audit_entries(updated.telegram_user_id, limit=1)[0]

    assert "VIP Alice" not in reminder_text
    assert "Alice Example" in reminder_text
    assert "Клиент: VIP Alice" in review_text
    assert "Имя в Telegram: Alice Example" in review_text
    assert audit_entry.action == "setname"

    db.close()


def test_clients_list_uses_admin_name_for_visible_sort_and_label(tmp_path: Path) -> None:
    database_path = tmp_path / "bot.sqlite3"
    db = Database(database_path)
    db.init()
    bot = VPNPaymentBot(make_settings(database_path), db)

    first = bot.sync_existing_customer(
        user_id=111,
        chat_id=701,
        username="first",
        full_name="Zed Telegram",
    )
    second = bot.sync_existing_customer(
        user_id=222,
        chat_id=702,
        username="second",
        full_name="Alpha Telegram",
    )
    db.set_subscription_expiry(first.telegram_user_id, date(2026, 4, 10))
    db.set_subscription_expiry(second.telegram_user_id, date(2026, 4, 10))
    db.set_customer_admin_name(first.telegram_user_id, "Beta Admin")

    customers = db.list_customers()

    assert [bot.admin_customer_name(customer) for customer in customers] == ["Alpha Telegram", "Beta Admin"]

    db.close()


def test_duplicate_receipt_is_marked_in_review_message(tmp_path: Path) -> None:
    database_path = tmp_path / "bot.sqlite3"
    db = Database(database_path)
    db.init()
    bot = VPNPaymentBot(make_settings(database_path), db)

    customer = bot.sync_existing_customer(
        user_id=123456,
        chat_id=777,
        username="alice",
        full_name="Alice Example",
    )
    customer = db.set_subscription_expiry(customer.telegram_user_id, date(2026, 4, 10))

    first = db.create_receipt(
        telegram_user_id=customer.telegram_user_id,
        customer_id=customer.id,
        source_chat_id=customer.chat_id or 777,
        source_message_id=1,
        kind="photo",
        caption="Первый чек",
        file_id="file-1",
        file_unique_id="unique-1",
    )
    second = db.create_receipt(
        telegram_user_id=customer.telegram_user_id,
        customer_id=customer.id,
        source_chat_id=customer.chat_id or 777,
        source_message_id=2,
        kind="photo",
        caption="Второй чек",
        file_id="file-2",
        file_unique_id="unique-1",
    )

    text = bot.render_receipt_review_message(second)

    assert second.duplicate_of_receipt_id == first.id
    assert f"Дубликат файла: чек #{first.id}" in text

    db.close()


def test_client_command_shows_recent_receipts_and_audit_history(tmp_path: Path) -> None:
    database_path = tmp_path / "bot.sqlite3"
    db = Database(database_path)
    db.init()
    bot = VPNPaymentBot(make_settings(database_path), db)

    customer = bot.sync_existing_customer(
        user_id=123456,
        chat_id=777,
        username="alice",
        full_name="Alice Example",
    )
    customer = db.set_subscription_expiry(customer.telegram_user_id, date(2026, 4, 10))
    db.record_customer_audit(
        telegram_user_id=customer.telegram_user_id,
        action="setexpiry",
        details="Срок подписки изменен на 10.04.2026.",
        actor_user_id=999,
        actor_username="admin",
        actor_full_name="Admin Example",
    )
    db.create_receipt(
        telegram_user_id=customer.telegram_user_id,
        customer_id=customer.id,
        source_chat_id=customer.chat_id or 777,
        source_message_id=3,
        kind="photo",
        caption="Чек",
        file_id="file-3",
        file_unique_id="unique-3",
    )

    message = SimpleNamespace(reply_text=AsyncMock(), reply_to_message=None)
    update = SimpleNamespace(
        effective_message=message,
        effective_chat=SimpleNamespace(id=1),
    )
    context = SimpleNamespace(args=[bot.client_code(customer.telegram_user_id)])

    asyncio.run(bot.client_command(update, context))

    text = message.reply_text.await_args.args[0]
    assert "Последние чеки:" in text
    assert "Последние изменения:" in text
    assert "setexpiry | Срок подписки изменен на 10.04.2026." in text
    assert "#1 | pending" in text

    db.close()


def test_build_customer_reminder_message_uses_custom_template(tmp_path: Path) -> None:
    database_path = tmp_path / "bot.sqlite3"
    db = Database(database_path)
    db.init()
    settings = replace(
        make_settings(database_path),
        reminder_overdue_template="⚠️ {customer_name}\n{payment_details}\n{status}",
    )
    bot = VPNPaymentBot(settings, db)

    customer = bot.sync_existing_customer(
        user_id=123456,
        chat_id=777,
        username="alice",
        full_name="Alice Example",
    )
    customer = db.set_subscription_expiry(customer.telegram_user_id, date(2026, 3, 20))
    customer = db.set_customer_device_count(customer.telegram_user_id, 2)

    text = bot.build_customer_reminder_message(customer, date(2026, 3, 20), -2)

    assert text.startswith("⚠️ Alice Example")
    assert "300₽ (2 устройства)" in text
    assert "test-bank-details" not in text
    assert "Подписка закончилась 20.03.2026" in text

    db.close()


def test_extend_command_sends_customer_confirmation_with_line_break(tmp_path: Path) -> None:
    database_path = tmp_path / "bot.sqlite3"
    db = Database(database_path)
    db.init()
    bot = VPNPaymentBot(make_settings(database_path), db)

    customer = bot.sync_existing_customer(
        user_id=123456,
        chat_id=777,
        username="alice",
        full_name="Alice Example",
    )
    db.set_subscription_expiry(customer.telegram_user_id, date(2026, 4, 10))

    message = SimpleNamespace(reply_text=AsyncMock(), reply_to_message=None)
    application_bot = AsyncMock()
    update = SimpleNamespace(
        effective_message=message,
        effective_chat=SimpleNamespace(id=1),
        effective_user=SimpleNamespace(id=999, username="admin", full_name="Admin Example"),
    )
    context = SimpleNamespace(
        args=[bot.client_code(customer.telegram_user_id), "30"],
        application=SimpleNamespace(bot=application_bot),
    )

    asyncio.run(bot.extend_command(update, context))

    application_bot.send_message.assert_awaited()
    text = application_bot.send_message.await_args.kwargs["text"]
    assert "Платёж подтверждён ✅\nПодписка продлена." in text

    db.close()
