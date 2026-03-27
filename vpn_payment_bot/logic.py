from __future__ import annotations

from calendar import monthrange
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta


RELATIVE_EXPIRY_RE = re.compile(
    r"^\+(?P<value>\d+)(?:\s*(?P<unit>d|day|days|m|mo|mon|month|months|д|дн|день|дня|дней|м|мес|месяц|месяца|месяцев))?$",
    re.IGNORECASE,
)
CLIENT_CODE_PREFIX = "VPN-"
DEFAULT_PRICE_PER_DEVICE_RUB = 150


@dataclass(slots=True, frozen=True)
class ReminderDecision:
    reminder_type: str
    days_left: int
    reminder_key: str


@dataclass(slots=True, frozen=True)
class RelativeExpiryPeriod:
    amount: int
    unit: str


def _russian_plural(value: int, form_1: str, form_2_4: str, form_other: str) -> str:
    remainder_100 = value % 100
    remainder_10 = value % 10
    if 11 <= remainder_100 <= 14:
        return form_other
    if remainder_10 == 1:
        return form_1
    if remainder_10 in (2, 3, 4):
        return form_2_4
    return form_other


def _encode_base36(value: int) -> str:
    alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    if value == 0:
        return "0"

    digits: list[str] = []
    remainder = value
    while remainder > 0:
        remainder, index = divmod(remainder, 36)
        digits.append(alphabet[index])
    return "".join(reversed(digits))


def _decode_base36(value: str) -> int:
    return int(value, 36)


def make_client_code(user_id: int) -> str:
    return f"{CLIENT_CODE_PREFIX}{_encode_base36(user_id)}"


def parse_client_code(raw: str) -> int | None:
    value = raw.strip().upper()
    if not value.startswith(CLIENT_CODE_PREFIX):
        return None

    encoded = value.removeprefix(CLIENT_CODE_PREFIX)
    if not encoded:
        return None

    try:
        return _decode_base36(encoded)
    except ValueError:
        return None


def parse_expiry_input(raw: str, current_expiry: date | None, today: date) -> date:
    value = raw.strip()
    relative_period = parse_relative_expiry(value)
    if relative_period is not None:
        base_date = today if current_expiry is None else max(today, current_expiry)
        return apply_relative_expiry(base_date, relative_period)

    return parse_absolute_date(value)


def parse_absolute_date(raw: str) -> date:
    value = raw.strip()
    if "." in value:
        return datetime.strptime(value, "%d.%m.%Y").date()
    return date.fromisoformat(value)


def format_date(value: date) -> str:
    return value.strftime("%d.%m.%Y")


def parse_relative_expiry(raw: str) -> RelativeExpiryPeriod | None:
    value = raw.strip()
    match = RELATIVE_EXPIRY_RE.fullmatch(value)
    if match is None:
        return None

    amount = int(match.group("value"))
    unit = (match.group("unit") or "d").lower()

    if unit in {"d", "day", "days", "д", "дн", "день", "дня", "дней"}:
        return RelativeExpiryPeriod(amount=amount, unit="days")
    if unit in {"m", "mo", "mon", "month", "months", "м", "мес", "месяц", "месяца", "месяцев"}:
        return RelativeExpiryPeriod(amount=amount, unit="months")
    return None


def apply_relative_expiry(base_date: date, period: RelativeExpiryPeriod) -> date:
    if period.unit == "days":
        return base_date + timedelta(days=period.amount)
    if period.unit == "months":
        return _add_months(base_date, period.amount)
    raise ValueError(f"Unsupported expiry period unit: {period.unit}")


def format_relative_expiry(period: RelativeExpiryPeriod) -> str:
    if period.unit == "days":
        suffix = _russian_plural(period.amount, "день", "дня", "дней")
        return f"{period.amount} {suffix}"
    if period.unit == "months":
        suffix = _russian_plural(period.amount, "месяц", "месяца", "месяцев")
        return f"{period.amount} {suffix}"
    raise ValueError(f"Unsupported expiry period unit: {period.unit}")


def _add_months(base_date: date, months: int) -> date:
    month_index = base_date.month - 1 + months
    year = base_date.year + month_index // 12
    month = month_index % 12 + 1
    day = min(base_date.day, monthrange(year, month)[1])
    return date(year, month, day)


def format_expiry_status(expires_on: date | None, today: date) -> str:
    if expires_on is None:
        return "Пока срок подписки не привязан."

    days_left = (expires_on - today).days
    if days_left > 0:
        suffix = _russian_plural(days_left, "день", "дня", "дней")
        return f"Подписка активна до {format_date(expires_on)} ({days_left} {suffix} осталось)."
    if days_left == 0:
        return f"Подписка заканчивается сегодня ({format_date(expires_on)})."

    overdue_days = abs(days_left)
    suffix = _russian_plural(overdue_days, "день", "дня", "дней")
    return f"Подписка закончилась {format_date(expires_on)} ({overdue_days} {suffix} назад)."


def format_device_count(device_count: int) -> str:
    if device_count <= 0:
        raise ValueError("Количество устройств должно быть больше нуля.")
    noun = _russian_plural(device_count, "устройство", "устройства", "устройств")
    return f"{device_count} {noun}"


def calculate_payment_amount(device_count: int, price_per_device: int = DEFAULT_PRICE_PER_DEVICE_RUB) -> int:
    if device_count <= 0:
        raise ValueError("Количество устройств должно быть больше нуля.")
    if price_per_device <= 0:
        raise ValueError("Цена за устройство должна быть больше нуля.")
    return device_count * price_per_device


def build_payment_details_text(
    device_count: int,
    payment_destination_text: str,
    *,
    uppercase_amount_line: bool = False,
) -> str:
    amount = calculate_payment_amount(device_count)
    amount_line = f"К оплате: {amount}₽ ({format_device_count(device_count)})"
    if uppercase_amount_line:
        amount_line = amount_line.upper()
    return f"{amount_line}\n{payment_destination_text}"


def build_payment_amount_text(device_count: int, *, uppercase_amount_line: bool = False) -> str:
    amount = calculate_payment_amount(device_count)
    amount_line = f"К оплате: {amount}₽ ({format_device_count(device_count)})"
    if uppercase_amount_line:
        amount_line = amount_line.upper()
    return amount_line


def decide_reminder(
    *,
    expires_on: date,
    today: date,
    reminder_days_before: tuple[int, ...],
    overdue_interval_days: int,
) -> ReminderDecision | None:
    days_left = (expires_on - today).days

    if days_left == 0 or days_left in reminder_days_before:
        return ReminderDecision(
            reminder_type="before_due",
            days_left=days_left,
            reminder_key=f"before:{expires_on.isoformat()}:{days_left}",
        )

    if overdue_interval_days > 0 and days_left < 0:
        days_overdue = abs(days_left)
        if days_overdue == 1 or (days_overdue - 1) % overdue_interval_days == 0:
            return ReminderDecision(
                reminder_type="overdue",
                days_left=days_left,
                reminder_key=f"overdue:{expires_on.isoformat()}:{days_overdue}",
            )

    return None


def build_reminder_text(customer_name: str, expires_on: date, days_left: int, device_count: int) -> str:
    if days_left > 0:
        suffix = _russian_plural(days_left, "день", "дня", "дней")
        return (
            f"Привет, {customer_name}! 😊\n"
            f"Напоминаю: ваша VPN-подписка закончится {format_date(expires_on)} "
            f"({days_left} {suffix} осталось).\n\n"
            f"{build_payment_amount_text(device_count)}\n\n"
            "Для оплаты используйте /pay. Бот покажет реквизиты и примет чек."
        )

    if days_left == 0:
        return (
            f"Привет, {customer_name}! 😊\n"
            f"Сегодня заканчивается ваша VPN-подписка ({format_date(expires_on)}).\n\n"
            f"{build_payment_amount_text(device_count)}\n\n"
            "Для оплаты используйте /pay. Бот покажет реквизиты и примет чек."
        )

    overdue_days = abs(days_left)
    suffix = _russian_plural(overdue_days, "день", "дня", "дней")
    return (
        "⚠️💳 Подписка закончилась\n"
        f"{customer_name}, ваша VPN-подписка закончилась {format_date(expires_on)} "
        f"({overdue_days} {suffix} назад).\n\n"
        f"{build_payment_amount_text(device_count)}\n\n"
        "Если оплата не поступит, доступ будет отключен.\n"
        "Для оплаты используйте /pay. Бот покажет реквизиты и примет чек."
    )
