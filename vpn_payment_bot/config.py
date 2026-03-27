from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import time
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv


def _parse_optional_int(raw: str | None) -> int | None:
    if raw is None or not raw.strip():
        return None
    return int(raw.strip())


def _parse_optional_template(raw: str | None) -> str | None:
    if raw is None or not raw.strip():
        return None
    return raw.strip().replace("\\n", "\n")


def _parse_csv_ints(raw: str) -> tuple[int, ...]:
    values = []
    for chunk in raw.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        values.append(int(chunk))
    if not values:
        raise ValueError("REMINDER_DAYS_BEFORE должен содержать хотя бы одно число.")
    return tuple(dict.fromkeys(values))


def _parse_clock(raw: str) -> tuple[int, int]:
    hour_text, minute_text = raw.split(":", maxsplit=1)
    hour = int(hour_text)
    minute = int(minute_text)
    if hour not in range(24) or minute not in range(60):
        raise ValueError("DAILY_REMINDER_TIME должен быть в формате HH:MM.")
    return hour, minute


@dataclass(slots=True, frozen=True)
class Settings:
    bot_token: str
    database_path: Path
    timezone_name: str
    admin_chat_id: int | None
    admin_code: str | None
    payment_destination_text: str
    reminder_days_before: tuple[int, ...]
    overdue_reminder_interval_days: int
    daily_reminder_time_raw: str
    log_level: str
    reminder_before_due_template: str | None
    reminder_due_today_template: str | None
    reminder_overdue_template: str | None
    admin_overdue_reminder_template: str | None

    @property
    def tzinfo(self) -> ZoneInfo:
        return ZoneInfo(self.timezone_name)

    @property
    def daily_reminder_time(self) -> time:
        hour, minute = _parse_clock(self.daily_reminder_time_raw)
        return time(hour=hour, minute=minute, tzinfo=self.tzinfo)


def load_settings() -> Settings:
    load_dotenv()

    bot_token = os.getenv("BOT_TOKEN", "").strip()
    if not bot_token:
        raise ValueError("Нужно указать BOT_TOKEN.")

    admin_chat_id = _parse_optional_int(os.getenv("ADMIN_CHAT_ID"))
    admin_code = os.getenv("ADMIN_CODE", "").strip() or None
    if admin_chat_id is None and not admin_code:
        raise ValueError("Перед запуском укажите ADMIN_CHAT_ID или ADMIN_CODE.")

    database_path = Path(os.getenv("DATABASE_PATH", "bot.sqlite3")).expanduser()
    timezone_name = os.getenv("TIMEZONE", "Asia/Yekaterinburg").strip()
    payment_destination_text = (
        os.getenv("PAYMENT_DESTINATION_TEXT", "").strip() or "УКАЖИТЕ PAYMENT_DESTINATION_TEXT В .env"
    )
    reminder_days_before = _parse_csv_ints(os.getenv("REMINDER_DAYS_BEFORE", "1"))
    overdue_reminder_interval_days = int(os.getenv("OVERDUE_REMINDER_INTERVAL_DAYS", "1"))
    if overdue_reminder_interval_days < 0:
        raise ValueError("OVERDUE_REMINDER_INTERVAL_DAYS не может быть отрицательным.")

    daily_reminder_time_raw = os.getenv("DAILY_REMINDER_TIME", "10:00").strip()
    _parse_clock(daily_reminder_time_raw)

    log_level = os.getenv("LOG_LEVEL", "INFO").strip().upper()
    reminder_before_due_template = _parse_optional_template(os.getenv("REMINDER_BEFORE_DUE_TEMPLATE"))
    reminder_due_today_template = _parse_optional_template(os.getenv("REMINDER_DUE_TODAY_TEMPLATE"))
    reminder_overdue_template = _parse_optional_template(os.getenv("REMINDER_OVERDUE_TEMPLATE"))
    admin_overdue_reminder_template = _parse_optional_template(os.getenv("ADMIN_OVERDUE_REMINDER_TEMPLATE"))

    return Settings(
        bot_token=bot_token,
        database_path=database_path,
        timezone_name=timezone_name,
        admin_chat_id=admin_chat_id,
        admin_code=admin_code,
        payment_destination_text=payment_destination_text,
        reminder_days_before=reminder_days_before,
        overdue_reminder_interval_days=overdue_reminder_interval_days,
        daily_reminder_time_raw=daily_reminder_time_raw,
        log_level=log_level,
        reminder_before_due_template=reminder_before_due_template,
        reminder_due_today_template=reminder_due_today_template,
        reminder_overdue_template=reminder_overdue_template,
        admin_overdue_reminder_template=admin_overdue_reminder_template,
    )
