from datetime import date

from vpn_payment_bot.logic import (
    format_relative_expiry,
    build_payment_details_text,
    build_reminder_text,
    calculate_payment_amount,
    decide_reminder,
    format_device_count,
    format_expiry_status,
    make_client_code,
    parse_client_code,
    parse_expiry_input,
    parse_relative_expiry,
)


def test_parse_relative_expiry_uses_later_of_today_and_current_expiry() -> None:
    today = date(2026, 3, 26)
    current_expiry = date(2026, 4, 10)
    assert parse_expiry_input("+30", current_expiry, today) == date(2026, 5, 10)


def test_parse_relative_expiry_uses_today_when_subscription_expired() -> None:
    today = date(2026, 3, 26)
    current_expiry = date(2026, 3, 20)
    assert parse_expiry_input("+7", current_expiry, today) == date(2026, 4, 2)


def test_parse_relative_expiry_supports_calendar_months() -> None:
    today = date(2026, 1, 10)
    current_expiry = date(2026, 1, 31)
    assert parse_expiry_input("+1m", current_expiry, today) == date(2026, 2, 28)


def test_parse_relative_expiry_supports_russian_month_suffix() -> None:
    today = date(2026, 3, 26)
    current_expiry = date(2026, 4, 10)
    assert parse_expiry_input("+2мес", current_expiry, today) == date(2026, 6, 10)


def test_parse_expiry_input_supports_dd_mm_yyyy() -> None:
    assert parse_expiry_input("02.04.2026", None, date(2026, 3, 26)) == date(2026, 4, 2)


def test_parse_relative_expiry_returns_structured_period() -> None:
    period = parse_relative_expiry("+3m")
    assert period is not None
    assert period.unit == "months"
    assert format_relative_expiry(period) == "3 месяца"


def test_decide_reminder_before_due() -> None:
    decision = decide_reminder(
        expires_on=date(2026, 4, 2),
        today=date(2026, 3, 26),
        reminder_days_before=(7, 3, 1, 0),
        overdue_interval_days=3,
    )
    assert decision is not None
    assert decision.reminder_type == "before_due"
    assert decision.days_left == 7


def test_decide_reminder_on_expiry_day_even_if_zero_not_configured() -> None:
    decision = decide_reminder(
        expires_on=date(2026, 3, 26),
        today=date(2026, 3, 26),
        reminder_days_before=(1,),
        overdue_interval_days=3,
    )
    assert decision is not None
    assert decision.reminder_type == "before_due"
    assert decision.days_left == 0


def test_decide_reminder_overdue_interval() -> None:
    decision = decide_reminder(
        expires_on=date(2026, 3, 20),
        today=date(2026, 3, 24),
        reminder_days_before=(7, 3, 1, 0),
        overdue_interval_days=3,
    )
    assert decision is not None
    assert decision.reminder_type == "overdue"
    assert decision.days_left == -4


def test_format_expiry_status_for_active_subscription() -> None:
    assert (
        format_expiry_status(date(2026, 4, 2), date(2026, 3, 26))
        == "Подписка активна до 02.04.2026 (7 дней осталось)."
    )


def test_build_payment_details_uses_default_price_per_device() -> None:
    assert calculate_payment_amount(3) == 450
    assert format_device_count(3) == "3 устройства"
    assert build_payment_details_text(3, "test-bank-details") == "К оплате: 450₽ (3 устройства)\ntest-bank-details"


def test_overdue_reminder_uses_normal_case_and_omits_bank_details() -> None:
    text = build_reminder_text("Alice", date(2026, 3, 20), -2, 2)
    assert text.startswith("⚠️💳 Подписка закончилась")
    assert "Alice, ваша VPN-подписка закончилась 20.03.2026" in text
    assert "К оплате: 300₽ (2 устройства)" in text
    assert "test-bank-details" not in text
    assert "Если оплата не поступит, доступ будет отключен." in text
    assert "Для оплаты используйте /pay." in text
    assert text.count("/pay") == 1


def test_due_soon_reminder_mentions_payment_is_processed_via_pay_without_bank_details() -> None:
    text = build_reminder_text("Alice", date(2026, 4, 2), 7, 2)
    assert "К оплате: 300₽ (2 устройства)" in text
    assert "test-bank-details" not in text
    assert "Для оплаты используйте /pay." in text
    assert text.count("/pay") == 1
    assert "После оплаты просто пришлите чек сюда через /pay." not in text


def test_client_code_roundtrip() -> None:
    code = make_client_code(123456789)
    assert parse_client_code(code) == 123456789
