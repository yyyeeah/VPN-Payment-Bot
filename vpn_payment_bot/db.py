from __future__ import annotations

import sqlite3
import threading
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

from .logic import ReminderDecision, decide_reminder


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _date_or_none(raw: str | None) -> date | None:
    if not raw:
        return None
    return date.fromisoformat(raw)


@dataclass(slots=True, frozen=True)
class Customer:
    id: int
    telegram_user_id: int
    chat_id: int | None
    username: str | None
    full_name: str
    admin_name: str | None
    subscription_expires_on: date | None
    notes: str | None
    device_count: int
    receipts_muted: bool
    is_active: bool
    created_at: str
    updated_at: str


@dataclass(slots=True, frozen=True)
class Receipt:
    id: int
    customer_id: int | None
    telegram_user_id: int
    source_chat_id: int
    source_message_id: int
    kind: str
    caption: str | None
    file_id: str | None
    file_unique_id: str | None
    duplicate_of_receipt_id: int | None
    status: str
    admin_chat_id: int | None
    admin_copy_message_id: int | None
    admin_control_message_id: int | None
    review_note: str | None
    created_at: str
    reviewed_at: str | None


@dataclass(slots=True, frozen=True)
class ReminderCandidate:
    customer: Customer
    decision: ReminderDecision


@dataclass(slots=True, frozen=True)
class AuditEntry:
    id: int
    customer_id: int | None
    telegram_user_id: int
    actor_user_id: int | None
    actor_username: str | None
    actor_full_name: str | None
    action: str
    details: str
    created_at: str


def _customer_from_row(row: sqlite3.Row) -> Customer:
    return Customer(
        id=row["id"],
        telegram_user_id=row["telegram_user_id"],
        chat_id=row["chat_id"],
        username=row["username"],
        full_name=row["full_name"],
        admin_name=row["admin_name"],
        subscription_expires_on=_date_or_none(row["subscription_expires_on"]),
        notes=row["notes"],
        device_count=row["device_count"],
        receipts_muted=bool(row["receipts_muted"]),
        is_active=bool(row["is_active"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _receipt_from_row(row: sqlite3.Row) -> Receipt:
    return Receipt(
        id=row["id"],
        customer_id=row["customer_id"],
        telegram_user_id=row["telegram_user_id"],
        source_chat_id=row["source_chat_id"],
        source_message_id=row["source_message_id"],
        kind=row["kind"],
        caption=row["caption"],
        file_id=row["file_id"],
        file_unique_id=row["file_unique_id"],
        duplicate_of_receipt_id=row["duplicate_of_receipt_id"],
        status=row["status"],
        admin_chat_id=row["admin_chat_id"],
        admin_copy_message_id=row["admin_copy_message_id"],
        admin_control_message_id=row["admin_control_message_id"],
        review_note=row["review_note"],
        created_at=row["created_at"],
        reviewed_at=row["reviewed_at"],
    )


def _audit_entry_from_row(row: sqlite3.Row) -> AuditEntry:
    return AuditEntry(
        id=row["id"],
        customer_id=row["customer_id"],
        telegram_user_id=row["telegram_user_id"],
        actor_user_id=row["actor_user_id"],
        actor_username=row["actor_username"],
        actor_full_name=row["actor_full_name"],
        action=row["action"],
        details=row["details"],
        created_at=row["created_at"],
    )


class Database:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.RLock()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def init(self) -> None:
        with self._lock, self._conn:
            self._conn.executescript(
                """
                PRAGMA foreign_keys = ON;

                CREATE TABLE IF NOT EXISTS app_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS customers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    telegram_user_id INTEGER NOT NULL UNIQUE,
                    chat_id INTEGER,
                    username TEXT,
                    full_name TEXT NOT NULL,
                    admin_name TEXT,
                    subscription_expires_on TEXT,
                    notes TEXT,
                    device_count INTEGER NOT NULL DEFAULT 1,
                    receipts_muted INTEGER NOT NULL DEFAULT 0,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS receipts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    customer_id INTEGER,
                    telegram_user_id INTEGER NOT NULL,
                    source_chat_id INTEGER NOT NULL,
                    source_message_id INTEGER NOT NULL,
                    kind TEXT NOT NULL,
                    caption TEXT,
                    file_id TEXT,
                    file_unique_id TEXT,
                    duplicate_of_receipt_id INTEGER,
                    status TEXT NOT NULL DEFAULT 'pending',
                    admin_chat_id INTEGER,
                    admin_copy_message_id INTEGER,
                    admin_control_message_id INTEGER,
                    review_note TEXT,
                    created_at TEXT NOT NULL,
                    reviewed_at TEXT,
                    UNIQUE(source_chat_id, source_message_id),
                    FOREIGN KEY(customer_id) REFERENCES customers(id) ON DELETE SET NULL,
                    FOREIGN KEY(duplicate_of_receipt_id) REFERENCES receipts(id) ON DELETE SET NULL
                );

                CREATE TABLE IF NOT EXISTS reminder_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    customer_id INTEGER NOT NULL,
                    reminder_key TEXT NOT NULL,
                    reminder_type TEXT NOT NULL,
                    sent_at TEXT NOT NULL,
                    UNIQUE(customer_id, reminder_key),
                    FOREIGN KEY(customer_id) REFERENCES customers(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS customer_audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    customer_id INTEGER,
                    telegram_user_id INTEGER NOT NULL,
                    actor_user_id INTEGER,
                    actor_username TEXT,
                    actor_full_name TEXT,
                    action TEXT NOT NULL,
                    details TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(customer_id) REFERENCES customers(id) ON DELETE SET NULL
                );
                """
            )
            customer_columns = self._table_columns("customers")
            if "device_count" not in customer_columns:
                self._conn.execute(
                    "ALTER TABLE customers ADD COLUMN device_count INTEGER NOT NULL DEFAULT 1"
                )
            if "receipts_muted" not in customer_columns:
                self._conn.execute(
                    "ALTER TABLE customers ADD COLUMN receipts_muted INTEGER NOT NULL DEFAULT 0"
                )
            if "admin_name" not in customer_columns:
                self._conn.execute(
                    "ALTER TABLE customers ADD COLUMN admin_name TEXT"
                )
            receipt_columns = self._table_columns("receipts")
            if "duplicate_of_receipt_id" not in receipt_columns:
                self._conn.execute(
                    "ALTER TABLE receipts ADD COLUMN duplicate_of_receipt_id INTEGER"
                )

    def _table_columns(self, table_name: str) -> set[str]:
        rows = self._conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        return {row["name"] for row in rows}

    def get_setting(self, key: str) -> str | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT value FROM app_settings WHERE key = ?",
                (key,),
            ).fetchone()
        return None if row is None else row["value"]

    def set_setting(self, key: str, value: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO app_settings(key, value)
                VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (key, value),
            )

    def get_customer_by_user_id(self, telegram_user_id: int) -> Customer | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM customers WHERE telegram_user_id = ?",
                (telegram_user_id,),
            ).fetchone()
        return None if row is None else _customer_from_row(row)

    def get_customer_by_id(self, customer_id: int) -> Customer | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM customers WHERE id = ?",
                (customer_id,),
            ).fetchone()
        return None if row is None else _customer_from_row(row)

    def upsert_customer_profile(
        self,
        *,
        telegram_user_id: int,
        chat_id: int | None,
        username: str | None,
        full_name: str,
    ) -> Customer:
        now = _utc_now_iso()
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO customers(
                    telegram_user_id, chat_id, username, full_name, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(telegram_user_id) DO UPDATE SET
                    chat_id = excluded.chat_id,
                    username = excluded.username,
                    full_name = excluded.full_name,
                    updated_at = excluded.updated_at
                """,
                (telegram_user_id, chat_id, username, full_name, now, now),
            )
        customer = self.get_customer_by_user_id(telegram_user_id)
        assert customer is not None
        return customer

    def ensure_customer_placeholder(self, telegram_user_id: int) -> Customer:
        existing = self.get_customer_by_user_id(telegram_user_id)
        if existing is not None:
            return existing

        now = _utc_now_iso()
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO customers(
                    telegram_user_id, full_name, created_at, updated_at
                )
                VALUES (?, ?, ?, ?)
                """,
                (telegram_user_id, f"Клиент {telegram_user_id}", now, now),
            )
        customer = self.get_customer_by_user_id(telegram_user_id)
        assert customer is not None
        return customer

    def set_subscription_expiry(self, telegram_user_id: int, expires_on: date) -> Customer:
        customer = self.ensure_customer_placeholder(telegram_user_id)
        now = _utc_now_iso()
        with self._lock, self._conn:
            self._conn.execute(
                """
                UPDATE customers
                SET subscription_expires_on = ?, is_active = 1, updated_at = ?
                WHERE telegram_user_id = ?
                """,
                (expires_on.isoformat(), now, telegram_user_id),
            )
        updated = self.get_customer_by_user_id(telegram_user_id)
        assert updated is not None
        return updated

    def set_customer_device_count(self, telegram_user_id: int, device_count: int) -> Customer:
        if device_count <= 0:
            raise ValueError("Количество устройств должно быть больше нуля.")

        self.ensure_customer_placeholder(telegram_user_id)
        now = _utc_now_iso()
        with self._lock, self._conn:
            self._conn.execute(
                """
                UPDATE customers
                SET device_count = ?, updated_at = ?
                WHERE telegram_user_id = ?
                """,
                (device_count, now, telegram_user_id),
            )
        updated = self.get_customer_by_user_id(telegram_user_id)
        assert updated is not None
        return updated

    def set_customer_admin_name(self, telegram_user_id: int, admin_name: str | None) -> Customer:
        self.ensure_customer_placeholder(telegram_user_id)
        normalized_name = None
        if admin_name is not None:
            stripped_name = admin_name.strip()
            normalized_name = stripped_name or None

        now = _utc_now_iso()
        with self._lock, self._conn:
            self._conn.execute(
                """
                UPDATE customers
                SET admin_name = ?, updated_at = ?
                WHERE telegram_user_id = ?
                """,
                (normalized_name, now, telegram_user_id),
            )
        updated = self.get_customer_by_user_id(telegram_user_id)
        assert updated is not None
        return updated

    def set_customer_receipts_muted(self, telegram_user_id: int, muted: bool) -> Customer:
        self.ensure_customer_placeholder(telegram_user_id)
        now = _utc_now_iso()
        with self._lock, self._conn:
            self._conn.execute(
                """
                UPDATE customers
                SET receipts_muted = ?, updated_at = ?
                WHERE telegram_user_id = ?
                """,
                (1 if muted else 0, now, telegram_user_id),
            )
        updated = self.get_customer_by_user_id(telegram_user_id)
        assert updated is not None
        return updated

    def deactivate_customer(self, telegram_user_id: int) -> Customer | None:
        customer = self.get_customer_by_user_id(telegram_user_id)
        if customer is None:
            return None

        now = _utc_now_iso()
        with self._lock, self._conn:
            self._conn.execute(
                """
                UPDATE customers
                SET is_active = 0,
                    subscription_expires_on = NULL,
                    updated_at = ?
                WHERE telegram_user_id = ?
                """,
                (now, telegram_user_id),
            )
        return self.get_customer_by_user_id(telegram_user_id)

    def create_receipt(
        self,
        *,
        telegram_user_id: int,
        customer_id: int | None,
        source_chat_id: int,
        source_message_id: int,
        kind: str,
        caption: str | None,
        file_id: str | None,
        file_unique_id: str | None,
    ) -> Receipt:
        now = _utc_now_iso()
        duplicate_of_receipt_id = None
        if file_unique_id:
            duplicate = self.get_latest_receipt_by_file_unique_id(file_unique_id)
            if duplicate is not None:
                duplicate_of_receipt_id = duplicate.id

        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO receipts(
                    customer_id,
                    telegram_user_id,
                    source_chat_id,
                    source_message_id,
                    kind,
                    caption,
                    file_id,
                    file_unique_id,
                    duplicate_of_receipt_id,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    customer_id,
                    telegram_user_id,
                    source_chat_id,
                    source_message_id,
                    kind,
                    caption,
                    file_id,
                    file_unique_id,
                    duplicate_of_receipt_id,
                    now,
                ),
            )
            receipt_id = self._conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        receipt = self.get_receipt_by_id(receipt_id)
        assert receipt is not None
        return receipt

    def get_receipt_by_id(self, receipt_id: int) -> Receipt | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM receipts WHERE id = ?",
                (receipt_id,),
            ).fetchone()
        return None if row is None else _receipt_from_row(row)

    def get_latest_receipt_by_file_unique_id(self, file_unique_id: str) -> Receipt | None:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT *
                FROM receipts
                WHERE file_unique_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (file_unique_id,),
            ).fetchone()
        return None if row is None else _receipt_from_row(row)

    def get_receipt_by_admin_message(self, admin_chat_id: int, message_id: int) -> Receipt | None:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT *
                FROM receipts
                WHERE admin_chat_id = ?
                  AND (admin_copy_message_id = ? OR admin_control_message_id = ?)
                ORDER BY id DESC
                LIMIT 1
                """,
                (admin_chat_id, message_id, message_id),
            ).fetchone()
        return None if row is None else _receipt_from_row(row)

    def attach_admin_messages(
        self,
        *,
        receipt_id: int,
        admin_chat_id: int,
        admin_copy_message_id: int,
        admin_control_message_id: int,
    ) -> Receipt:
        with self._lock, self._conn:
            self._conn.execute(
                """
                UPDATE receipts
                SET admin_chat_id = ?, admin_copy_message_id = ?, admin_control_message_id = ?
                WHERE id = ?
                """,
                (admin_chat_id, admin_copy_message_id, admin_control_message_id, receipt_id),
            )
        receipt = self.get_receipt_by_id(receipt_id)
        assert receipt is not None
        return receipt

    def mark_receipt_status(self, receipt_id: int, status: str, review_note: str | None = None) -> Receipt:
        now = _utc_now_iso()
        with self._lock, self._conn:
            self._conn.execute(
                """
                UPDATE receipts
                SET status = ?, review_note = ?, reviewed_at = ?
                WHERE id = ?
                """,
                (status, review_note, now, receipt_id),
            )
        receipt = self.get_receipt_by_id(receipt_id)
        assert receipt is not None
        return receipt

    def list_customers(self, limit: int = 50) -> list[Customer]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT *
                FROM customers
                WHERE is_active = 1
                  AND subscription_expires_on IS NOT NULL
                ORDER BY
                    subscription_expires_on ASC,
                    COALESCE(admin_name, full_name) COLLATE NOCASE ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [_customer_from_row(row) for row in rows]

    def list_broadcast_recipients(self, *, exclude_chat_id: int | None = None) -> list[Customer]:
        with self._lock:
            if exclude_chat_id is None:
                rows = self._conn.execute(
                    """
                    SELECT *
                    FROM customers
                    WHERE chat_id IS NOT NULL
                    ORDER BY id ASC
                    """
                ).fetchall()
            else:
                rows = self._conn.execute(
                    """
                    SELECT *
                    FROM customers
                    WHERE chat_id IS NOT NULL
                      AND chat_id != ?
                    ORDER BY id ASC
                    """,
                    (exclude_chat_id,),
                ).fetchall()
        return [_customer_from_row(row) for row in rows]

    def list_pending_receipts(self, limit: int = 20) -> list[Receipt]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT *
                FROM receipts
                WHERE status = 'pending'
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [_receipt_from_row(row) for row in rows]

    def list_receipts_for_user(self, telegram_user_id: int, limit: int = 5) -> list[Receipt]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT *
                FROM receipts
                WHERE telegram_user_id = ?
                ORDER BY created_at DESC, id DESC
                LIMIT ?
                """,
                (telegram_user_id, limit),
            ).fetchall()
        return [_receipt_from_row(row) for row in rows]

    def list_due_reminders(
        self,
        *,
        today: date,
        reminder_days_before: tuple[int, ...],
        overdue_interval_days: int,
    ) -> list[ReminderCandidate]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT *
                FROM customers
                WHERE is_active = 1
                  AND subscription_expires_on IS NOT NULL
                """
            ).fetchall()

        candidates: list[ReminderCandidate] = []
        for row in rows:
            customer = _customer_from_row(row)
            expires_on = customer.subscription_expires_on
            if expires_on is None:
                continue

            decision = decide_reminder(
                expires_on=expires_on,
                today=today,
                reminder_days_before=reminder_days_before,
                overdue_interval_days=overdue_interval_days,
            )
            if decision is None:
                continue

            if self.reminder_exists(customer.id, decision.reminder_key):
                continue

            candidates.append(ReminderCandidate(customer=customer, decision=decision))

        return candidates

    def reminder_exists(self, customer_id: int, reminder_key: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT 1
                FROM reminder_history
                WHERE customer_id = ? AND reminder_key = ?
                """,
                (customer_id, reminder_key),
            ).fetchone()
        return row is not None

    def record_reminder(self, *, customer_id: int, reminder_key: str, reminder_type: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT OR IGNORE INTO reminder_history(customer_id, reminder_key, reminder_type, sent_at)
                VALUES (?, ?, ?, ?)
                """,
                (customer_id, reminder_key, reminder_type, _utc_now_iso()),
            )

    def record_customer_audit(
        self,
        *,
        telegram_user_id: int,
        action: str,
        details: str,
        actor_user_id: int | None,
        actor_username: str | None,
        actor_full_name: str | None,
    ) -> AuditEntry:
        customer = self.get_customer_by_user_id(telegram_user_id)
        customer_id = customer.id if customer is not None else None
        now = _utc_now_iso()
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO customer_audit_log(
                    customer_id,
                    telegram_user_id,
                    actor_user_id,
                    actor_username,
                    actor_full_name,
                    action,
                    details,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    customer_id,
                    telegram_user_id,
                    actor_user_id,
                    actor_username,
                    actor_full_name,
                    action,
                    details,
                    now,
                ),
            )
            audit_id = self._conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            row = self._conn.execute(
                "SELECT * FROM customer_audit_log WHERE id = ?",
                (audit_id,),
            ).fetchone()
        assert row is not None
        return _audit_entry_from_row(row)

    def list_customer_audit_entries(self, telegram_user_id: int, limit: int = 5) -> list[AuditEntry]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT *
                FROM customer_audit_log
                WHERE telegram_user_id = ?
                ORDER BY created_at DESC, id DESC
                LIMIT ?
                """,
                (telegram_user_id, limit),
            ).fetchall()
        return [_audit_entry_from_row(row) for row in rows]
