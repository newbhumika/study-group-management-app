import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "study_groups.db"


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def init_db() -> None:
    conn = get_connection()
    cur = conn.cursor()

    cur.executescript(
        """
        CREATE TABLE IF NOT EXISTS students (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT NOT NULL UNIQUE,
            preferred_group_size INTEGER NOT NULL DEFAULT 3,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS courses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS student_courses (
            student_id INTEGER NOT NULL,
            course_id INTEGER NOT NULL,
            PRIMARY KEY (student_id, course_id),
            FOREIGN KEY (student_id) REFERENCES students(id) ON DELETE CASCADE,
            FOREIGN KEY (course_id) REFERENCES courses(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS timeslots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            label TEXT NOT NULL UNIQUE,
            day_of_week TEXT NOT NULL,
            start_time TEXT NOT NULL,
            end_time TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS student_availability (
            student_id INTEGER NOT NULL,
            timeslot_id INTEGER NOT NULL,
            PRIMARY KEY (student_id, timeslot_id),
            FOREIGN KEY (student_id) REFERENCES students(id) ON DELETE CASCADE,
            FOREIGN KEY (timeslot_id) REFERENCES timeslots(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS study_groups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            course_id INTEGER NOT NULL,
            group_index INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (course_id) REFERENCES courses(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS study_group_members (
            group_id INTEGER NOT NULL,
            student_id INTEGER NOT NULL,
            PRIMARY KEY (group_id, student_id),
            FOREIGN KEY (group_id) REFERENCES study_groups(id) ON DELETE CASCADE,
            FOREIGN KEY (student_id) REFERENCES students(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_student_courses_student ON student_courses(student_id);
        CREATE INDEX IF NOT EXISTS idx_student_courses_course ON student_courses(course_id);
        CREATE INDEX IF NOT EXISTS idx_student_availability_student ON student_availability(student_id);
        CREATE INDEX IF NOT EXISTS idx_student_availability_timeslot ON student_availability(timeslot_id);
        CREATE INDEX IF NOT EXISTS idx_study_groups_course ON study_groups(course_id);
        """
    )

    # Seed a small set of default courses and timeslots for convenience.
    # These inserts are idempotent thanks to UNIQUE constraints and INSERT OR IGNORE.
    cur.executemany(
        "INSERT OR IGNORE INTO courses (code, name) VALUES (?, ?)",
        [
            ("CS101", "Intro to Computer Science"),
            ("MATH201", "Discrete Mathematics"),
            ("PHYS150", "General Physics"),
        ],
    )

    cur.executemany(
        """
        INSERT OR IGNORE INTO timeslots (label, day_of_week, start_time, end_time)
        VALUES (?, ?, ?, ?)
        """,
        [
            ("Mon 10-12", "Mon", "10:00", "12:00"),
            ("Mon 14-16", "Mon", "14:00", "16:00"),
            ("Tue 10-12", "Tue", "10:00", "12:00"),
            ("Tue 14-16", "Tue", "14:00", "16:00"),
            ("Wed 10-12", "Wed", "10:00", "12:00"),
            ("Wed 14-16", "Wed", "14:00", "16:00"),
        ],
    )

    conn.commit()
    conn.close()


def fetch_all(query: str, params: Iterable[Any] = ()) -> List[Dict[str, Any]]:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(query, tuple(params))
    rows = cur.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def fetch_one(query: str, params: Iterable[Any] = ()) -> Optional[Dict[str, Any]]:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(query, tuple(params))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def execute(query: str, params: Iterable[Any] = ()) -> int:
    """Execute a write query and return last row id if applicable."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(query, tuple(params))
    conn.commit()
    lastrowid = cur.lastrowid
    conn.close()
    return lastrowid


def executemany(query: str, seq_of_params: Iterable[Iterable[Any]]) -> None:
    conn = get_connection()
    cur = conn.cursor()
    cur.executemany(query, [tuple(p) for p in seq_of_params])
    conn.commit()
    conn.close()
