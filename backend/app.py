
from __future__ import annotations

from typing import Any, Dict, List
from pathlib import Path

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

from . import db, matching


def create_app() -> Flask:
    app = Flask(__name__)
    CORS(app)

    # Ensure schema and seed data exist
    db.init_db()

    base_dir = Path(__file__).resolve().parent.parent
    frontend_dir = base_dir / "frontend"

    # --- Frontend assets ---

    @app.get("/")
    def index() -> Any:
        """Serve the main frontend page."""
        return send_from_directory(frontend_dir, "index.html")

    @app.get("/app.js")
    def app_js() -> Any:
        return send_from_directory(frontend_dir, "app.js")

    @app.get("/styles.css")
    def styles_css() -> Any:
        return send_from_directory(frontend_dir, "styles.css")

    @app.get("/api/health")
    def health() -> Any:
        return {"status": "ok"}

    # --- Reference data ---

    @app.get("/api/courses")
    def get_courses() -> Any:
        courses = db.fetch_all("SELECT id, code, name FROM courses ORDER BY code")
        return jsonify(courses)

    @app.get("/api/timeslots")
    def get_timeslots() -> Any:
        timeslots = db.fetch_all(
            "SELECT id, label, day_of_week, start_time, end_time FROM timeslots ORDER BY day_of_week, start_time"
        )
        return jsonify(timeslots)

    # --- Students ---

    @app.get("/api/students")
    def list_students() -> Any:
        students = db.fetch_all(
            "SELECT id, name, email, preferred_group_size FROM students ORDER BY created_at DESC"
        )

        if not students:
            return jsonify([])

        # attach course_ids and availability for convenience
        student_ids = [s["id"] for s in students]

        courses_rows = db.fetch_all(
            "SELECT student_id, course_id FROM student_courses WHERE student_id IN (%s)"
            % ",".join(["?"] * len(student_ids)),
            student_ids,
        )
        times_rows = db.fetch_all(
            "SELECT student_id, timeslot_id FROM student_availability WHERE student_id IN (%s)"
            % ",".join(["?"] * len(student_ids)),
            student_ids,
        )

        by_student_courses: Dict[int, List[int]] = {sid: [] for sid in student_ids}
        for row in courses_rows:
            by_student_courses.setdefault(row["student_id"], []).append(row["course_id"])

        by_student_times: Dict[int, List[int]] = {sid: [] for sid in student_ids}
        for row in times_rows:
            by_student_times.setdefault(row["student_id"], []).append(row["timeslot_id"])

        enriched: List[Dict[str, Any]] = []
        for s in students:
            enriched.append(
                {
                    **s,
                    "course_ids": by_student_courses.get(s["id"], []),
                    "availability_timeslot_ids": by_student_times.get(s["id"], []),
                }
            )

        return jsonify(enriched)

    @app.post("/api/students")
    def upsert_student() -> Any:
        data = request.get_json(force=True) or {}

        name = (data.get("name") or "").strip()
        email = (data.get("email") or "").strip().lower()
        preferred_group_size = int(data.get("preferred_group_size") or 3)
        course_ids = data.get("course_ids") or []
        availability_ids = data.get("availability_timeslot_ids") or []

        if not name or not email:
            return jsonify({"error": "name and email are required"}), 400

        # Clamp preferred group size
        preferred_group_size = max(2, min(5, preferred_group_size))

        existing = db.fetch_one("SELECT id FROM students WHERE email = ?", (email,))
        if existing:
            student_id = existing["id"]
            db.execute(
                "UPDATE students SET name = ?, preferred_group_size = ? WHERE id = ?",
                (name, preferred_group_size, student_id),
            )
            # Clear old relations
            db.execute("DELETE FROM student_courses WHERE student_id = ?", (student_id,))
            db.execute(
                "DELETE FROM student_availability WHERE student_id = ?", (student_id,)
            )
        else:
            student_id = db.execute(
                "INSERT INTO students (name, email, preferred_group_size) VALUES (?, ?, ?)",
                (name, email, preferred_group_size),
            )

        # (Re)insert course memberships
        if course_ids:
            values = [(student_id, int(cid)) for cid in course_ids]
            db.executemany(
                "INSERT OR IGNORE INTO student_courses (student_id, course_id) VALUES (?, ?)",
                values,
            )

        # (Re)insert availability
        if availability_ids:
            values = [(student_id, int(tid)) for tid in availability_ids]
            db.executemany(
                "INSERT OR IGNORE INTO student_availability (student_id, timeslot_id) VALUES (?, ?)",
                values,
            )

        student = db.fetch_one(
            "SELECT id, name, email, preferred_group_size FROM students WHERE id = ?",
            (student_id,),
        )

        return jsonify(student), 201

    # --- Matching & groups ---

    @app.post("/api/match")
    def run_match() -> Any:
        """Run matching and return detailed group structures."""

        matching_result = matching.run_matching()
        if not matching_result:
            return jsonify({"groups": []})

        # Load groups from DB to build a human-friendly response
        group_rows = db.fetch_all(
            """
            SELECT sg.id AS group_id,
                   sg.course_id,
                   sg.group_index,
                   c.code AS course_code,
                   c.name AS course_name
            FROM study_groups sg
            JOIN courses c ON c.id = sg.course_id
            ORDER BY c.code, sg.group_index
            """
        )

        if not group_rows:
            return jsonify({"groups": []})

        member_rows = db.fetch_all(
            """
            SELECT sgm.group_id,
                   s.id AS student_id,
                   s.name,
                   s.email
            FROM study_group_members sgm
            JOIN students s ON s.id = sgm.student_id
            ORDER BY s.name
            """
        )

        members_by_group: Dict[int, List[Dict[str, Any]]] = {}
        for row in member_rows:
            members_by_group.setdefault(row["group_id"], []).append(
                {
                    "id": row["student_id"],
                    "name": row["name"],
                    "email": row["email"],
                }
            )

        groups: List[Dict[str, Any]] = []
        for row in group_rows:
            gid = row["group_id"]
            groups.append(
                {
                    "group_id": gid,
                    "course_id": row["course_id"],
                    "course_code": row["course_code"],
                    "course_name": row["course_name"],
                    "group_index": row["group_index"],
                    "members": members_by_group.get(gid, []),
                }
            )

        return jsonify({"groups": groups})

    @app.get("/api/groups")
    def get_groups() -> Any:
        """Fetch the latest groups without rerunning the algorithm."""

        group_rows = db.fetch_all(
            """
            SELECT sg.id AS group_id,
                   sg.course_id,
                   sg.group_index,
                   c.code AS course_code,
                   c.name AS course_name
            FROM study_groups sg
            JOIN courses c ON c.id = sg.course_id
            ORDER BY c.code, sg.group_index
            """
        )

        if not group_rows:
            return jsonify({"groups": []})

        member_rows = db.fetch_all(
            """
            SELECT sgm.group_id,
                   s.id AS student_id,
                   s.name,
                   s.email
            FROM study_group_members sgm
            JOIN students s ON s.id = sgm.student_id
            ORDER BY s.name
            """
        )

        members_by_group: Dict[int, List[Dict[str, Any]]] = {}
        for row in member_rows:
            members_by_group.setdefault(row["group_id"], []).append(
                {
                    "id": row["student_id"],
                    "name": row["name"],
                    "email": row["email"],
                }
            )

        groups: List[Dict[str, Any]] = []
        for row in group_rows:
            gid = row["group_id"]
            groups.append(
                {
                    "group_id": gid,
                    "course_id": row["course_id"],
                    "course_code": row["course_code"],
                    "course_name": row["course_name"],
                    "group_index": row["group_index"],
                    "members": members_by_group.get(gid, []),
                }
            )

        return jsonify({"groups": groups})

    return app


app = create_app()


if __name__ == "__main__":  # pragma: no cover
    app.run(debug=True)
