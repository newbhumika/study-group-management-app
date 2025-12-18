from collections import defaultdict
from statistics import median
from typing import Dict, List, Set, Tuple

from . import db
from .models import Student


def load_students_by_course() -> Dict[int, List[Student]]:
    """Load students and group them by course_id.

    Returns a dict: {course_id: [Student, ...]}
    """
    # Get all students
    students_rows = db.fetch_all(
        "SELECT id, name, email, preferred_group_size FROM students"
    )
    if not students_rows:
        return {}

    # Map student_id -> base info
    students_base: Dict[int, Student] = {}
    for row in students_rows:
        students_base[row["id"]] = Student(
            id=row["id"],
            name=row["name"],
            email=row["email"],
            preferred_group_size=row["preferred_group_size"],
            course_ids=[],
            availability_timeslot_ids=set(),
        )

    # Load course memberships
    course_rows = db.fetch_all(
        "SELECT student_id, course_id FROM student_courses"
    )
    for row in course_rows:
        sid = row["student_id"]
        cid = row["course_id"]
        if sid in students_base:
            students_base[sid].course_ids.append(cid)

    # Load availability
    avail_rows = db.fetch_all(
        "SELECT student_id, timeslot_id FROM student_availability"
    )
    for row in avail_rows:
        sid = row["student_id"]
        tid = row["timeslot_id"]
        if sid in students_base:
            students_base[sid].availability_timeslot_ids.add(tid)

    # Group by course
    by_course: Dict[int, List[Student]] = defaultdict(list)
    for student in students_base.values():
        for cid in student.course_ids:
            by_course[cid].append(student)

    return by_course


def compatibility(a: Student, b: Student) -> int:
    """Compute compatibility score between two students.

    - Base +5 points (same course context; could be tuned per course later)
    - +1 per overlapping availability timeslot
    - Small penalty for very different preferred_group_size
    """
    base_score = 5
    overlap = len(a.availability_timeslot_ids & b.availability_timeslot_ids)
    size_diff = abs(a.preferred_group_size - b.preferred_group_size)
    penalty = 0
    if size_diff >= 2:
        penalty = size_diff - 1
    return max(0, base_score + overlap - penalty)


def build_compatibility_matrix(students: List[Student]) -> Dict[Tuple[int, int], int]:
    scores: Dict[Tuple[int, int], int] = {}
    for i, s1 in enumerate(students):
        for j in range(i + 1, len(students)):
            s2 = students[j]
            score = compatibility(s1, s2)
            scores[(s1.id, s2.id)] = score
            scores[(s2.id, s1.id)] = score
    return scores


def form_groups_for_course(students: List[Student]) -> List[List[Student]]:
    if not students:
        return []
    if len(students) == 1:
        # Leave singleton as its own group (can be handled specially in UI)
        return [students]

    # Determine target group size based on median preference, clamped
    prefs = [max(2, min(5, s.preferred_group_size)) for s in students]
    try:
        target_size = int(median(prefs))
    except Exception:
        target_size = 3
    target_size = max(2, min(5, target_size))

    scores = build_compatibility_matrix(students)

    unassigned: Set[int] = {s.id for s in students}
    by_id: Dict[int, Student] = {s.id: s for s in students}
    groups: List[List[Student]] = []

    def avg_compat(candidate_id: int, current_ids: List[int]) -> float:
        if not current_ids:
            return 0.0
        total = 0
        for cid in current_ids:
            total += scores.get((candidate_id, cid), 0)
        return total / len(current_ids)

    while unassigned:
        # Pick seed: student with highest total compatibility with others
        best_seed_id = None
        best_seed_total = -1
        for sid in list(unassigned):
            total = 0
            for other in unassigned:
                if other == sid:
                    continue
                total += scores.get((sid, other), 0)
            if total > best_seed_total:
                best_seed_total = total
                best_seed_id = sid

        if best_seed_id is None:
            break

        current_group_ids: List[int] = [best_seed_id]
        unassigned.remove(best_seed_id)

        # Greedily add members with highest average compatibility
        while len(current_group_ids) < target_size and unassigned:
            best_candidate = None
            best_score = -1.0
            for sid in list(unassigned):
                score = avg_compat(sid, current_group_ids)
                if score > best_score:
                    best_score = score
                    best_candidate = sid

            # If no positive compatibility, still add one student to avoid being stuck
            if best_candidate is None:
                best_candidate = next(iter(unassigned))

            current_group_ids.append(best_candidate)
            unassigned.remove(best_candidate)

        groups.append([by_id[sid] for sid in current_group_ids])

    # If last group is very small (size 1) and there is another group, merge
    if len(groups) >= 2 and len(groups[-1]) == 1:
        lone = groups.pop()
        # merge lone student into group with best average compatibility
        lone_student = lone[0]
        best_group_idx = 0
        best_group_score = -1
        for idx, group in enumerate(groups):
            if not group:
                continue
            total = sum(compatibility(lone_student, member) for member in group)
            avg = total / len(group)
            if avg > best_group_score:
                best_group_score = avg
                best_group_idx = idx
        groups[best_group_idx].append(lone_student)

    return groups


def run_matching() -> Dict[int, List[List[int]]]:
    """Run matching for all courses.

    Returns a mapping: {course_id: [[student_ids...], ...]}
    Also persists study_groups and study_group_members tables.
    """
    by_course = load_students_by_course()
    if not by_course:
        return {}

    # Clear old groups
    db.execute("DELETE FROM study_group_members")
    db.execute("DELETE FROM study_groups")

    result: Dict[int, List[List[int]]] = {}

    for course_id, students in by_course.items():
        groups = form_groups_for_course(students)
        result[course_id] = []
        group_index = 1
        for group in groups:
            group_id = db.execute(
                "INSERT INTO study_groups (course_id, group_index) VALUES (?, ?)",
                (course_id, group_index),
            )
            member_ids = [s.id for s in group]
            db.executemany(
                "INSERT INTO study_group_members (group_id, student_id) VALUES (?, ?)",
                [(group_id, sid) for sid in member_ids],
            )
            result[course_id].append(member_ids)
            group_index += 1

    return result
