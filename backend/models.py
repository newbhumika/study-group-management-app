from dataclasses import dataclass
from typing import List, Set


@dataclass
class Student:
    id: int
    name: str
    email: str
    preferred_group_size: int
    course_ids: List[int]
    availability_timeslot_ids: Set[int]


@dataclass
class Course:
    id: int
    code: str
    name: str


@dataclass
class TimeSlot:
    id: int
    label: str
    day_of_week: str
    start_time: str
    end_time: str
