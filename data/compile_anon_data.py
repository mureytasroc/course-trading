"""
NOTE: This export script only works in a `python manage.py shell` context in the
open source Penn Courses Django app:
https://github.com/pennlabs/penn-courses

It is used to export an approximation of anonymous user course registrations,
watched sections on Penn Course Alert, and detailed section information.

Exported Files (each of these dictionaries is parameterized by semester):
1. 'watching.pkl': This file contains a dictionary mapping anonymized student numbers
   to a list of watched sections at the end of a specified semester. The sections are 
   in the chronological order of when each student began monitoring them on Penn Course Alert.

2. 'section_info.pkl': This file contains a dictionary mapping each section's full code
   to a dictionary of its details. These details include activity type (as a string),
   current enrollment count (int), capacity (int), open status (bool), permit requirement
   status (bool), and meeting times. Meeting times are further detailed in a nested
   dictionary that includes the day of the week (string), start time (float, formatted as hh.mm),
   and end time (float, formatted as hh.mm).

3. 'estimated-registration.pkl': This file contains a dictionary mapping anonymized student numbers
   to a set of sections, representing an estimated course registration for that semester.
   This data approximates the course selections that each student might have made during
   the registration period.
"""


import os
import pickle
import random
from collections import defaultdict
from datetime import timedelta

from django.db.models import F, OuterRef, Q, Subquery

from alert.models import Registration  # Penn Course Alert subscription
from alert.models import AddDropPeriod
from courses.models import Course, NGSSRestriction, PreNGSSRestriction, Section
from PennCourses.settings.base import FIRST_BANNER_SEM
from plan.models import Schedule  # Penn Course Plan schedule


all_section_info = dict()
all_watching = dict()
all_est_registration = dict()

for semester in ["2020C", "2021A", "2021C", "2022A", "2022C", "2023A", "2023C"]:
    adp = AddDropPeriod.objects.get(semester=semester)
    snapshot_date = adp.estimated_end - timedelta(days=1)

    # Get a map from primary course id (integer) -> full code (e.g. CIS-120)
    primary_course_full_codes = dict(
        Course.objects.filter(semester=semester, primary_listing_id=F("id")).values_list(
            "id", "full_code"
        )
    )

    # Section registration restriction, used to determine if permit is required
    Restriction = NGSSRestriction if semester >= FIRST_BANNER_SEM else PreNGSSRestriction
    permit_required_ids = set(Restriction.special_approval().values_list("sections__id", flat=True))

    section_info = {
        s.full_code: {
            "activity": dict(Section.ACTIVITY_CHOICES)[s.activity],
            "meetings": [
                {
                    "day": m.day,
                    "start": m.start,  # hh:mm is formatted as hh.mm = h+mm/100
                    "end": m.end,  # hh:mm is formatted as hh.mm = h+mm/100
                }
                for m in s.meetings.all()
            ],
            "enrollment": s.enrollment,
            "capacity": s.capacity,
            "open": s.status == "O",
            "permit_required": s.id in permit_required_ids,
        }
        for s in Section.objects.filter(
            ~Q(status="X"),
            ~Q(activity=""),
            course__semester=semester,
            course__primary_listing_id=F("course_id"),  # exclude crosslistings
        )
    }

    valid_sections = set(section_info.keys())

    # Get active Penn Course Alert subscriptions for each student in this semester
    active_subscriptions = (
        Registration.objects.filter(
            Q(notification_sent_at__isnull=True) | Q(notification_sent_at__gt=snapshot_date),
            Q(cancelled_at__isnull=True) | Q(cancelled_at__gt=snapshot_date),
            Q(deleted_at__isnull=True) | Q(deleted_at__gt=snapshot_date),
            created_at__lte=snapshot_date,
            section__course__semester=semester,
        )
        .values_list("user_id", "section__code", "section__course__primary_listing_id")
        .order_by("original_created_at")
    )
    watching = defaultdict(list)
    for user_id, code, primary_listing_id in active_subscriptions:
        full_code = primary_course_full_codes[primary_listing_id] + f"-{code}"
        if full_code not in valid_sections:
            continue
        watching[user_id].append(full_code)

    # Get the most recently updated Penn Course Plan schedule for each student in this semester
    latest_schedules = (
        Schedule.objects.filter(semester=semester)
        .annotate(
            max_updated_at=Subquery(
                Schedule.objects.filter(person_id=OuterRef("person_id"), semester=semester)
                .order_by("-updated_at")
                .values("updated_at")[:1]
            )
        )
        .filter(updated_at=F("max_updated_at"))
        .prefetch_related("sections", "sections__course")
    )

    # Get map from student to their estimated registration in this semester
    # (defined as their latest-updated Penn Course Plan schedule,
    #  minus sections watched on Penn Course Alert)
    est_registration = {}
    for schedule in latest_schedules:
        user_id = schedule.person_id
        sections = {
            primary_course_full_codes[s.course.primary_listing_id] + f"-{s.code}"
            for s in schedule.sections.all()
        }
        sections &= valid_sections
        est_registration[user_id] = sections - set(watching[user_id])

    # Anonymize user IDs by shuffling and taking index in list as new ID
    student_ids = list(set(watching.keys()) | set(est_registration.keys()))
    random.shuffle(student_ids)
    anon_num = {old_id: i for i, old_id in enumerate(student_ids)}
    watching = {anon_num[user_id]: v for user_id, v in watching.items()}
    est_registration = {anon_num[user_id]: v for user_id, v in est_registration.items()}

    all_section_info[semester] = section_info
    all_watching[semester] = watching
    all_est_registration[semester] = est_registration

# Export watching[semester] (map from anon student # ->
#   list of watched sections at the end of this semester, in chronological order of watching initiation)
with open(os.path.expanduser("~/git/course-trading/data/watching.pkl"), "wb") as file:
    pickle.dump(all_watching, file)

# Export section_info[semester] (map from section full_code ->
#   {activity: string, enrollment: int, capacity: int, open: bool, permit_required: bool,
#    meetings: { day: string,
#                start: float,  # hh:mm is formatted as hh.mm = hh+mm/100
#                end: float,     # hh:mm is formatted as hh.mm = hh+mm/100
#               }
#   })
with open(os.path.expanduser("~/git/course-trading/data/section_info.pkl"), "wb") as file:
    pickle.dump(all_section_info, file)

# Export est_registration[semester] (map from anon student # ->
#   set of sections estimated to be their Path registration for this semester)
with open(os.path.expanduser("~/git/course-trading/data/estimated-registration.pkl"), "wb") as file:
    pickle.dump(all_est_registration, file)
