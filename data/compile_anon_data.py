"""
NOTE: this export script only works in a `python manage.py shell` context in the
open source Penn Courses Django app:
https://github.com/pennlabs/penn-courses

It is used to export an approximation of anonymous user course registrations,
as well as watched sections on Penn Course Alert.

Exported Files:
1. 'watching.pkl': This file contains a dict mapping anonymized student # to set of 
   watched sections (from Penn Course Alert). The data represents the sections that
   each student was monitoring at the end of the fall 2023 semester.

2. 'estimated-registration.pkl': This file contains a dict mapping anonymized student # 
   to set of sections (estimated course registration).
"""


import os
import pickle
import random
from collections import defaultdict
from datetime import datetime

import pytz
from django.db.models import F, Max, OuterRef, Q, Subquery, Value

from alert.models import Registration  # Penn Course Alert subscription
from plan.models import Schedule  # Penn Course Plan schedule
from courses.models import Section  # Course section


semester = "2023C"  # fall 2023
et_tz = pytz.timezone("America/New_York")
snapshot_date = et_tz.localize(
    datetime(2023, 9, 11, 23, 0, 0)
)  # 11pm ET on 2023/09/11 (day before add/drop deadline)

# Get the most recently updated Penn Course Plan schedule for each student in this semester
latest_schedules = (
    Schedule.objects.filter(semester=semester)
    .annotate(
        max_updated_at=Subquery(
            Schedule.objects.filter(person_id=OuterRef("person_id"))
            .order_by("-semester")
            .values("updated_at")[:1]
        )
    )
    .filter(updated_at=F("max_updated_at"))
    .prefetch_related("sections")
)

# Get active Penn Course Alert subscriptions for each student in this semester
active_subscriptions = Registration.objects.filter(
    Q(notification_sent_at__isnull=True) | Q(notification_sent_at__gt=snapshot_date),
    Q(cancelled_at__isnull=True) | Q(cancelled_at__gt=snapshot_date),
    Q(deleted_at__isnull=True) | Q(deleted_at__gt=snapshot_date),
    created_at__lt=snapshot_date,
).values_list("user_id", "section__full_code")
watching = defaultdict(set)
for user_id, section in active_subscriptions:
    watching[user_id].add(section)

# Get map from student to their estimated registration in this semester
# (defined as their latest-updated Penn Course Plan schedule,
#  minus sections watched on Penn Course Alert)
est_registration = {}
for schedule in latest_schedules:
    user_id = schedule.person_id
    sections = set(schedule.sections.values_list("full_code", flat=True))
    est_registration[user_id] = sections - watching[user_id]

# Anonymize user IDs by shuffling and taking index in list as new ID
student_ids = list(set(watching.keys()) | set(est_registration.keys()))
random.shuffle(student_ids)
anon_num = {old_id: i for i, old_id in enumerate(student_ids)}
watching = {anon_num[user_id]: v for user_id, v in watching.items()}
est_registration = {anon_num[user_id]: v for user_id, v in est_registration.items()}

section_info = {
    s["full_code"]: {
        "activity": dict(Section.ACTIVITY_CHOICES)[s["activity"]],
        "capacity": s["capacity"],
        "open": s["status"] == "O",
    }
    for s in Section.objects.filter(course__semester=semester).values(
        "full_code", "activity", "capacity", "status"
    )
}

# Export watching (map from anon student # -> set of watched sections at the end of this semester)
with open(os.path.expanduser("~/git/course-trading/data/watching.pkl"), "wb") as file:
    pickle.dump(watching, file)

# Export section_info (map from section full_code -> {activity: string, capacity: int, open: bool})
with open(os.path.expanduser("~/git/course-trading/data/section_info.pkl"), "wb") as file:
    pickle.dump(section_info, file)

# Export est_registration (map from anon student # ->
#   set of sections estimated to be their Path registration for this semester)
with open(os.path.expanduser("~/git/course-trading/data/estimated-registration.pkl"), "wb") as file:
    pickle.dump(est_registration, file)
