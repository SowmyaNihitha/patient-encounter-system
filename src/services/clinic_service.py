from datetime import timedelta, timezone
from sqlalchemy.orm import Session
from fastapi import HTTPException

from src.models.models import Doctor, Appointment


def _to_utc(dt):
    """
    Normalize datetime to timezone-aware UTC.
    SQLite returns naive datetimes; MySQL may return aware ones.
    """
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def check_overlap(db: Session, doctor_id: int, start_time, duration: int) -> bool:
    """
    Database-agnostic, timezone-safe overlap detection.
    Works for SQLite (CI) and MySQL (prod).
    """

    start_time = _to_utc(start_time)
    new_end = start_time + timedelta(minutes=duration)

    existing_appointments = (
        db.query(Appointment).filter(Appointment.doctor_id == doctor_id).all()
    )

    for appt in existing_appointments:
        existing_start = _to_utc(appt.start_time)
        existing_end = existing_start + timedelta(minutes=appt.duration_minutes)

        # Overlap condition:
        # existing_start < new_end AND existing_end > new_start
        if existing_start < new_end and existing_end > start_time:
            return True

    return False


def create_appointment(db: Session, obj_in):
    """
    Create an appointment after enforcing:
    - Doctor exists and is active
    - No overlapping appointments
    """

    doctor = db.query(Doctor).filter(Doctor.id == obj_in.doctor_id).first()

    if not doctor or not doctor.is_active:
        raise HTTPException(
            status_code=400,
            detail="Doctor not found or inactive",
        )

    if check_overlap(
        db,
        obj_in.doctor_id,
        obj_in.start_time,
        obj_in.duration_minutes,
    ):
        raise HTTPException(
            status_code=409,
            detail="Appointment overlap detected",
        )

    # Pydantic v2 compatible
    db_obj = Appointment(**obj_in.model_dump())

    db.add(db_obj)
    db.commit()
    db.refresh(db_obj)
    return db_obj
