#!/usr/bin/env python3
from __future__ import annotations

import argparse
import dataclasses
import hashlib
import re
import sys
from datetime import date, datetime, time, timedelta, timezone
from typing import Optional, Tuple, List

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateutil_parser
from zoneinfo import ZoneInfo


DEFAULT_URL = "https://www.sfmta.com/notices/current-special-event-parking-regulations-schedule"
DEFAULT_OUTPUT = "sfmta_special_event_parking.ics"
DEFAULT_CALNAME = "SFMTA Special Event Parking"
LOCAL_TZID = "America/Los_Angeles"
LOCAL_TZ = ZoneInfo(LOCAL_TZID)

MONTHS = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}


@dataclasses.dataclass(frozen=True)
class ParsedRow:
    month: int
    day: int
    venue: str
    hours: str


@dataclasses.dataclass(frozen=True)
class CalendarEvent:
    uid: str
    summary: str
    description: str
    details: Tuple[Tuple[str, str], ...]
    location: str
    start_local: Optional[datetime]
    end_local: Optional[datetime]
    all_day_date: Optional[date]


def _collapse_ws(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _extract_effective_year(page_text: str, fallback_year: int) -> int:
    match = re.search(r"Effective Date.*?\b(20\d{2})\b", page_text, flags=re.IGNORECASE | re.DOTALL)
    if match:
        return int(match.group(1))
    return fallback_year


def _extract_rows_from_tables(soup: BeautifulSoup) -> list[ParsedRow]:
    rows: list[ParsedRow] = []
    for table in soup.find_all("table"):
        header_text = _collapse_ws(table.get_text(" ", strip=True)).lower()
        if "event date" not in header_text or "venue" not in header_text or "rate hours" not in header_text:
            continue

        for tr in table.find_all("tr"):
            cells = [
                _collapse_ws(cell.get_text(" ", strip=True))
                for cell in tr.find_all(["th", "td"])
                if _collapse_ws(cell.get_text(" ", strip=True))
            ]
            if len(cells) < 3:
                continue
            if cells[0].lower() == "event date":
                continue
            parsed = _parse_row_fields(cells[0], cells[1], cells[2])
            if parsed:
                rows.append(parsed)

        if rows:
            return rows
    return []


def _parse_row_fields(event_date: str, venue: str, hours: str) -> Optional[ParsedRow]:
    event_date = _collapse_ws(event_date)
    venue = _collapse_ws(venue)
    hours = _collapse_ws(hours)

    month_match = re.match(r"^([A-Za-z]+)\s*(\d{1,2})\b", event_date)
    if not month_match:
        return None
    month_name = month_match.group(1).lower()
    if month_name not in MONTHS:
        return None
    month = MONTHS[month_name]
    day = int(month_match.group(2))
    return ParsedRow(month=month, day=day, venue=venue, hours=hours)


def _extract_rows_from_text(page_text: str) -> list[ParsedRow]:
    normalized = _collapse_ws(page_text)
    month_re = r"(January|February|March|April|May|June|July|August|September|October|November|December)"
    venue_re = r"(Oracle(?: Park)?|Chase Center)"
    time_token = r"(?:Noon|Midnight|\d{1,2}(?::\d{2})?\s*(?:A\.?M\.?|P\.?M\.?)?)"
    hours_re = rf"{time_token}\s+to\s+{time_token}"
    row_re = rf"\b{month_re}\s*(\d{{1,2}})\s+{venue_re}\s+({hours_re})\b"

    rows: list[ParsedRow] = []
    for match in re.finditer(row_re, normalized, flags=re.IGNORECASE):
        month_name = match.group(1).lower()
        if month_name not in MONTHS:
            continue
        rows.append(
            ParsedRow(
                month=MONTHS[month_name],
                day=int(match.group(2)),
                venue=_collapse_ws(match.group(3)),
                hours=_collapse_ws(match.group(4)),
            )
        )
    return rows


def _parse_hours_range(hours: str) -> Optional[Tuple[time, time]]:
    parts = re.split(r"\s+to\s+", _collapse_ws(hours), flags=re.IGNORECASE, maxsplit=1)
    if len(parts) != 2:
        return None
    start_raw, end_raw = parts[0], parts[1]

    def normalize_time_token(token: str) -> str:
        token = _collapse_ws(token)
        lower = token.lower()
        if lower == "noon":
            return "12:00 PM"
        if lower == "midnight":
            return "12:00 AM"

        # Normalize A.M./P.M. variants to AM/PM so dateutil doesn't treat the
        # trailing "M" as a timezone token.
        token = re.sub(
            r"\b([ap])\s*\.?\s*m\.?\b",
            lambda m: "AM" if m.group(1).lower() == "a" else "PM",
            token,
            flags=re.IGNORECASE,
        )
        return token

    def has_meridiem(token: str) -> bool:
        return re.search(r"\b(AM|PM)\b", token, flags=re.IGNORECASE) is not None

    def extract_meridiem(token: str) -> Optional[str]:
        match = re.search(r"\b(AM|PM)\b", token, flags=re.IGNORECASE)
        return match.group(1).upper() if match else None

    start_token = normalize_time_token(start_raw)
    end_token = normalize_time_token(end_raw)

    end_hint = extract_meridiem(end_token)
    if not has_meridiem(start_token) and end_hint is not None:
        start_token = f"{start_token} {end_hint}"

    default_dt = datetime(2000, 1, 1, 0, 0, 0)
    try:
        start_dt = dateutil_parser.parse(start_token, default=default_dt, fuzzy=True)
        end_dt = dateutil_parser.parse(end_token, default=default_dt, fuzzy=True)
    except (ValueError, OverflowError):
        return None

    start_time = start_dt.time().replace(second=0, microsecond=0)
    end_time = end_dt.time().replace(second=0, microsecond=0)
    return (start_time, end_time)


def _format_time_12h(value: time) -> str:
    hour24 = value.hour
    minute = value.minute
    meridiem = "AM" if hour24 < 12 else "PM"
    hour12 = hour24 % 12
    if hour12 == 0:
        hour12 = 12
    if minute:
        return f"{hour12}:{minute:02d} {meridiem}"
    return f"{hour12} {meridiem}"


def _normalize_hours_for_display(hours: str) -> str:
    hours = _collapse_ws(hours)
    parsed = _parse_hours_range(hours)
    if parsed is None:
        return hours
    start_t, end_t = parsed
    return f"{_format_time_12h(start_t)} to {_format_time_12h(end_t)}"


def _infer_years(rows: list[ParsedRow], *, start_year: int) -> List[Tuple[int, ParsedRow]]:
    year = start_year
    prev_month: Optional[int] = None
    out: List[Tuple[int, ParsedRow]] = []
    for row in rows:
        if prev_month is not None and row.month < prev_month:
            year += 1
        prev_month = row.month
        out.append((year, row))
    return out


def _stable_uid(*parts: str) -> str:
    raw = "|".join(_collapse_ws(p) for p in parts)
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()
    return f"{digest}@missionbayparking"


def _venue_location(venue: str) -> str:
    v = venue.lower()
    if v == "oracle":
        return "Oracle Park, San Francisco, CA"
    if v == "oracle park":
        return "Oracle Park, San Francisco, CA"
    if v == "chase center":
        return "Chase Center, San Francisco, CA"
    return venue


def _venue_display_name(venue: str) -> str:
    v = _collapse_ws(venue).lower()
    if v == "oracle":
        return "Oracle Park"
    if v == "oracle park":
        return "Oracle Park"
    if v == "chase center":
        return "Chase Center"
    return _collapse_ws(venue)


def _multi_venue_location(venues: List[str]) -> str:
    normalized = sorted({_venue_display_name(v) for v in venues})
    if normalized == ["Chase Center", "Oracle Park"]:
        return "Oracle Park & Chase Center, San Francisco, CA"
    if len(normalized) == 1:
        return _venue_location(normalized[0])
    return "San Francisco, CA"


def _event_window_for_row(event_date: date, hours: str) -> Optional[Tuple[datetime, datetime]]:
    hours_range = _parse_hours_range(hours)
    if hours_range is None:
        return None
    start_local_time, end_local_time = hours_range
    start_local = datetime.combine(event_date, start_local_time, tzinfo=LOCAL_TZ)
    end_local = datetime.combine(event_date, end_local_time, tzinfo=LOCAL_TZ)
    if end_local <= start_local:
        end_local = end_local + timedelta(days=1)
    return (start_local, end_local)


def _make_merged_event(*, event_date: date, rows: List[ParsedRow], url: str) -> CalendarEvent:
    entries = sorted({(_venue_display_name(r.venue), _normalize_hours_for_display(r.hours)) for r in rows})
    details: Tuple[Tuple[str, str], ...] = tuple(entries)
    venues = sorted({venue for venue, _ in details})
    location = _multi_venue_location(venues)

    if len(venues) == 1:
        summary = f"SFMTA event parking rates — {venues[0]}"
    else:
        summary = f"SFMTA event parking rates — {' / '.join(venues)}"

    description_lines = ["SFMTA Special Event Parking Regulations are in effect.", ""]
    description_lines.extend([f"{venue}: {hours}" for venue, hours in details])
    description_lines.extend(["", f"Source: {url}", ""])
    description = "\n".join(description_lines)

    uid_key = ";".join([f"{venue}:{hours}" for venue, hours in details])
    uid = _stable_uid(str(event_date), uid_key, url)

    windows: List[Tuple[datetime, datetime]] = []
    for _, hours in details:
        window = _event_window_for_row(event_date, hours)
        if window is None:
            windows = []
            break
        windows.append(window)

    if not windows:
        return CalendarEvent(
            uid=uid,
            summary=summary,
            description=description,
            details=details,
            location=location,
            start_local=None,
            end_local=None,
            all_day_date=event_date,
        )

    start_local = min(start for start, _ in windows)
    end_local = max(end for _, end in windows)
    return CalendarEvent(
        uid=uid,
        summary=summary,
        description=description,
        details=details,
        location=location,
        start_local=start_local,
        end_local=end_local,
        all_day_date=None,
    )


def _ical_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace(";", "\\;").replace(",", "\\,").replace("\r\n", "\n").replace(
        "\n", "\\n"
    )


def _fold_ical_line(line: str) -> str:
    if len(line) <= 75:
        return line
    out: list[str] = []
    while len(line) > 75:
        out.append(line[:75])
        line = " " + line[75:]
    out.append(line)
    return "\r\n".join(out)


def _format_dt_local(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=LOCAL_TZ)
    else:
        dt = dt.astimezone(LOCAL_TZ)
    return dt.strftime("%Y%m%dT%H%M%S")


def _event_date_for_sort(event: CalendarEvent) -> date:
    return event.all_day_date or event.start_local.date()  # type: ignore[union-attr]


def _deterministic_dtstamp(event: CalendarEvent) -> str:
    # RFC 5545: DTSTAMP must be in UTC (with trailing "Z"). We compute a stable
    # value that only changes when the event content changes, to avoid churn in
    # the committed .ics while still letting calendar clients detect updates.
    stamp_basis = f"{event.uid}\n{event.summary}\n{event.description}\n{event.location}\n{event.details}"
    digest = hashlib.sha1(stamp_basis.encode("utf-8")).digest()
    seconds = int.from_bytes(digest[:4], "big") % 86400
    dt = datetime.combine(_event_date_for_sort(event), time(0, 0), tzinfo=timezone.utc) + timedelta(seconds=seconds)
    return dt.strftime("%Y%m%dT%H%M%SZ")


def _format_date(d: date) -> str:
    return d.strftime("%Y%m%d")


def build_ical(*, calname: str, url: str, events: List[CalendarEvent]) -> str:
    lines: list[str] = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//missionbayparking//sfmta-special-event-parking//EN",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
        f"X-WR-CALNAME:{_ical_escape(calname)}",
        f"X-WR-CALDESC:{_ical_escape('SFMTA special event parking meter rates and tow-away zones dates.')}",
        f"X-WR-TIMEZONE:{LOCAL_TZID}",
    ]

    for event in sorted(
        events,
        key=lambda e: (
            _event_date_for_sort(e),
            e.start_local or datetime.min.replace(tzinfo=LOCAL_TZ),
            e.summary,
            e.uid,
        ),
    ):
        lines.append("BEGIN:VEVENT")
        lines.append(f"UID:{_ical_escape(event.uid)}")

        lines.append(f"DTSTAMP:{_deterministic_dtstamp(event)}")

        if event.all_day_date is not None:
            lines.append(f"DTSTART;VALUE=DATE:{_format_date(event.all_day_date)}")
            lines.append(f"DTEND;VALUE=DATE:{_format_date(event.all_day_date + timedelta(days=1))}")
        else:
            assert event.start_local is not None and event.end_local is not None
            lines.append(f"DTSTART;TZID={LOCAL_TZID}:{_format_dt_local(event.start_local)}")
            lines.append(f"DTEND;TZID={LOCAL_TZID}:{_format_dt_local(event.end_local)}")

        lines.append(f"SUMMARY:{_ical_escape(event.summary)}")
        lines.append(f"DESCRIPTION:{_ical_escape(event.description)}")
        lines.append(f"LOCATION:{_ical_escape(event.location)}")
        lines.append("TRANSP:OPAQUE")
        lines.append("END:VEVENT")

    lines.append("END:VCALENDAR")
    return "\r\n".join(_fold_ical_line(line) for line in lines) + "\r\n"


def fetch_html(url: str, *, timeout_s: int) -> str:
    response = requests.get(
        url,
        timeout=timeout_s,
        headers={
            "User-Agent": "missionbayparking/1.0 (+https://github.com/)",
            "Accept": "text/html,application/xhtml+xml",
        },
    )
    response.raise_for_status()
    response.encoding = response.apparent_encoding
    return response.text


def parse_events(*, html: str, url: str, fallback_year: int) -> List[CalendarEvent]:
    soup = BeautifulSoup(html, "html.parser")
    page_text = soup.get_text("\n", strip=True)
    effective_year = _extract_effective_year(page_text, fallback_year)

    rows = _extract_rows_from_tables(soup)
    if not rows:
        rows = _extract_rows_from_text(page_text)

    by_date: dict[date, List[ParsedRow]] = {}
    for year, row in _infer_years(rows, start_year=effective_year):
        try:
            event_date = date(year, row.month, row.day)
        except ValueError:
            # Skip impossible dates (defensive against bad parses).
            continue
        by_date.setdefault(event_date, []).append(row)

    events: List[CalendarEvent] = []
    for event_date in sorted(by_date.keys()):
        events.append(_make_merged_event(event_date=event_date, rows=by_date[event_date], url=url))
    return events


def main(argv: List[str]) -> int:
    parser = argparse.ArgumentParser(description="Generate an iCalendar file from SFMTA's special event schedule.")
    parser.add_argument("--url", default=DEFAULT_URL, help="SFMTA schedule URL to fetch")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="Output .ics path")
    parser.add_argument("--calname", default=DEFAULT_CALNAME, help="Calendar name embedded in the .ics")
    parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout (seconds)")
    args = parser.parse_args(argv)

    html = fetch_html(args.url, timeout_s=args.timeout)
    events = parse_events(html=html, url=args.url, fallback_year=date.today().year)

    for event in events:
        event_date = event.all_day_date or event.start_local.date()  # type: ignore[union-attr]
        details = "; ".join([f"{venue}: {hours}" for venue, hours in event.details])
        suffix = " (all day)" if event.all_day_date is not None else ""
        print(f"{event_date.isoformat()}{suffix}: {details}")

    ical = build_ical(calname=args.calname, url=args.url, events=events)

    output_path = args.output
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        f.write(ical)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
