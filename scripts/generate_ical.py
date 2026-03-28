#!/usr/bin/env python3
from __future__ import annotations

import argparse
import dataclasses
import hashlib
import re
import sys
from datetime import date, datetime, time, timedelta, timezone
from typing import Optional, Tuple, List, Dict

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateutil_parser
from zoneinfo import ZoneInfo
from icalendar import Calendar, Event


# Bumping this version will force a one-time update of all events in the .ics,
# causing Google Calendar to refresh them if you change formatting or logic.
LOGIC_VERSION = "2"

DEFAULT_URL = "https://www.sfmta.com/notices/current-special-event-parking-regulations-schedule"
DEFAULT_OUTPUT = "sfmta_special_event_parking.ics"
DEFAULT_CALNAME = "SFMTA Special Event Parking"
LOCAL_TZID = "America/Los_Angeles"
LOCAL_TZ = ZoneInfo(LOCAL_TZID)

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
}


@dataclasses.dataclass(frozen=True)
class ParsedRow:
    month: int
    day: int
    venue: str
    hours: str


@dataclasses.dataclass(frozen=True)
class EventMetadata:
    uid: str
    dtstamp: datetime
    last_modified: datetime
    sequence: int
    content_hash: str


def _collapse_ws(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _parse_existing_metadata(path: str) -> Dict[str, EventMetadata]:
    """Extract stable metadata from the existing .ics file to minimize churn."""
    try:
        with open(path, "rb") as f:
            cal = Calendar.from_ical(f.read())
    except (FileNotFoundError, ValueError):
        return {}

    metadata = {}
    for component in cal.walk():
        if component.name == "VEVENT":
            uid = str(component.get("UID"))
            dtstamp = component.get("DTSTAMP").dt
            last_mod = component.get("LAST-MODIFIED")
            last_mod = last_mod.dt if last_mod else dtstamp
            seq = int(component.get("SEQUENCE", 0))
            # Retrieve our custom hash property
            content_hash = str(component.get("X-CONTENT-HASH", ""))
            
            metadata[uid] = EventMetadata(
                uid=uid,
                dtstamp=dtstamp,
                last_modified=last_mod,
                sequence=seq,
                content_hash=content_hash
            )
    return metadata


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
            cells = [_collapse_ws(cell.get_text(" ", strip=True)) for cell in tr.find_all(["th", "td"]) if _collapse_ws(cell.get_text(" ", strip=True))]
            if len(cells) < 3 or cells[0].lower() == "event date":
                continue
            parsed = _parse_row_fields(cells[0], cells[1], cells[2])
            if parsed:
                rows.append(parsed)
        if rows:
            return rows
    return []


def _parse_row_fields(event_date: str, venue: str, hours: str) -> Optional[ParsedRow]:
    month_match = re.match(r"^([A-Za-z]+)\s*(\d{1,2})\b", _collapse_ws(event_date))
    if not month_match:
        return None
    month_name = month_match.group(1).lower()
    if month_name not in MONTHS:
        return None
    return ParsedRow(month=MONTHS[month_name], day=int(month_match.group(2)), venue=_collapse_ws(venue), hours=_collapse_ws(hours))


def _extract_rows_from_text(page_text: str) -> list[ParsedRow]:
    normalized = _collapse_ws(page_text)
    month_re = r"(January|February|March|April|May|June|July|August|September|October|November|December)"
    venue_re = r"(Oracle(?: Park)?|Chase Center)"
    time_token = r"(?:Noon|Midnight|\d{1,2}(?::\d{2})?\s*(?:A\.?M\.?|P\.?M\.?)?)"
    row_re = rf"\b{month_re}\s*(\d{{1,2}})\s+{venue_re}\s+({time_token}\s+to\s+{time_token})\b"
    rows: list[ParsedRow] = []
    for match in re.finditer(row_re, normalized, flags=re.IGNORECASE):
        rows.append(ParsedRow(month=MONTHS[match.group(1).lower()], day=int(match.group(2)), venue=_collapse_ws(match.group(3)), hours=_collapse_ws(match.group(4))))
    return rows


def _parse_hours_range(hours: str) -> Optional[Tuple[time, time]]:
    parts = re.split(r"\s+to\s+", _collapse_ws(hours), flags=re.IGNORECASE, maxsplit=1)
    if len(parts) != 2:
        return None
    def normalize_time_token(token: str) -> str:
        token = _collapse_ws(token).lower()
        if token == "noon": return "12:00 PM"
        if token == "midnight": return "12:00 AM"
        return re.sub(r"\b([ap])\s*\.?\s*m\.?\b", lambda m: "AM" if m.group(1) == "a" else "PM", token)

    start_token, end_token = normalize_time_token(parts[0]), normalize_time_token(parts[1])
    if "am" not in start_token and "pm" not in start_token and ("am" in end_token or "pm" in end_token):
        start_token += " " + ("AM" if "am" in end_token else "PM")
    
    try:
        start_dt = dateutil_parser.parse(start_token, default=datetime(2000,1,1), fuzzy=True)
        end_dt = dateutil_parser.parse(end_token, default=datetime(2000,1,1), fuzzy=True)
        return (start_dt.time(), end_dt.time())
    except:
        return None


def _format_time_12h(value: time) -> str:
    h12 = value.hour % 12 or 12
    return f"{h12}{f':{value.minute:02d}' if value.minute else ''} {'AM' if value.hour < 12 else 'PM'}"


def _normalize_hours_for_display(hours: str) -> str:
    parsed = _parse_hours_range(hours)
    if not parsed: return _collapse_ws(hours)
    return f"{_format_time_12h(parsed[0])} to {_format_time_12h(parsed[1])}"


def _stable_uid(event_date: date, venue_key: str, url: str) -> str:
    raw = f"{event_date}|{venue_key}|{url}"
    return f"{hashlib.sha1(raw.encode()).hexdigest()}@missionbayparking"


def _venue_display_name(venue: str) -> str:
    v = _collapse_ws(venue).lower()
    if "oracle" in v: return "Oracle Park"
    if "chase" in v: return "Chase Center"
    return _collapse_ws(venue)


def _multi_venue_location(venues: List[str]) -> str:
    normalized = sorted({_venue_display_name(v) for v in venues})
    if normalized == ["Chase Center", "Oracle Park"]: return "Oracle Park & Chase Center, San Francisco, CA"
    if len(normalized) == 1: 
        v = normalized[0]
        return f"{v}, San Francisco, CA"
    return "San Francisco, CA"


def _generate_event(event_date: date, rows: List[ParsedRow], url: str, existing_metadata: Dict[str, EventMetadata], now: datetime) -> Event:
    details = sorted({(_venue_display_name(r.venue), _normalize_hours_for_display(r.hours)) for r in rows})
    venues = sorted({v for v, _ in details})
    
    summary = f"SFMTA event parking rates — {' / '.join(venues)}"
    description = "SFMTA Special Event Parking Regulations are in effect.\n\n" + \
                  "\n".join([f"{v}: {h}" for v, h in details]) + \
                  f"\n\nSource: {url}\n"
    location = _multi_venue_location(venues)
    
    uid = _stable_uid(event_date, ";".join([f"{v}:{h}" for v, h in details]), url)
    
    # We include LOGIC_VERSION in the hash so script changes can trigger updates
    content_blob = f"{LOGIC_VERSION}|{summary}|{description}|{location}"

    # Time handling for the blob
    windows = []
    for _, h in details:
        hr = _parse_hours_range(h)
        if hr:
            start = datetime.combine(event_date, hr[0], tzinfo=LOCAL_TZ)
            end = datetime.combine(event_date, hr[1], tzinfo=LOCAL_TZ)
            if end <= start: end += timedelta(days=1)
            windows.append((start, end))

    if windows:
        start_dt = min(s for s, _ in windows)
        end_dt = max(e for _, e in windows)
        content_blob += f"|{start_dt.isoformat()}|{end_dt.isoformat()}"
    else:
        content_blob += f"|{event_date.isoformat()}"

    content_hash = hashlib.sha1(content_blob.encode()).hexdigest()

    event = Event()
    event.add("UID", uid)
    event.add("SUMMARY", summary)
    event.add("DESCRIPTION", description)
    event.add("LOCATION", location)
    event.add("TRANSP", "OPAQUE")
    event.add("X-CONTENT-HASH", content_hash)

    if windows:
        event.add("DTSTART", min(s for s, _ in windows))
        event.add("DTEND", max(e for _, e in windows))
    else:
        event.add("DTSTART", event_date)
        event.add("DTEND", event_date + timedelta(days=1))

    meta = existing_metadata.get(uid)    
    if meta:
        if meta.content_hash == content_hash:
            # Nothing changed, keep everything stable
            event.add("DTSTAMP", meta.dtstamp)
            event.add("SEQUENCE", meta.sequence)
            event.add("LAST-MODIFIED", meta.last_modified)
        else:
            event.add("DTSTAMP", now)
            event.add("SEQUENCE", meta.sequence + 1)
            event.add("LAST-MODIFIED", now)
    else:
        # New event
        event.add("DTSTAMP", now)
        event.add("SEQUENCE", 0) # Start at 0 for genuinely new events
        event.add("LAST-MODIFIED", now)

    return event


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--calname", default=DEFAULT_CALNAME)
    args = parser.parse_args()

    now = datetime.now(timezone.utc)
    existing_meta = _parse_existing_metadata(args.output)
    
    resp = requests.get(args.url, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    
    effective_year = _extract_effective_year(soup.get_text(), date.today().year)
    rows = _extract_rows_from_tables(soup) or _extract_rows_from_text(soup.get_text())
    
    by_date = {}
    year = effective_year
    prev_m = None
    for r in rows:
        if prev_m and r.month < prev_m: year += 1
        prev_m = r.month
        d = date(year, r.month, r.day)
        by_date.setdefault(d, []).append(r)

    cal = Calendar()
    cal.add("PRODID", f"-//missionbayparking//sfmta-special-event-parking//{LOGIC_VERSION}//EN")
    cal.add("VERSION", "2.0")
    cal.add("METHOD", "PUBLISH")
    cal.add("X-WR-CALNAME", args.calname)
    cal.add("X-WR-CALDESC", "SFMTA special event parking meter rates dates.")
    cal.add("X-WR-TIMEZONE", LOCAL_TZID)

    events = []
    for d in sorted(by_date.keys()):
        ev = _generate_event(d, by_date[d], args.url, existing_meta, now)
        events.append(ev)
        cal.add_component(ev)
    
    new_ical = cal.to_ical()    
    try:
        with open(args.output, "rb") as f:
            old_ical = f.read()
    except FileNotFoundError:
        old_ical = b""

    if new_ical != old_ical:
        with open(args.output, "wb") as f:
            f.write(new_ical)
        print(f"Updated {args.output} ({len(events)} events)")
    else:
        print("No changes detected.")


if __name__ == "__main__":
    main()
