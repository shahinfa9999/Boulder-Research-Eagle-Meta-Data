#!/usr/bin/env python3
"""
FRNBES Nest-Maintenance Pipeline v2
Author: ChatGPT for Dana Bove

A clean, reliable pipeline for:
- repairing minutes
- assigning sessions using TIME ONLY (hybrid sessionizer)
- summarizing nest-building (D and 7D)
- generating session-level & biweekly summaries

Outputs:
    <prefix>_session_nest_maintenance.csv
    <prefix>_biweekly_nest_maintenance_expanded.csv
"""

import os
import numpy as np
import pandas as pd
from datetime import timedelta


############################################################
# 1. LOAD & CLEAN DATA
############################################################

def load_data(input_file: str) -> pd.DataFrame:
    df = pd.read_csv(input_file, low_memory=False)

    if 'date_time' not in df.columns:
        raise ValueError("Input must contain a 'date_time' column.")

    df['dt'] = pd.to_datetime(df['date_time'], errors='coerce')

    if 'minutes' not in df.columns:
        raise ValueError("Input must contain a 'minutes' column.")

    df['minutes'] = pd.to_numeric(df['minutes'], errors='coerce')

    # Remove rows missing dt or minutes
    df = df.dropna(subset=['dt', 'minutes']).copy()

    # Sort chronologically (per nest if present)
    if 'nest.name' in df.columns:
        df = df.sort_values(['nest.name', 'dt']).reset_index(drop=True)
    else:
        df = df.sort_values('dt').reset_index(drop=True)

    return df


############################################################
# 2. HYBRID SESSIONIZER — OPTION C
############################################################

def assign_sessions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Hybrid FRNBES sessionizer:

    - Sessions defined strictly by time gaps (> 8 minutes).
    - 'minutes' column is never used to form sessions.
    - minutes_repaired tracks corrected minute counters.
    - Very robust to human errors in minutes column.

    Output adds:
        session_id
        minutes_repaired
    """

    df = df.copy()

    if 'nest.name' in df.columns:
        groups = df.groupby('nest.name', sort=False)
    else:
        groups = [(None, df)]

    session_ids = {}
    repaired_minutes = []
    current_session = 0

    for nest_key, sub in groups:
        prev_dt = None
        expected_min = 0

        for idx, row in sub.iterrows():
            dt = row['dt']
            raw_min = row['minutes']

            # new session based on time gap
            if (prev_dt is None) or ((dt - prev_dt).total_seconds() > 480):
                current_session += 1
                expected_min = 0
                repaired_val = 0
            else:
                # same session → repair minutes if needed
                if np.isnan(raw_min):
                    raw_min = expected_min

                if abs(raw_min - expected_min) > 3:
                    raw_min = expected_min

                repaired_val = raw_min
                expected_min = raw_min + 3

            session_ids[idx] = current_session
            repaired_minutes.append(repaired_val)
            prev_dt = dt

    df['session_id'] = df.index.map(session_ids)
    df['minutes_repaired'] = repaired_minutes

    return df


############################################################
# 3. SESSION-LEVEL NEST-MAINTENANCE SUMMARY
############################################################

ROLES = ["female", "male", "undiff1", "undiff2"]


def extract_session_summaries(df: pd.DataFrame) -> pd.DataFrame:
    records = []

    by_fields = ['session_id']
    if 'nest.name' in df.columns:
        by_fields = ['nest.name', 'session_id']

    for keys, grp in df.groupby(by_fields):
        if isinstance(keys, tuple):
            nest_name, sid = keys
        else:
            nest_name, sid = (None, keys)

        start = grp['dt'].min()
        end = grp['dt'].max()
        session_minutes = (end - start).total_seconds() / 60.0

        start_minutes = grp['minutes_repaired'].min()
        n_rows = len(grp)
        singleton = session_minutes < 9.0

        # ----- Base session fields -----
        entry = {
            'session_id': sid,
            'session_start': start,
            'session_end': end,
            'session_minutes': session_minutes,
            'singleton_session': singleton,
            'start_minutes': start_minutes,
            'n_rows': n_rows,
            'date': start.date(),
        }

        if nest_name is not None:
            entry['nest.name'] = nest_name

        # ----- Repaired minutes diagnostics -----
        raw = grp['minutes'].fillna(-99999)
        rep = grp['minutes_repaired']

        entry['minutes_corrupt_flag'] = bool((raw != rep).any())
        entry['minutes_repaired_min'] = rep.min()
        entry['minutes_repaired_max'] = rep.max()
        entry['minutes_repaired_range'] = rep.max() - rep.min()

        # ----- Nest-maintenance counts -----
        all_D = 0
        all_7D = 0

        for role in ROLES:
            loc_col = f'{role}.loc'
            be_col = f'{role}.be'

            if (loc_col not in grp.columns) or (be_col not in grp.columns):
                entry[f'{role}_D'] = 0
                entry[f'{role}_7D'] = 0
                continue

            loc = pd.to_numeric(grp[loc_col], errors='coerce')
            be = grp[be_col].astype(str).str.strip().str.upper()

            is_D = be.eq('D')
            valid_loc = loc.isin([1,2,3,4,5,6,7])
            is_7 = loc.eq(7)

            count_D = int((is_D & valid_loc).sum())
            count_7D = int((is_D & is_7).sum())

            entry[f'{role}_D'] = count_D
            entry[f'{role}_7D'] = count_7D

            all_D += count_D
            all_7D += count_7D

        entry['all_roles_D'] = all_D
        entry['all_roles_7D'] = all_7D

        entry['suspect_start_minutes'] = bool(
            (start_minutes not in (0,3)) and not np.isnan(start_minutes)
        )

        records.append(entry)

    # Convert list of dicts → DataFrame
    sessions = pd.DataFrame.from_records(records)

    # ----- Order columns -----
    base_cols = [
        'session_id',
        'nest.name' if 'nest.name' in sessions.columns else None,
        'date',
        'session_start',
        'session_end',
        'session_minutes',
        'singleton_session',
        'start_minutes',
        'suspect_start_minutes',
        'minutes_corrupt_flag',
        'minutes_repaired_min',
        'minutes_repaired_max',
        'minutes_repaired_range',
        'n_rows',
        'all_roles_D',
        'all_roles_7D',
    ]
    base_cols = [c for c in base_cols if c is not None]

    role_cols = []
    for role in ROLES:
        role_cols.extend([f'{role}_D', f'{role}_7D'])

    ordered = base_cols + role_cols
    remaining = [c for c in sessions.columns if c not in ordered]

    sessions = sessions[ordered + remaining]

    return sessions


############################################################
# 4. BIWEEKLY SUMMARIES
############################################################

def make_biweekly_bins(sessions: pd.DataFrame):
    if sessions.empty:
        return []

    first_day = sessions['session_start'].dt.date.min()
    last_day  = sessions['session_start'].dt.date.max()

    bins = []
    cur = first_day
    while cur <= last_day:
        bins.append((cur, cur + timedelta(days=13)))
        cur = cur + timedelta(days=14)

    return bins


def assign_biweek(date_val, bins):
    if pd.isna(date_val):
        return np.nan

    d = date_val.date()
    for i, (start, end) in enumerate(bins, start=1):
        if start <= d <= end:
            return i
    return np.nan


def summarize_biweekly(sessions: pd.DataFrame) -> pd.DataFrame:
    bins = make_biweekly_bins(sessions)

    if not bins:
        return pd.DataFrame()

    sessions = sessions.copy()
    sessions['biweek_id'] = sessions['session_start'].apply(lambda x: assign_biweek(x, bins))

    records = []

    for bi_id in sorted(sessions['biweek_id'].dropna().unique()):
        bi_id = int(bi_id)
        sub = sessions[sessions['biweek_id']==bi_id]

        start_date, end_date = bins[bi_id-1]

        total_minutes = float(sub['session_minutes'].sum())
        survey_hours = total_minutes/60 if total_minutes > 0 else 0

        rec = {
            'biweek_id': bi_id,
            'biweek_start': start_date,
            'biweek_end': end_date,
            'total_minutes': total_minutes,
            'survey_hours': survey_hours,
            'n_sessions': len(sub),
            'n_singletons': int(sub['singleton_session'].sum()),
            'all_roles_D': int(sub['all_roles_D'].sum()),
            'all_roles_7D': int(sub['all_roles_7D'].sum()),
        }

        # D and 7D by role
        for role in ROLES:
            rec[f'{role}_D'] = int(sub[f'{role}_D'].sum())
            rec[f'{role}_7D'] = int(sub[f'{role}_7D'].sum())

        # rates
        if survey_hours > 0:
            rec['all_roles_D_per_100hr'] = rec['all_roles_D']/survey_hours * 100
            rec['all_roles_7D_per_100hr'] = rec['all_roles_7D']/survey_hours * 100

            for role in ROLES:
                rec[f'{role}_D_per_100hr'] = rec[f'{role}_D']/survey_hours * 100
                rec[f'{role}_7D_per_100hr'] = rec[f'{role}_7D']/survey_hours * 100
        else:
            rec['all_roles_D_per_100hr'] = np.nan
            rec['all_roles_7D_per_100hr'] = np.nan
            for role in ROLES:
                rec[f'{role}_D_per_100hr'] = np.nan
                rec[f'{role}_7D_per_100hr'] = np.nan

        records.append(rec)

    biweekly = pd.DataFrame.from_records(records)

    # Order columns
    base_cols = [
        'biweek_id', 'biweek_start', 'biweek_end',
        'total_minutes', 'survey_hours',
        'n_sessions', 'n_singletons',
        'all_roles_D', 'all_roles_7D',
        'all_roles_D_per_100hr', 'all_roles_7D_per_100hr',
    ]

    role_cols = []
    for role in ROLES:
        role_cols.extend([
            f'{role}_D', f'{role}_7D',
            f'{role}_D_per_100hr', f'{role}_7D_per_100hr'
        ])

    ordered = base_cols + role_cols
    remaining = [c for c in biweekly.columns if c not in ordered]

    return biweekly[ordered + remaining]


############################################################
# 5. MAIN DRIVER
############################################################

def run_pipeline_nm(input_file: str):
    df = load_data(input_file)
    df = assign_sessions(df)
    sessions = extract_session_summaries(df)
    biweekly = summarize_biweekly(sessions)

    base = os.path.splitext(os.path.basename(input_file))[0]

    out1 = f"{base}_session_nest_maintenance.csv"
    out2 = f"{base}_biweekly_nest_maintenance_expanded.csv"

    sessions.to_csv(out1, index=False, float_format="%.6f")
    biweekly.to_csv(out2, index=False, float_format="%.6f")

    print("\nNest-maintenance pipeline completed.")
    print(f"  Sessions file : {out1}")
    print(f"  Biweekly file : {out2}\n")


    return sessions, biweekly