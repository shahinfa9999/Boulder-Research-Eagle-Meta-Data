#!/usr/bin/env python3
"""
FRNBES — Session-Level Statistics Engine (Option A)

Author: ChatGPT for Dana Bove

This script:
• Loads ANY session-level nest maintenance file
• Validates the structure
• Computes:
    - total_minutes
    - survey_hours
    - D / 100 hr
    - 7D / 100 hr
    - role-specific totals + rates
    - Poisson 95% confidence intervals
• Exports clean stats files for FRNBES analysis + manuscripts

Outputs:
    <base>_stats_summary.csv
    <base>_stats_by_role.csv
    <base>_stats_CIs.csv

Usage:
    python session_stats.py path/to/my_session_file.csv
"""

import os
import sys
import numpy as np
import pandas as pd
from math import sqrt


###############################################
# 1. Helper: Poisson 95% Confidence Intervals
###############################################

def poisson_ci(count, exposure_hours):
    """
    Poisson 95% CI for rate per 100 hours.

    Inputs:
        count = event count (D or 7D)
        exposure_hours = survey_hours

    Returns:
        (lower_CI, upper_CI) per 100 hours
    """
    if exposure_hours <= 0:
        return (np.nan, np.nan)

    # rate per hour
    lam = count / exposure_hours

    # standard error (normal approximation)
    if count > 0:
        se = sqrt(count) / exposure_hours
    else:
        se = 1.96 / exposure_hours

    lower = max(0, lam - 1.96 * se)
    upper = lam + 1.96 * se

    # convert to rate per 100 hours
    return (lower * 100, upper * 100)


###############################################
# 2. Load and Validate
###############################################

def load_session_file(fpath):
    df = pd.read_csv(fpath, low_memory=False)

    required = [
        "session_minutes",
        "all_roles_D",
        "all_roles_7D",
    ]

    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"Missing needed columns in session file: {missing}"
        )

    # Gracefully handle absence of one or more roles
    roles = ["female", "male", "undiff1", "undiff2"]
    role_cols = []

    for r in roles:
        D = f"{r}_D"
        D7 = f"{r}_7D"
        if D in df.columns and D7 in df.columns:
            role_cols.append((r, D, D7))

    return df, role_cols


###############################################
# 3. Compute Statistics
###############################################

def compute_stats(df, role_cols):
    out_rows_summary = []
    out_rows_roles = []
    out_rows_ci = []

    # ---- Summary totals ----
    total_minutes = df["session_minutes"].sum()
    survey_hours = total_minutes / 60.0

    allD = df["all_roles_D"].sum()
    all7D = df["all_roles_7D"].sum()

    rate_D = allD / survey_hours * 100 if survey_hours > 0 else np.nan
    rate_7D = all7D / survey_hours * 100 if survey_hours > 0 else np.nan

    ciD_lo, ciD_hi = poisson_ci(allD, survey_hours)
    ci7_lo, ci7_hi = poisson_ci(all7D, survey_hours)

    out_rows_summary.append({
        "total_minutes": total_minutes,
        "survey_hours": survey_hours,
        "all_roles_D_total": allD,
        "all_roles_7D_total": all7D,
        "all_roles_D_per_100hr": rate_D,
        "all_roles_7D_per_100hr": rate_7D,
        "D_CI_low": ciD_lo,
        "D_CI_high": ciD_hi,
        "7D_CI_low": ci7_lo,
        "7D_CI_high": ci7_hi,
    })

    # ---- Role-specific totals ----
    for (role, Dcol, D7col) in role_cols:
        rD = df[Dcol].sum()
        r7D = df[D7col].sum()

        rate_rD = rD / survey_hours * 100 if survey_hours > 0 else np.nan
        rate_r7D = r7D / survey_hours * 100 if survey_hours > 0 else np.nan

        ci_rD_lo, ci_rD_hi = poisson_ci(rD, survey_hours)
        ci_r7_lo, ci_r7_hi = poisson_ci(r7D, survey_hours)

        out_rows_roles.append({
            "role": role,
            "role_D_total": rD,
            "role_7D_total": r7D,
            "role_D_per_100hr": rate_rD,
            "role_7D_per_100hr": rate_r7D,
        })

        out_rows_ci.append({
            "role": role,
            "D_CI_low": ci_rD_lo,
            "D_CI_high": ci_rD_hi,
            "7D_CI_low": ci_r7_lo,
            "7D_CI_high": ci_r7_hi,
        })

    return (
        pd.DataFrame(out_rows_summary),
        pd.DataFrame(out_rows_roles),
        pd.DataFrame(out_rows_ci),
    )


###############################################
# 4. Driver
###############################################

def run_stats(input_file):
    df, role_cols = load_session_file(input_file)
    summary, roles, cis = compute_stats(df, role_cols)

    base = os.path.splitext(os.path.basename(input_file))[0]

    out_summary = f"{base}_stats_summary.csv"
    out_roles = f"{base}_stats_by_role.csv"
    out_cis = f"{base}_stats_CIs.csv"

    summary.to_csv(out_summary, index=False)
    roles.to_csv(out_roles, index=False)
    cis.to_csv(out_cis, index=False)

    print("\nStatistics completed.")
    print(f"Summary file : {out_summary}")
    print(f"Roles file   : {out_roles}")
    print(f"CI file      : {out_cis}\n")


def run_stats_in_memory(df):
    """
    Streamlit-safe wrapper: takes a DataFrame, returns outputs as DataFrames.
    """
    role_cols = []
    roles = ["female", "male", "undiff1", "undiff2"]

    for r in roles:
        D = f"{r}_D"
        D7 = f"{r}_7D"
        if D in df.columns and D7 in df.columns:
            role_cols.append((r, D, D7))

    return compute_stats(df, role_cols)
