"""
FRNBES QC Output Standard – Selected + QC
Author: Faisal
Purpose:
    Implements standardized QC fields accompanying selected metrics.
    QC explains reliability and coverage without exposing intermediate logic.
"""

from dataclasses import dataclass
from typing import Dict, Optional
import pandas as pd
import numpy as np

# =========================
# QC CONFIG
# =========================

@dataclass
class QCConfig:
    exclude_singletons: bool = True
    sex_filter_threshold_pct: float = 90.0
    sex_filter_scope: str = "date"  # "session" or "date"
    config_release_id: str = "FRNBES_v1.0"


# =========================
# CORE QC ENGINE
# =========================

def run_qc(
    df: pd.DataFrame,
    qc: QCConfig,
) -> (pd.DataFrame, Dict):
    """
    Runs QC filtering and computes QC metadata.
    Does NOT compute metrics.
    """

    qc_meta = {}

    qc_meta["total_rows"] = len(df)

    # ---- Singleton rule (< 3 rows = < 9 minutes) ----
    date_counts = df.groupby("date").size()
    singleton_dates = date_counts[date_counts < 3].index

    df = df.copy()
    df["singleton_session"] = df["date"].isin(singleton_dates)

    qc_meta["rows_excluded_singleton"] = int(df["singleton_session"].sum())

    if qc.exclude_singletons:
        df = df[~df["singleton_session"]]

    # ---- Rows + effort ----
    qc_meta["rows_used"] = len(df)
    qc_meta["rows_excluded_total"] = qc_meta["total_rows"] - qc_meta["rows_used"]
    qc_meta["effort_minutes"] = qc_meta["rows_used"] * 3

    qc_meta["config_release_id"] = qc.config_release_id

    return df, qc_meta


# =========================
# SEX DIFFERENTIATION RULE
# =========================

def compute_sex_qc(
    df: pd.DataFrame,
    qc: QCConfig,
) -> Dict:
    """
    Computes sex differentiation QC fields.
    Applies only if sex-specific metrics are requested.
    """
    df = df.copy()
    df['sex'] = np.where()
    
    if "sex" not in df.columns or "sex_confidence" not in df.columns:
        return {
            "sex_diff_pct": None,
            "sex_filter_threshold_pct": qc.sex_filter_threshold_pct,
            "sex_filter_scope": qc.sex_filter_scope,
            "sex_filter_result": "not_applicable",
        }
    
    confident = df["sex_confidence"] >= 1
    assigned = df["sex"].isin(["M", "F"])

    if len(df) == 0:
        sex_diff_pct = 0.0
    else:
        sex_diff_pct = 100 * (confident & assigned).sum() / len(df)

    sex_filter_result = (
        "kept"
        if sex_diff_pct >= qc.sex_filter_threshold_pct
        else "discarded"
    )

    return {
        "sex_diff_pct": round(sex_diff_pct, 1),
        "sex_filter_threshold_pct": qc.sex_filter_threshold_pct,
        "sex_filter_scope": qc.sex_filter_scope,
        "sex_filter_result": sex_filter_result,
    }


# =========================
# QC ROW BUILDER
# =========================

def build_qc_row(
    scope_keys: Dict,
    qc_meta: Dict,
    sex_qc: Dict,
) -> Dict:
    """
    Builds a single QC summary row for one output unit.
    """

    return {
        "nest_id": scope_keys.get("nest_id"),
        "date_start": scope_keys.get("date_start"),
        "date_end": scope_keys.get("date_end"),
        "session_id": scope_keys.get("session_id"),
        **qc_meta,
        **sex_qc,
    }


# =========================
# OUTPUT MODES
# =========================

def format_output(
    metrics_df: pd.DataFrame,
    qc_df: Optional[pd.DataFrame],
    mode: str,
    join_keys: list,
) -> pd.DataFrame:
    """
    Output modes:
        - selected
        - selected_qc
        - all
    """

    if mode == "selected":
        return metrics_df

    if mode == "selected_qc":
        return metrics_df.merge(qc_df, on=join_keys, how="left")

    if mode == "all":
        return metrics_df

    raise ValueError(f"Unknown output mode: {mode}")


# =========================
# EXAMPLE PIPELINE USAGE
# =========================

def process_unit(
    df: pd.DataFrame,
    scope_keys: Dict,
    qc: QCConfig,
    sex_metrics_requested: bool,
):
    """
    Example unit processor (session or date).
    """

    # ---- QC ----
    df_qc, qc_meta = run_qc(df, qc)

    # ---- Metrics (placeholder) ----
    metrics = {
        "example_metric": df_qc.shape[0]
    }
    metrics_df = pd.DataFrame([metrics])

    # ---- Sex QC ----
    sex_qc = (
        compute_sex_qc(df_qc, qc)
        if sex_metrics_requested
        else {
            "sex_diff_pct": None,
            "sex_filter_threshold_pct": qc.sex_filter_threshold_pct,
            "sex_filter_scope": qc.sex_filter_scope,
            "sex_filter_result": "not_applicable",
        }
    )

    # ---- Enforce sex rule ----
    if sex_qc["sex_filter_result"] == "discarded":
        metrics_df["example_sex_metric"] = np.nan

    # ---- QC row ----
    qc_row = build_qc_row(scope_keys, qc_meta, sex_qc)
    qc_df = pd.DataFrame([qc_row])

    return metrics_df, qc_df
