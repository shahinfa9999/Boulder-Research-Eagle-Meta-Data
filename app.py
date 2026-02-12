import streamlit as st
import pandas as pd
from pipeline import load_data_streamlit, run_pipeline, aggregate_by_two_weeks, load_perch_coords, PIPELINE_FUNCTIONS_LIST

from session_stats import run_stats_in_memory
from nest_maintenance_pipeline import run_pipeline_nm


st.set_page_config(layout="wide")
st.title("Bird Nest Observation Pipeline")

# ---------------- Sidebar ----------------
st.sidebar.header("Inputs")

obs_file = st.sidebar.file_uploader(
    "Nest Aggregate", type=["csv"],
    help="Required. Current nest-level aggregate export for the selected territory."
)

perch_file = st.sidebar.file_uploader(
    "Upload Perch Code Excel", type=["xlsx"],
    help="Required. Master perch reference table for all nests."
)

selected_nests = st.sidebar.selectbox(
    "Nest", ["Hygiene", "BOCR", "CR16.5", "Stearns", "Erie", "White Rocks", "Erie", "ERLA", "RD15"]
)

date_range = st.sidebar.date_input(
    "Date range (optional)", value=[],
    help="Select start and end date to filter observations by date. Default is no date filtering."
)
min_valid_sex_counts = st.sidebar.slider(
    "Minimum valid gender counts per period (0-1)", min_value=0.0, max_value=1.0, value=0.9,
    help=(
        "Filters out periods where the proportion of valid sex counts (female + male) to total counts (female + male + undiff1 + undiff2) is greater or equal to this threshold. This helps ensure that the aggregated metrics are based on a sufficient amount of valid sex data."
    )
)
period = st.sidebar.selectbox(
    "Aggregation Window",
    ["1D", "1W", "2W", "4W"],
    help=(
        "Defines time binning for summary outputs.\n\n"
        "• 4W = fixed 28-day window (not calendar month)\n"
        "• 1D bins are calendar days\n"
        "• Seasonal framing handled via Start / End Date"
    )
)



normalize = st.sidebar.selectbox(
    "Calculated (effort-normalized) output",
    ["yes", "no"],
    index=0,
    help=(
        "Most metrics return % of survey time.\n"
        "Stick metrics return rate/hour.\n"
        "7D returns a raw count."
    )
)

session_file = st.sidebar.file_uploader(
    "Upload Session Stats CSV (optional)", type=["csv"]
)

maintenance_file = st.sidebar.file_uploader(
    "Upload Nest-Maintenance Raw CSV (optional)",
    type=["csv"]
)

################ dummy imghdr for Streamlit compatibility ################
import streamlit as st

# Example metrics (replace with your real PIPELINE_FUNCTIONS_LIST)
PIPELINE_FUNCTIONS_LIST = PIPELINE_FUNCTIONS_LIST # assuming this already exists

st.sidebar.subheader("Metric(s) to aggregate")

# Master toggle
use_all_metrics = st.sidebar.checkbox("All metrics (None)", value=True)

selected_metrics = []

# Individual metric checkboxes
for metric_name in PIPELINE_FUNCTIONS_LIST:
    checked = st.sidebar.checkbox(
        metric_name,
        value=False if use_all_metrics else False,
        key=f"metric_{metric_name}"
    )
    if checked:
        selected_metrics.append(metric_name)

# Final output that replaces your multiselect variable
'''
if use_all_metrics or len(selected_metrics) == 0:
    metric = ["(None)"]   # mimic your old multiselect default
else:
    metric = selected_metrics

st.sidebar.caption(f"Selected: {metric}")
'''

#######################################################


metric = st.sidebar.multiselect(
    "Metric(s) to aggregate - leave blank/None for all",
    ["(None)"] + PIPELINE_FUNCTIONS_LIST,
    default=["(None)"]
)
'''
if isinstance(metric, list):
    # If user accidentally includes "(None)" along with other metrics, drop it.
    metric = [m for m in metric if m != "(None)"]
    if len(metric) == 0:
        metric = None
    elif len(metric) == 1:
        metric = metric[0]
elif isinstance(metric, list) and len(metric) >= 1:
    metric = metric[0]
'''

run_button = st.sidebar.button("Run Pipeline")

# ---------------- Run ----------------
if run_button:
    if not obs_file:
        st.error("Please upload observation CSV.")
        st.stop()

    df = load_data_streamlit(obs_file)
    # ---------------- Session-Level Statistics ----------------
    if session_file:
        st.subheader("Session-Level Statistics")

        try:
            session_df = load_data_streamlit(session_file)

            with st.spinner("Computing session-level statistics…"):
                stats_summary, stats_roles, stats_cis = run_stats_in_memory(session_df)

            st.markdown("### Summary")
            st.dataframe(stats_summary, use_container_width=True)

            st.markdown("### By Role")
            st.dataframe(stats_roles, use_container_width=True)

            st.markdown("### Poisson 95% Confidence Intervals")
            st.dataframe(stats_cis, use_container_width=True)

            # ---- Downloads ----
            st.download_button(
                "Download stats summary CSV",
                stats_summary.to_csv(index=False),
                "session_stats_summary.csv"
            )

            st.download_button(
                "Download stats by role CSV",
                stats_roles.to_csv(index=False),
                "session_stats_by_role.csv"
            )

            st.download_button(
                "Download stats CIs CSV",
                stats_cis.to_csv(index=False),
                "session_stats_CIs.csv"
            )

        except Exception as e:
            st.error("Session-level statistics could not be computed.")
            st.exception(e)

    if maintenance_file:
        sessions_df, biweekly_df = run_pipeline_nm(maintenance_file)

        st.subheader("Session Nest-Maintenance Output")
        st.dataframe(sessions_df)
        st.download_button(
            "Download session Nest-Maintenance CSV",
            sessions_df.to_csv(index=False),
            "session_nest_maintenance.csv"
        )

        st.subheader("Biweekly Nest-Maintenance Output")
        st.dataframe(biweekly_df)
        st.download_button(
            "Download biweekly Nest-Maintenance CSV",
            biweekly_df.to_csv(index=False),
            "biweekly_nest_maintenance.csv"
        )

        # Then stats:
        stats_summary, stats_roles, stats_cis = run_stats_in_memory(sessions_df)

    if perch_file:
        perch_coords = load_perch_coords(
            perch_file,
            selected_nests if selected_nests else None
        )
        from pipeline import PERCH_COORDS
        PERCH_COORDS.clear()
        PERCH_COORDS.update(perch_coords)

    if date_range:
        if len(date_range) == 2:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df = df[
                (df["date"] >= pd.to_datetime(date_range[0])) &
                (df["date"] <= pd.to_datetime(date_range[1]))
            ]
    if selected_nests:
        df = df[df["nest.name"] == selected_nests]

    with st.spinner("Running pipeline…"):
        df_processed = run_pipeline(df)

    st.success("Pipeline complete!")

    # ---------------- Results ----------------
    st.subheader("Processed Data Preview")
    st.dataframe(df_processed.head(50))
    

    

    # Aggregation
    agg_df = aggregate_by_two_weeks(
        df_processed,
        period=period,
        metric=metric if metric else None,
        min_valid_sex_counts=0.9, 
        percent=(normalize == "yes")
    )
    if metric:
        metric_cols = metric if isinstance(metric, list) else [metric]
        cols = ['Period_bin', 'num_observations'] + metric_cols
        if normalize == "yes":
            cols += [f"{m}_percent" for m in metric_cols]
        cols = [c for c in cols if c in agg_df.columns]
        agg_df = agg_df[cols]

    st.subheader("Aggregated Output")
    st.dataframe(agg_df)

    # ---------------- Downloads ----------------
    st.download_button(
        "Download processed CSV",
        df_processed.to_csv(index=False),
        "processed_data.csv"
    )

    import io

    excel_buffer = io.BytesIO()
    agg_df.to_excel(excel_buffer, index=False, engine="openpyxl")
    excel_buffer.seek(0)

    st.download_button(
        label="Download aggregation Excel",
        data=excel_buffer,
        file_name="Period_summary.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )