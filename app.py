import streamlit as st
import pandas as pd
from pipeline import load_data_streamlit, run_pipeline, aggregate_by_two_weeks, load_perch_coords, PIPELINE_FUNCTIONS_LIST, load_data

st.set_page_config(layout="wide")
st.title("Raptor Nest Observation Pipeline")

# ---------------- Sidebar ----------------
st.sidebar.header("Inputs")

obs_file = st.sidebar.file_uploader(
    "Upload Observation CSV", type=["csv"]
)

perch_file = st.sidebar.file_uploader(
    "Upload Perch Code Excel", type=["xlsx"]
)

selected_nests = st.sidebar.text_input(
    "Nest(s) (comma separated)", placeholder="Hygiene"
)

date_range = st.sidebar.date_input(
    "Date range (optional)", value=[]
)

period = st.sidebar.selectbox(
    "Aggregation period",
    ["2W", "1M", "1W"]
)



metric = st.sidebar.selectbox(
    "Metric to aggregate",
    ["(None)"] + PIPELINE_FUNCTIONS_LIST
)

if metric == "(None)":
    metric = None


run_button = st.sidebar.button("Run Pipeline")

# ---------------- Run ----------------
if run_button:
    if not obs_file:
        st.error("Please upload observation CSV.")
        st.stop()

    df = load_data_streamlit(obs_file)




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
        metric=metric if metric else None
    )

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

