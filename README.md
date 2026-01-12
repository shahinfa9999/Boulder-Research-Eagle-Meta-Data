
# Raptor Nest Observation Pipeline

A Streamlit-based data processing and analysis tool for raptor nest observation surveys.  
The application transforms raw field observation CSVs into standardized behavioral metrics, distance-based indicators, and time-aggregated summaries suitable for ecological analysis and reporting.

---

## Overview

This project processes long-term raptor nest monitoring data and derives:

- Adult and juvenile presence indicators
- Nest attendance and behavioral metrics
- Distance-from-nest classifications
- Juvenile–adult proximity metrics
- Two-week or custom-period aggregated summaries

The pipeline is designed to be:
- Deterministic and reproducible
- Modular and extensible
- Usable by non-programmers through a web UI

---

## Features

- Upload raw observation CSVs
- Upload perch coordinate Excel files
- Filter by nest and date range
- Automatically compute 200+ derived metrics
- Aggregate metrics by selectable time periods
- Download processed datasets and summaries
- Fully browser-based (no local install required)

---


## Input Data Requirements

### Observation CSV

Must include (case-insensitive):
- `date`
- `p1`–`p5` (perch codes)
- Location codes (`female.loc`, `male.loc`, etc.)
- Behavior codes (`female.be`, `male.be`, etc.)

The pipeline tolerates:
- Mixed encodings (UTF-8 / Latin-1)
- Missing values
- Historical schema drift

---

### Perch Code Excel

Must contain columns:
- `Nest`
- `Perch`
- `X` (longitude)
- `Y` (latitude)
- `Dist from Nest (m)`

---

## Running Locally

### 1. Install dependencies
```bash
pip install -r requirements.txt
````

### 2. Start the app

```bash
streamlit run streamlit/app.py
```

### 3. Open in browser

```
http://localhost:8501
```

---

## Streamlit App Workflow

1. Upload observation CSV
2. Upload perch code Excel (optional)
3. Select nest(s) and date range (optional)
4. Choose aggregation period and metric
5. Run pipeline
6. Download results

---

## Output Files

### Processed Dataset

* One row per observation
* Includes all derived indicators and distances

### Aggregated Summary

* Metrics summed by selected time period
* Observation counts per period
* Ready for statistical analysis or visualization

---

## Metrics

Metrics are derived via modular pipeline functions and include:

* Presence indicators (adult, juvenile, species-specific)
* Nest attendance
* Behavioral classifications
* Distance bins (0m, 0–200m, 200–400m, etc.)
* Juvenile proximity to nearest adult

The full metric list is automatically populated in the app UI.

---

## Versioning

The pipeline may evolve over time.
Results should be considered valid **per pipeline version** used at time of export.

Recommended:

* Archive exported files with date and version
* Note metric definitions when publishing results

---

## Deployment

The app is designed for deployment via Streamlit Community Cloud:

1. Push repository to GitHub
2. Connect GitHub repo to Streamlit
3. Automatic redeploy on every commit

---

## Design Philosophy

* Explicit over implicit
* No hidden state
* Reproducible transformations
* Transparent assumptions
* Scientist-friendly outputs

---

## License

Internal research use.
Contact the author for redistribution or extension permissions.

---

## Author

**Faisal Shahin**
Software Engineer / Data Scientist
Applied ecological data pipelines and scientific tooling

