# Production Data App - Latest Rebuild

This is a clean rebuilt Streamlit version of the production data capture app discussed previously.

## Features
- Admin and operator login
- SQLite local database
- Production entry by date, hour, machine, material, and feeding equipment
- Per-machine recirculation enable/disable
- Per-material recirculation factor
- Per-machine deduction percentage with deduction history
- Downtime capture with duration calculation
- Dashboard with export to Excel
- Settings pages for machines, materials, feeding equipment, and deduction history
- Duplicate prevention for the same machine/date/shift/hour combination

## Default logins
- admin / admin123
- operator / operator123

## Run locally
```bash
pip install -r requirements.txt
streamlit run app.py
```

## GitHub workflow
1. Create a new empty GitHub repository.
2. Upload `app.py`, `requirements.txt`, and `README.md`.
3. Clone locally if needed:
```bash
git clone <your-repo-url>
cd <repo-name>
pip install -r requirements.txt
streamlit run app.py
```

## Notes
- The SQLite database file `production.db` is created automatically on first run.
- This is intended as a clean restart point for continuing development.
