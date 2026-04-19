# Production Data App - Latest Version

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

## Notes
- The SQLite database file `production.db` is created automatically on first run.
- This is intended as a clean restart point for continuing development.
