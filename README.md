# Dataset Analysis Toolkit

A Python-based toolkit for analyzing and profiling large-scale intent and company datasets. Provides automated data quality assessment, relationship validation, and comprehensive reporting with SQLite database support.

## Overview

This toolkit processes CSV datasets containing company information, intent signals, contact data, and keyword mappings. It generates comprehensive reports on data relationships, quality metrics, and coverage analysis with local database capabilities for advanced querying.

## Features

- **Relationship Analysis**: Validates foreign key relationships and referential integrity
- **Data Quality Assessment**: Identifies null values, type mismatches, and coverage gaps  
- **Automated Data Profiling**: Analyzes file structure, data types, and basic statistics
- **Sample Generation**: Creates manageable subsets of large datasets for testing
- **SQLite Database**: Local database setup for SQL querying and analysis
- **Markdown Reporting**: Generates formatted summary reports

## Scripts

### `relationship_analyzer.py`
Primary analysis engine that validates relationships across datasets and generates detailed quality metrics.

**Key Capabilities:**
- Foreign key relationship validation with integrity percentages
- Data coverage analysis across all columns
- Null value identification and quality scoring
- Automated output generation to `private/analysis/`

**Output Files:**
- `relationship_analysis.csv` - Foreign key relationship metrics
- `data_coverage_analysis.csv` - Column-level data quality statistics

### `sqlite_setup.py`
Creates and populates a local SQLite database from CSV datasets for SQL-based analysis.

**Key Capabilities:**
- Imports all CSV files into structured database tables
- Creates indexes on key fields for performance
- Enables complex SQL queries and joins
- Stores database in `private/data/database/intent-data.db`

### `analyze_datasets.py`
Legacy analysis script for basic dataset profiling and statistics generation.

### `sampler.py`
Creates sample datasets for development and testing purposes.

**Behavior:**
- Files ≤500 rows: Full copy with `copy_` prefix
- Files >500 rows: First 500 rows with `sample_` prefix
- Maintains original structure and data types

## Dataset Types

The toolkit handles several dataset categories:

- **Company Data**: Basic company information and full company profiles
- **Intent Data**: Geographic and temporal intent signals with keyword mappings
- **Contact Data**: Individual contact records linked to companies
- **Keyword Data**: Keyword sets and individual keyword mappings

## Usage

### Initial Setup
1. Place CSV files in `private/data/raw/` directory
2. Install dependencies: `pandas`, `numpy`, `sqlite3`

### Database Setup
```bash
cd scripts
python sqlite_setup.py
```
This creates `private/data/database/intent-data.db` with all CSV data imported.

### Analysis Workflows
```bash
cd scripts

# Run relationship analysis
python relationship_analyzer.py

# Generate samples
python sampler.py

# Basic profiling
python analyze_datasets.py
```

### Querying the Database
```python
import sqlite3
import pandas as pd

conn = sqlite3.connect('../private/data/database/intent-data.db')

# High-intent contacts
result = pd.read_sql("""
    SELECT c.company_name, co.first_name, co.title, cs.intent_score
    FROM companies c
    JOIN contacts co ON c.company_id = co.company_id  
    JOIN contact_intent_scores cs ON co.employment_id = cs.employment_id
    WHERE cs.intent_score > 80
    ORDER BY cs.intent_score DESC
""", conn)

conn.close()
```

### Review Reports
- Analysis outputs: `private/analysis/`
- Generated reports: `data_summary.md`

## File Structure

```
├── README.md
├── data_summary.md
├── scripts/
│   ├── analyze_datasets.py
│   ├── relationship_analyzer.py
│   ├── sampler.py
│   └── sqlite_setup.py
└── private/
    └── data/
        ├── raw/           # CSV datasets
        ├── samples/       # Sample data
        └── database/      # SQLite database
```

## Database Tables

The SQLite database contains the following tables:
- `keyword_sets` - Keyword set definitions
- `keyword_set_keywords` - Individual keywords per set
- `companies` - Company data (eligible for intent tracking)
- `companies_full` - Full company hierarchy data
- `contacts` - Contact information and profiles
- `company_intent_geo` - Geographic intent signals by company
- `contact_intent_scores` - Person-level intent scores (1-100)

## Requirements

- Python 3.x
- pandas
- numpy
- sqlite3 (included with Python)
- pathlib