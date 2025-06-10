# Dataset Analysis Toolkit

A Python-based toolkit for analyzing and profiling large-scale intent and company datasets. Provides automated data quality assessment, relationship validation, and comprehensive reporting.

## Overview

This toolkit processes CSV datasets containing company information, intent signals, contact data, and keyword mappings. It generates comprehensive reports on data relationships, quality metrics, and coverage analysis.

## Features

- **Relationship Analysis**: Validates foreign key relationships and referential integrity
- **Data Quality Assessment**: Identifies null values, type mismatches, and coverage gaps  
- **Automated Data Profiling**: Analyzes file structure, data types, and basic statistics
- **Sample Generation**: Creates manageable subsets of large datasets for testing
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

1. Place CSV files in `private/datasets/` directory
2. Run relationship analysis: `python relationship_analyzer.py`
3. Generate samples: `python sampler.py`
4. Review generated reports in `private/analysis/`

## File Structure

```
.
├── README.md
├── analyze_datasets.py
├── data_summary.md
├── relationship_analyzer.py
├── sampler.py
└── private/
    ├── analysis/
    ├── datasets/
    ├── oracle/
    └── samples/
```

## Requirements

- Python 3.x
- pandas
- numpy
- pathlib

## Key Insights

The relationship analyzer identifies:
- **Strong relationships** (>80% referential integrity) for core business flow
- **Broken relationships** (≤80% integrity) requiring attention
- **Data quality issues** across all columns with null percentage analysis
- **Coverage gaps** and type validation across datasets

Perfect integrity found in core intent flow: keywords → companies → contacts → intent scores.