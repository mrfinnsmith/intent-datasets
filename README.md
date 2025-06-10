# Dataset Analysis Toolkit

A Python-based toolkit for analyzing and profiling large-scale intent and company datasets. Provides automated data quality assessment, schema validation, and summary reporting.

## Overview

This toolkit processes CSV datasets containing company information, intent signals, contact data, and keyword mappings. It generates comprehensive reports on data quality, coverage, and temporal distribution.

## Features

- **Automated Data Profiling**: Analyzes file structure, data types, and basic statistics
- **Schema Validation**: Compares actual data types against expected schemas
- **Data Quality Assessment**: Identifies null values, type mismatches, and coverage gaps
- **Sample Generation**: Creates manageable subsets of large datasets for testing
- **Markdown Reporting**: Generates formatted summary reports

## Scripts

### `analyze_datasets.py`
Main analysis engine that processes all CSV files in a directory and generates detailed statistics.

**Key Metrics:**
- Row and column counts
- File sizes and memory usage
- Null value analysis
- Data type validation
- Unique value counts for key identifiers
- Temporal coverage analysis

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
2. Run analysis: `python analyze_datasets.py`
3. Generate samples: `python sampler.py`
4. Review generated `data_summary.md` report

## Output

The analysis generates a comprehensive markdown report including:

- Overview table with key metrics
- Data coverage statistics
- Data quality issues identification
- Temporal coverage analysis
- Summary statistics across all datasets

## Requirements

- Python 3.x
- pandas
- numpy
- pathlib

## File Structure

```
├── README.md
├── analyze_datasets.py
├── sampler.py
├── data_summary.md
└── private/
    ├── datasets/          # Source CSV files
    └── samples/          # Generated sample files
```