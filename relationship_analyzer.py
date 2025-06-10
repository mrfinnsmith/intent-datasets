#!/usr/bin/env python3
"""
Dataset Relationship Analyzer for Demandbase Files
Analyzes foreign key relationships and data integrity across CSV files
"""

import pandas as pd
import os
from pathlib import Path
import json
from collections import defaultdict


def load_datasets(data_dir="private/datasets"):
    """Load all CSV files from the datasets directory"""
    datasets = {}
    data_path = Path(data_dir)

    if not data_path.exists():
        raise FileNotFoundError(f"Directory {data_dir} not found")

    csv_files = list(data_path.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {data_dir}")

    print(f"Loading {len(csv_files)} CSV files...")

    for file_path in csv_files:
        try:
            df = pd.read_csv(file_path, low_memory=False)
            datasets[file_path.name] = df
            print(f"  {file_path.name}: {len(df):,} rows, {len(df.columns)} columns")
        except Exception as e:
            print(f"  ERROR loading {file_path.name}: {e}")

    return datasets


def analyze_foreign_keys(datasets):
    """Analyze foreign key relationships between datasets"""

    # Define expected relationships based on field names
    relationships = [
        # (parent_table, parent_key, child_table, child_key)
        (
            "db1_keyword_sets_file_sample.csv",
            "id",
            "db1_keyword_set_keywords_file_sample.csv",
            "keyword_set_id",
        ),
        (
            "db1_keyword_sets_file_sample.csv",
            "id",
            "db_company_intent_geo_weekly_file_sample.csv",
            "keyword_set_id",
        ),
        (
            "db1_keyword_sets_file_sample.csv",
            "id",
            "db_company_intent_geo_weekly_keywordset_contact_scores_file_sample.csv",
            "keyword_set_id",
        ),
        (
            "db_company_file_sample.csv",
            "company_id",
            "db_contacts_file_sample.csv",
            "company_id",
        ),
        (
            "db_company_file_sample.csv",
            "company_id",
            "db_company_intent_geo_weekly_file_sample.csv",
            "company_id",
        ),
        (
            "db_company_file_sample.csv",
            "company_id",
            "db_company_intent_geo_weekly_keywordset_contact_scores_file_sample.csv",
            "company_id",
        ),
        (
            "db_company_full_file_sample.csv",
            "company_id",
            "db_contacts_file_sample.csv",
            "company_id",
        ),
        (
            "db_company_full_file_sample.csv",
            "company_id",
            "db_company_intent_geo_weekly_file_sample.csv",
            "company_id",
        ),
        (
            "db_company_full_file_sample.csv",
            "company_id",
            "db_company_intent_geo_weekly_keywordset_contact_scores_file_sample.csv",
            "company_id",
        ),
        (
            "db_contacts_file_sample.csv",
            "employment_id",
            "db_company_intent_geo_weekly_keywordset_contact_scores_file_sample.csv",
            "employment_id",
        ),
        # Self-referencing company hierarchy
        (
            "db_company_file_sample.csv",
            "company_id",
            "db_company_file_sample.csv",
            "parent_id",
        ),
        (
            "db_company_file_sample.csv",
            "company_id",
            "db_company_file_sample.csv",
            "ultimate_parent_id",
        ),
        (
            "db_company_full_file_sample.csv",
            "company_id",
            "db_company_full_file_sample.csv",
            "parent_id",
        ),
        (
            "db_company_full_file_sample.csv",
            "company_id",
            "db_company_full_file_sample.csv",
            "ultimate_parent_id",
        ),
    ]

    results = []

    for parent_table, parent_key, child_table, child_key in relationships:
        if parent_table not in datasets or child_table not in datasets:
            continue

        parent_df = datasets[parent_table]
        child_df = datasets[child_table]

        # Skip if columns don't exist
        if parent_key not in parent_df.columns or child_key not in child_df.columns:
            continue

        # Get unique values, excluding nulls
        parent_values = set(parent_df[parent_key].dropna().astype(str))
        child_values = set(child_df[child_key].dropna().astype(str))

        # Calculate relationship metrics
        total_child_refs = len(child_df[child_df[child_key].notna()])
        valid_refs = len(
            [v for v in child_df[child_key].dropna().astype(str) if v in parent_values]
        )
        invalid_refs = total_child_refs - valid_refs

        referential_integrity = (
            (valid_refs / total_child_refs * 100) if total_child_refs > 0 else 0
        )

        results.append(
            {
                "parent_table": parent_table,
                "parent_key": parent_key,
                "child_table": child_table,
                "child_key": child_key,
                "parent_unique_values": len(parent_values),
                "child_total_refs": total_child_refs,
                "valid_refs": valid_refs,
                "invalid_refs": invalid_refs,
                "referential_integrity_pct": round(referential_integrity, 2),
                "relationship_type": (
                    "self-referencing" if parent_table == child_table else "foreign_key"
                ),
            }
        )

    return results


def analyze_data_coverage(datasets):
    """Analyze data coverage and quality metrics"""
    coverage = []

    for filename, df in datasets.items():
        for col in df.columns:
            null_count = df[col].isnull().sum()
            null_pct = (null_count / len(df)) * 100
            unique_count = df[col].nunique()

            coverage.append(
                {
                    "table": filename,
                    "column": col,
                    "total_rows": len(df),
                    "null_count": null_count,
                    "null_percentage": round(null_pct, 2),
                    "unique_values": unique_count,
                    "data_type": str(df[col].dtype),
                }
            )

    return coverage


def generate_report(datasets, fk_results, coverage_results):
    """Generate comprehensive analysis report"""

    print("\n" + "=" * 80)
    print("DEMANDBASE DATASET RELATIONSHIP ANALYSIS")
    print("=" * 80)

    # Dataset summary
    print(f"\nDATASET SUMMARY:")
    total_rows = sum(len(df) for df in datasets.values())
    print(f"Total files: {len(datasets)}")
    print(f"Total rows: {total_rows:,}")

    for filename, df in datasets.items():
        print(f"  {filename}: {len(df):,} rows × {len(df.columns)} columns")

    # Foreign key analysis
    print(f"\nFOREIGN KEY RELATIONSHIP ANALYSIS:")
    print("-" * 60)

    valid_relationships = [r for r in fk_results if r["referential_integrity_pct"] > 80]
    broken_relationships = [
        r for r in fk_results if r["referential_integrity_pct"] <= 80
    ]

    print(f"Strong relationships (>80% integrity): {len(valid_relationships)}")
    print(f"Broken relationships (≤80% integrity): {len(broken_relationships)}")

    print(f"\nSTRONG RELATIONSHIPS:")
    for rel in valid_relationships:
        print(
            f"  {rel['parent_table']}({rel['parent_key']}) → {rel['child_table']}({rel['child_key']})"
        )
        print(
            f"    Integrity: {rel['referential_integrity_pct']}% ({rel['valid_refs']:,}/{rel['child_total_refs']:,} valid)"
        )

    print(f"\nBROKEN RELATIONSHIPS:")
    for rel in broken_relationships:
        print(
            f"  {rel['parent_table']}({rel['parent_key']}) → {rel['child_table']}({rel['child_key']})"
        )
        print(
            f"    Integrity: {rel['referential_integrity_pct']}% ({rel['valid_refs']:,}/{rel['child_total_refs']:,} valid)"
        )

    # Data quality summary
    print(f"\nDATA QUALITY SUMMARY:")
    print("-" * 60)

    high_null_cols = [c for c in coverage_results if c["null_percentage"] > 30]
    key_columns = [
        c
        for c in coverage_results
        if any(
            key in c["column"].lower()
            for key in ["id", "company_id", "employment_id", "keyword_set_id"]
        )
    ]

    print(f"Columns with >30% nulls: {len(high_null_cols)}")
    print(f"Key columns analyzed: {len(key_columns)}")

    print(f"\nKEY COLUMN QUALITY:")
    for col in key_columns:
        print(
            f"  {col['table']} | {col['column']}: {col['null_percentage']}% null, {col['unique_values']:,} unique"
        )


def setup_output_directory():
    """Create output directory and clean previous results"""
    output_dir = Path("private/analysis")

    # Create directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)

    # Clean existing files
    for file in output_dir.glob("*.csv"):
        file.unlink()

    return output_dir


def save_detailed_results(fk_results, coverage_results, output_dir):
    """Save detailed results to CSV files in output directory"""

    # Save foreign key analysis
    fk_df = pd.DataFrame(fk_results)
    fk_df.to_csv(output_dir / "relationship_analysis.csv", index=False)

    # Save data coverage
    coverage_df = pd.DataFrame(coverage_results)
    coverage_df.to_csv(output_dir / "data_coverage_analysis.csv", index=False)

    print(f"\nDETAILED RESULTS SAVED TO {output_dir}:")
    print(
        f"  {output_dir}/relationship_analysis.csv - Foreign key relationship metrics"
    )
    print(f"  {output_dir}/data_coverage_analysis.csv - Column-level data quality")


def main():
    try:
        # Setup output directory
        output_dir = setup_output_directory()

        # Load datasets
        datasets = load_datasets()

        # Perform analyses
        print(f"\nAnalyzing foreign key relationships...")
        fk_results = analyze_foreign_keys(datasets)

        print(f"Analyzing data coverage...")
        coverage_results = analyze_data_coverage(datasets)

        # Generate report
        generate_report(datasets, fk_results, coverage_results)

        # Save detailed results
        save_detailed_results(fk_results, coverage_results, output_dir)

    except Exception as e:
        print(f"ERROR: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
