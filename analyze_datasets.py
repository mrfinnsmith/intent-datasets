import pandas as pd
import numpy as np
import os
from pathlib import Path
from datetime import datetime

# Define expected schemas based on the PDF documentation
SCHEMAS = {
    "db_company_intent_geo_weekly_file_sample.csv": {
        "company_id": "int64",
        "domain": "object",
        "start_date": "object",  # Will parse as date
        "end_date": "object",
        "duration_type": "object",
        "keyword_set_id": "int64",
        "keyword_set": "object",
        "keyword": "object",
        "country": "object",
        "census_division": "object",
        "region": "object",
        "city": "object",
        "num_people_researching": "int64",
        "intent_strength": "object",
        "partition_date": "object",
    },
    "db_company_intent_geo_weekly_keywordset_contact_scores_file_sample.csv": {
        "dt": "object",
        "keyword_set_id": "int64",
        "company_id": "int64",
        "employment_id": "int64",
        "intent_score": "int64",
        "partition_date": "object",
    },
    "db_company_file_sample.csv": {
        "company_id": "int64",
        "employees": "float64",
        "revenue": "float64",
        "isroot": "int64",
        "best_domain": "bool",
    },
    "db_company_full_file_sample.csv": {
        "company_id": "int64",
        "employees": "float64",
        "revenue": "float64",
        "isroot": "float64",
        "sic": "float64",
    },
    "db_contacts_file_sample.csv": {
        "executive_id": "int64",
        "employment_id": "int64",
        "company_id": "int64",
        "revenue_usd": "float64",
        "employees": "float64",
        "sic_us": "float64",
        "naics": "float64",
        "equifax_id": "float64",
    },
    "db1_keyword_sets_file_sample.csv": {"id": "int64", "competitive": "bool"},
    "db1_keyword_set_keywords_file_sample.csv": {"keyword_set_id": "int64"},
}


def analyze_csv_files(directory="private/datasets"):
    results = []

    # Get all CSV files
    csv_files = sorted(Path(directory).glob("*.csv"))

    for csv_file in csv_files:
        print(f"Analyzing {csv_file.name}...")

        # Read CSV
        df = pd.read_csv(csv_file)

        # Basic stats
        stats = {
            "file": csv_file.name,
            "rows": len(df),
            "columns": len(df.columns),
            "file_size_mb": round(csv_file.stat().st_size / (1024 * 1024), 2),
            "memory_usage_mb": round(
                df.memory_usage(deep=True).sum() / (1024 * 1024), 2
            ),
        }

        # Null analysis
        null_counts = df.isnull().sum()
        stats["total_nulls"] = null_counts.sum()
        stats["columns_with_nulls"] = (null_counts > 0).sum()
        stats["null_percentage"] = round(
            (stats["total_nulls"] / (stats["rows"] * stats["columns"]) * 100), 2
        )

        # Data type mismatches
        mismatches = []
        if csv_file.name in SCHEMAS:
            schema = SCHEMAS[csv_file.name]
            for col, expected_type in schema.items():
                if col in df.columns:
                    actual_type = str(df[col].dtype)
                    if expected_type == "bool" and actual_type not in [
                        "bool",
                        "boolean",
                    ]:
                        mismatches.append(f"{col}: expected bool, got {actual_type}")
                    elif (
                        expected_type in ["int64", "float64"]
                        and "object" in actual_type
                    ):
                        mismatches.append(f"{col}: expected numeric, got {actual_type}")

        stats["type_mismatches"] = len(mismatches)
        stats["mismatch_details"] = ", ".join(mismatches[:3]) + (
            "..." if len(mismatches) > 3 else ""
        )

        # Unique value analysis for key columns
        if "company_id" in df.columns:
            stats["unique_companies"] = df["company_id"].nunique()
        if "employment_id" in df.columns:
            stats["unique_contacts"] = df["employment_id"].nunique()
        if "keyword" in df.columns:
            stats["unique_keywords"] = df["keyword"].nunique()

        # Date range for temporal data
        date_cols = [
            col
            for col in df.columns
            if "date" in col.lower() and col != "partition_date"
        ]
        if date_cols:
            try:
                date_col = date_cols[0]
                df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
                stats["date_range"] = f"{df[date_col].min()} to {df[date_col].max()}"
            except:
                pass

        results.append(stats)

    return results


def generate_markdown_report(results, output_file="data_summary.md"):
    with open(output_file, "w") as f:
        f.write("# Dataset Summary Statistics\n\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

        # Overview table
        f.write("## Overview\n\n")
        f.write(
            "| File | Rows | Columns | File Size (MB) | Memory (MB) | Nulls (%) | Type Issues |\n"
        )
        f.write(
            "|------|------|---------|----------------|-------------|-----------|-------------|\n"
        )

        for r in results:
            f.write(
                f"| {r['file']} | {r['rows']:,} | {r['columns']} | {r['file_size_mb']} | {r['memory_usage_mb']} | {r['null_percentage']}% | {r['type_mismatches']} |\n"
            )

        # Detailed findings
        f.write("\n## Key Findings\n\n")

        # Data coverage
        f.write("### Data Coverage\n\n")
        for r in results:
            if "unique_companies" in r:
                f.write(
                    f"- **{r['file']}**: {r['unique_companies']:,} unique companies\n"
                )
            if "unique_contacts" in r:
                f.write(
                    f"- **{r['file']}**: {r['unique_contacts']:,} unique contacts\n"
                )
            if "unique_keywords" in r:
                f.write(
                    f"- **{r['file']}**: {r['unique_keywords']:,} unique keywords\n"
                )

        # Data quality issues
        f.write("\n### Data Quality Issues\n\n")
        issues_found = False
        for r in results:
            if r["type_mismatches"] > 0:
                f.write(
                    f"- **{r['file']}** has type mismatches: {r['mismatch_details']}\n"
                )
                issues_found = True
            if r["null_percentage"] > 10:
                f.write(
                    f"- **{r['file']}** has high null rate: {r['null_percentage']}%\n"
                )
                issues_found = True

        if not issues_found:
            f.write("- No significant data quality issues detected\n")

        # Temporal coverage
        f.write("\n### Temporal Coverage\n\n")
        for r in results:
            if "date_range" in r:
                f.write(f"- **{r['file']}**: {r['date_range']}\n")

        # Summary stats
        f.write("\n## Summary Statistics\n\n")
        total_rows = sum(r["rows"] for r in results)
        total_size = sum(r["file_size_mb"] for r in results)
        f.write(f"- Total rows across all files: {total_rows:,}\n")
        f.write(f"- Total file size: {total_size:.1f} MB\n")
        f.write(
            f"- Average null rate: {np.mean([r['null_percentage'] for r in results]):.1f}%\n"
        )


if __name__ == "__main__":
    results = analyze_csv_files()
    generate_markdown_report(results)
    print("\nReport generated: data_summary.md")
