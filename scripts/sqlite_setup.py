import sqlite3
import pandas as pd
from pathlib import Path


def setup_sqlite_db(
    csv_dir="../private/data/raw", db_path="../private/data/database/intent-data.db"
):
    """Load all CSVs into SQLite database with proper table names"""

    # Create connection
    conn = sqlite3.connect(db_path)

    # Table name mapping (remove file extensions, clean names)
    table_mapping = {
        "db1_keyword_sets_file_sample.csv": "keyword_sets",
        "db1_keyword_set_keywords_file_sample.csv": "keyword_set_keywords",
        "db_company_file_sample.csv": "companies",
        "db_company_full_file_sample.csv": "companies_full",
        "db_contacts_file_sample.csv": "contacts",
        "db_company_intent_geo_weekly_file_sample.csv": "company_intent_geo",
        "db_company_intent_geo_weekly_keywordset_contact_scores_file_sample.csv": "contact_intent_scores",
    }

    csv_path = Path(csv_dir)

    for csv_file in csv_path.glob("*.csv"):
        table_name = table_mapping.get(csv_file.name, csv_file.stem)

        print(f"Loading {csv_file.name} -> {table_name}")
        df = pd.read_csv(csv_file, low_memory=False)
        df.to_sql(table_name, conn, if_exists="replace", index=False)

    # Create indexes on key fields
    indexes = [
        "CREATE INDEX idx_companies_id ON companies(company_id)",
        "CREATE INDEX idx_contacts_company ON contacts(company_id)",
        "CREATE INDEX idx_contacts_employment ON contacts(employment_id)",
        "CREATE INDEX idx_intent_scores_company ON contact_intent_scores(company_id)",
        "CREATE INDEX idx_intent_scores_employment ON contact_intent_scores(employment_id)",
        "CREATE INDEX idx_intent_geo_company ON company_intent_geo(company_id)",
    ]

    for idx in indexes:
        try:
            conn.execute(idx)
        except:
            pass  # Index might already exist

    conn.commit()
    conn.close()
    print(f"\nDatabase created: {db_path}")
    return db_path


# Usage
if __name__ == "__main__":
    db_path = setup_sqlite_db()

    # Test query
    conn = sqlite3.connect(db_path)
    result = pd.read_sql(
        """
        SELECT 
            c.company_name,
            co.first_name || ' ' || co.last_name as contact_name,
            co.title,
            cs.intent_score
        FROM companies c
        JOIN contacts co ON c.company_id = co.company_id  
        JOIN contact_intent_scores cs ON co.employment_id = cs.employment_id
        WHERE cs.intent_score > 80
        LIMIT 10
    """,
        conn,
    )

    print("\nHigh-intent contacts:")
    print(result)
    conn.close()
