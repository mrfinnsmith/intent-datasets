import sqlite3
import pandas as pd

# Connect and query
conn = sqlite3.connect("../private/data/database/intent-data.db")

# High-intent contacts
high_intent = pd.read_sql(
    """
    SELECT 
        c.company_name,
        co.first_name || ' ' || co.last_name as name,
        co.title,
        co.job_function,
        cs.intent_score,
        co.email
    FROM companies c
    JOIN contacts co ON c.company_id = co.company_id  
    JOIN contact_intent_scores cs ON co.employment_id = cs.employment_id
    WHERE cs.intent_score > 70
    ORDER BY cs.intent_score DESC
""",
    conn,
)

print("High-intent contacts:")
print(high_intent.head())

# Company intent summary
company_summary = pd.read_sql(
    """
    SELECT 
        c.company_name,
        c.employees,
        c.revenue,
        AVG(cs.intent_score) as avg_intent,
        COUNT(cs.employment_id) as contact_count
    FROM companies c
    JOIN contacts co ON c.company_id = co.company_id
    JOIN contact_intent_scores cs ON co.employment_id = cs.employment_id
    GROUP BY c.company_id, c.company_name, c.employees, c.revenue
    HAVING contact_count > 1
    ORDER BY avg_intent DESC
""",
    conn,
)

print("\nCompany intent summary:")
print(company_summary.head())

# Job function analysis
job_analysis = pd.read_sql(
    """
    SELECT 
        co.job_function,
        COUNT(*) as contact_count,
        AVG(cs.intent_score) as avg_intent_score,
        MAX(cs.intent_score) as max_intent_score
    FROM contacts co
    JOIN contact_intent_scores cs ON co.employment_id = cs.employment_id
    WHERE co.job_function IS NOT NULL
    GROUP BY co.job_function
    ORDER BY avg_intent_score DESC
""",
    conn,
)

print("\nJob function intent analysis:")
print(job_analysis)

conn.close()
