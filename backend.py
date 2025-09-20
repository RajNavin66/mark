import psycopg2
from psycopg2 import extras
import json
from datetime import datetime
import os

# Database connection details, replace with your own configuration
DB_NAME = os.environ.get("DB_NAME", "mar")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "1230")
DB_HOST = os.environ.get("DB_HOST", "localhost")

def get_db_connection():
    """Establishes and returns a connection to the PostgreSQL database."""
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=mar,
            user=postgres,
            password=1230,
            host=DB_HOST
        )
        print("Database connection successful.")
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to the database: {e}")
        return None

# --- CRUD Operations for Campaigns ---

def create_campaign(name, budget, start_date, end_date, description, channels):
    """Creates a new campaign and its associated channels."""
    conn = get_db_connection()
    if conn is None:
        return False, "Database connection failed."
    try:
        with conn.cursor() as cur:
            # Insert campaign details
            cur.execute(
                "INSERT INTO campaigns (name, budget, start_date, end_date, description) VALUES (%s, %s, %s, %s, %s) RETURNING id;",
                (name, budget, start_date, end_date, description)
            )
            campaign_id = cur.fetchone()[0]

            # Insert associated channels
            for channel in channels:
                cur.execute(
                    "INSERT INTO campaign_channels (campaign_id, channel_name) VALUES (%s, %s);",
                    (campaign_id, channel)
                )

        conn.commit()
        return True, f"Campaign '{name}' created successfully."
    except psycopg2.Error as e:
        conn.rollback()
        return False, f"Error creating campaign: {e}"
    finally:
        if conn:
            conn.close()

def read_campaigns():
    """Retrieves all campaigns with their associated channels."""
    conn = get_db_connection()
    if conn is None:
        return []
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM campaigns ORDER BY start_date DESC;")
            campaigns = cur.fetchall()

            # For each campaign, fetch its channels
            for campaign in campaigns:
                cur.execute("SELECT channel_name FROM campaign_channels WHERE campaign_id = %s;", (campaign['id'],))
                channels = [row['channel_name'] for row in cur.fetchall()]
                campaign['channels'] = channels
        return campaigns
    except psycopg2.Error as e:
        print(f"Error reading campaigns: {e}")
        return []
    finally:
        if conn:
            conn.close()

def update_campaign(campaign_id, name, budget, start_date, end_date, description):
    """Updates an existing campaign's details."""
    conn = get_db_connection()
    if conn is None:
        return False, "Database connection failed."
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE campaigns SET name = %s, budget = %s, start_date = %s, end_date = %s, description = %s WHERE id = %s;",
                (name, budget, start_date, end_date, description, campaign_id)
            )
        conn.commit()
        return True, f"Campaign ID {campaign_id} updated successfully."
    except psycopg2.Error as e:
        conn.rollback()
        return False, f"Error updating campaign: {e}"
    finally:
        if conn:
            conn.close()

def delete_campaign(campaign_id):
    """Deletes a campaign and its associated records."""
    conn = get_db_connection()
    if conn is None:
        return False, "Database connection failed."
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM campaigns WHERE id = %s;", (campaign_id,))
        conn.commit()
        return True, f"Campaign ID {campaign_id} deleted successfully."
    except psycopg2.Error as e:
        conn.rollback()
        return False, f"Error deleting campaign: {e}"
    finally:
        if conn:
            conn.close()

# --- CRUD Operations for Customers ---

def create_customer(name, email, demographics):
    """Adds a new customer to the database."""
    conn = get_db_connection()
    if conn is None:
        return False, "Database connection failed."
    try:
        with conn.cursor() as cur:
            demographics_json = json.dumps(demographics)
            cur.execute(
                "INSERT INTO customers (name, email, demographics) VALUES (%s, %s, %s);",
                (name, email, demographics_json)
            )
        conn.commit()
        return True, f"Customer '{name}' added successfully."
    except psycopg2.Error as e:
        conn.rollback()
        return False, f"Error adding customer: {e}"
    finally:
        if conn:
            conn.close()

def read_customers():
    """Retrieves all customers from the database."""
    conn = get_db_connection()
    if conn is None:
        return []
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM customers ORDER BY created_at DESC;")
            return cur.fetchall()
    except psycopg2.Error as e:
        print(f"Error reading customers: {e}")
        return []
    finally:
        if conn:
            conn.close()

def update_customer(customer_id, name, email, demographics):
    """Updates an existing customer's details."""
    conn = get_db_connection()
    if conn is None:
        return False, "Database connection failed."
    try:
        with conn.cursor() as cur:
            demographics_json = json.dumps(demographics)
            cur.execute(
                "UPDATE customers SET name = %s, email = %s, demographics = %s WHERE id = %s;",
                (name, email, demographics_json, customer_id)
            )
        conn.commit()
        return True, f"Customer ID {customer_id} updated successfully."
    except psycopg2.Error as e:
        conn.rollback()
        return False, f"Error updating customer: {e}"
    finally:
        if conn:
            conn.close()

def delete_customer(customer_id):
    """Deletes a customer from the database."""
    conn = get_db_connection()
    if conn is None:
        return False, "Database connection failed."
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM customers WHERE id = %s;", (customer_id,))
        conn.commit()
        return True, f"Customer ID {customer_id} deleted successfully."
    except psycopg2.Error as e:
        conn.rollback()
        return False, f"Error deleting customer: {e}"
    finally:
        if conn:
            conn.close()

# --- CRUD Operations for Segments ---

def create_segment(segment_name, criteria):
    """Creates a new customer segment."""
    conn = get_db_connection()
    if conn is None:
        return False, "Database connection failed."
    try:
        with conn.cursor() as cur:
            criteria_json = json.dumps(criteria)
            cur.execute(
                "INSERT INTO segments (segment_name, criteria) VALUES (%s, %s);",
                (segment_name, criteria_json)
            )
        conn.commit()
        return True, f"Segment '{segment_name}' created successfully."
    except psycopg2.Error as e:
        conn.rollback()
        return False, f"Error creating segment: {e}"
    finally:
        if conn:
            conn.close()

def read_segments():
    """Retrieves all segments from the database."""
    conn = get_db_connection()
    if conn is None:
        return []
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM segments ORDER BY created_at DESC;")
            return cur.fetchall()
    except psycopg2.Error as e:
        print(f"Error reading segments: {e}")
        return []
    finally:
        if conn:
            conn.close()

def delete_segment(segment_id):
    """Deletes a segment from the database."""
    conn = get_db_connection()
    if conn is None:
        return False, "Database connection failed."
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM segments WHERE id = %s;", (segment_id,))
        conn.commit()
        return True, f"Segment ID {segment_id} deleted successfully."
    except psycopg2.Error as e:
        conn.rollback()
        return False, f"Error deleting segment: {e}"
    finally:
        if conn:
            conn.close()

# --- Performance Tracking and Mock Data Generation ---

def add_performance_data(campaign_id, metric_name, value):
    """Adds a new performance metric record for a campaign."""
    conn = get_db_connection()
    if conn is None:
        return False, "Database connection failed."
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO campaign_performance (campaign_id, metric_name, value) VALUES (%s, %s, %s);",
                (campaign_id, metric_name, value)
            )
        conn.commit()
        return True, "Performance data added successfully."
    except psycopg2.Error as e:
        conn.rollback()
        return False, f"Error adding performance data: {e}"
    finally:
        if conn:
            conn.close()

def get_performance_data_by_campaign(campaign_id):
    """Retrieves all performance data for a given campaign."""
    conn = get_db_connection()
    if conn is None:
        return []
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT metric_name, value FROM campaign_performance WHERE campaign_id = %s ORDER BY timestamp ASC;",
                (campaign_id,)
            )
            return cur.fetchall()
    except psycopg2.Error as e:
        print(f"Error reading performance data: {e}")
        return []
    finally:
        if conn:
            conn.close()

# --- Business Insights Functions ---

def get_total_campaign_count():
    """Returns the total number of campaigns."""
    conn = get_db_connection()
    if conn is None: return 0
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM campaigns;")
            return cur.fetchone()[0]
    except psycopg2.Error as e:
        print(f"Error getting total campaign count: {e}")
        return 0
    finally:
        if conn:
            conn.close()

def get_total_customers_count():
    """Returns the total number of customers."""
    conn = get_db_connection()
    if conn is None: return 0
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM customers;")
            return cur.fetchone()[0]
    except psycopg2.Error as e:
        print(f"Error getting total customer count: {e}")
        return 0
    finally:
        if conn:
            conn.close()

def get_avg_campaign_budget():
    """Returns the average budget of all campaigns."""
    conn = get_db_connection()
    if conn is None: return 0.0
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT AVG(budget) FROM campaigns;")
            result = cur.fetchone()[0]
            return float(result) if result else 0.0
    except psycopg2.Error as e:
        print(f"Error getting average campaign budget: {e}")
        return 0.0
    finally:
        if conn:
            conn.close()

def get_max_campaign_budget():
    """Returns the maximum budget of any campaign."""
    conn = get_db_connection()
    if conn is None: return 0.0
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(budget) FROM campaigns;")
            result = cur.fetchone()[0]
            return float(result) if result else 0.0
    except psycopg2.Error as e:
        print(f"Error getting max campaign budget: {e}")
        return 0.0
    finally:
        if conn:
            conn.close()

def get_min_campaign_budget():
    """Returns the minimum budget of any campaign."""
    conn = get_db_connection()
    if conn is None: return 0.0
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT MIN(budget) FROM campaigns;")
            result = cur.fetchone()[0]
            return float(result) if result else 0.0
    except psycopg2.Error as e:
        print(f"Error getting min campaign budget: {e}")
        return 0.0
    finally:
        if conn:
            conn.close()

def get_total_emails_sent():
    """Returns the total value for the 'emails_sent' metric across all campaigns."""
    conn = get_db_connection()
    if conn is None: return 0
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COALESCE(SUM(value), 0) FROM campaign_performance WHERE metric_name = 'emails_sent';")
            return cur.fetchone()[0]
    except psycopg2.Error as e:
        print(f"Error getting total emails sent: {e}")
        return 0
    finally:
        if conn:
            conn.close()
