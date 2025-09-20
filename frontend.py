import streamlit as st
import pandas as pd
from datetime import date
from backend_fin import (
    create_campaign, read_campaigns, update_campaign, delete_campaign,
    create_customer, read_customers, update_customer, delete_customer,
    create_segment, read_segments, delete_segment,
    add_performance_data, get_performance_data_by_campaign,
    get_total_campaign_count, get_total_customers_count,
    get_avg_campaign_budget, get_max_campaign_budget, get_min_campaign_budget,
    get_total_emails_sent
)
import random

# Set Streamlit page configuration
st.set_page_config(layout="wide", page_title="Marketing Campaign Manager")

st.title("Marketing Campaign Manager")
st.write("Plan, execute, and track your marketing campaigns.")

# --- Sidebar Navigation ---
st.sidebar.title("Navigation")
page = st.sidebar.radio(
    "Go to",
    ("Campaigns", "Customers", "Segments", "Performance Dashboard", "Business Insights")
)

# --- Campaign Management Section ---
if page == "Campaigns":
    st.header("Campaign Management")

    with st.expander("Create a New Campaign"):
        with st.form("new_campaign_form"):
            name = st.text_input("Campaign Name", max_chars=255, help="Enter a descriptive name for your campaign.")
            budget = st.number_input("Budget ($)", min_value=0.0, format="%.2f", help="Set the total budget for this campaign.")
            col1, col2 = st.columns(2)
            with col1:
                start_date = st.date_input("Start Date", value=date.today())
            with col2:
                end_date = st.date_input("End Date")
            description = st.text_area("Description", help="Provide a detailed description of the campaign's goals and strategy.")
            channels = st.multiselect(
                "Marketing Channels",
                ["Email", "Social Media", "Paid Ads", "Content Marketing", "SMS"],
                help="Select all the channels used for this campaign."
            )

            submit_button = st.form_submit_button("Create Campaign")
            if submit_button:
                if name and budget > 0 and start_date and end_date:
                    success, message = create_campaign(name, budget, start_date, end_date, description, channels)
                    if success:
                        st.success(message)
                    else:
                        st.error(message)
                else:
                    st.warning("Please fill in all required fields.")

    st.subheader("Existing Campaigns")
    campaigns_data = read_campaigns()
    df_campaigns = pd.DataFrame(campaigns_data)

    if not df_campaigns.empty:
        st.dataframe(df_campaigns, use_container_width=True)

        selected_campaign = st.selectbox("Select a campaign to update or delete:", df_campaigns['name'], index=0)
        selected_row = df_campaigns[df_campaigns['name'] == selected_campaign].iloc[0]

        st.subheader(f"Update/Delete '{selected_campaign}'")
        with st.form("update_campaign_form"):
            update_name = st.text_input("Name", value=selected_row['name'])
            update_budget = st.number_input("Budget ($)", value=float(selected_row['budget']), min_value=0.0, format="%.2f")
            update_description = st.text_area("Description", value=selected_row['description'])
            update_start_date = st.date_input("Start Date", value=selected_row['start_date'])
            update_end_date = st.date_input("End Date", value=selected_row['end_date'])

            col_upd, col_del = st.columns(2)
            with col_upd:
                if st.form_submit_button("Update Campaign"):
                    success, message = update_campaign(
                        selected_row['id'], update_name, update_budget,
                        update_start_date, update_end_date, update_description
                    )
                    if success:
                        st.success(message)
                        st.experimental_rerun()
                    else:
                        st.error(message)

            with col_del:
                if st.form_submit_button("Delete Campaign"):
                    success, message = delete_campaign(selected_row['id'])
                    if success:
                        st.success(message)
                        st.experimental_rerun()
                    else:
                        st.error(message)
    else:
        st.info("No campaigns found. Create one to get started!")

# --- Customer Management Section ---
elif page == "Customers":
    st.header("Customer Management")
    with st.expander("Add New Customer"):
        with st.form("new_customer_form"):
            name = st.text_input("Full Name")
            email = st.text_input("Email")
            demographics = st.text_area(
                "Demographics (e.g., {'city': 'New York', 'age': 30})",
                "{}", help="Enter customer demographics as a JSON string."
            )
            submit_button = st.form_submit_button("Add Customer")

            if submit_button:
                try:
                    demographics_json = eval(demographics)
                    if not isinstance(demographics_json, dict):
                        raise ValueError
                    success, message = create_customer(name, email, demographics_json)
                    if success:
                        st.success(message)
                    else:
                        st.error(message)
                except (ValueError, SyntaxError):
                    st.error("Invalid demographics format. Please use a valid JSON dictionary.")

    st.subheader("Customer Database")
    customers_data = read_customers()
    if customers_data:
        df_customers = pd.DataFrame(customers_data)
        st.dataframe(df_customers, use_container_width=True)
    else:
        st.info("No customers found.")

# --- Segments Section ---
elif page == "Segments":
    st.header("Customer Segmentation")
    with st.expander("Create a New Segment"):
        with st.form("new_segment_form"):
            segment_name = st.text_input("Segment Name")
            criteria = st.text_area(
                "Dynamic Criteria (e.g., {'city': 'New York', 'purchase_history': 'last_6_months'})",
                "{}", help="Define the criteria for this segment as a JSON string."
            )
            submit_button = st.form_submit_button("Create Segment")

            if submit_button:
                try:
                    criteria_json = eval(criteria)
                    if not isinstance(criteria_json, dict):
                        raise ValueError
                    success, message = create_segment(segment_name, criteria_json)
                    if success:
                        st.success(message)
                    else:
                        st.error(message)
                except (ValueError, SyntaxError):
                    st.error("Invalid criteria format. Please use a valid JSON dictionary.")

    st.subheader("Existing Segments")
    segments_data = read_segments()
    if segments_data:
        df_segments = pd.DataFrame(segments_data)
        st.dataframe(df_segments, use_container_width=True)

        selected_segment = st.selectbox("Select a segment to delete:", df_segments['segment_name'], index=0)
        selected_row = df_segments[df_segments['segment_name'] == selected_segment].iloc[0]
        if st.button(f"Delete '{selected_segment}'"):
            success, message = delete_segment(selected_row['id'])
            if success:
                st.success(message)
                st.experimental_rerun()
            else:
                st.error(message)
    else:
        st.info("No segments found.")

# --- Performance Dashboard Section ---
elif page == "Performance Dashboard":
    st.header("Real-Time Performance Dashboard")

    campaigns_data = read_campaigns()
    if not campaigns_data:
        st.info("No campaigns to track. Please create a campaign first.")
    else:
        df_campaigns = pd.DataFrame(campaigns_data)
        campaign_names = df_campaigns['name'].tolist()
        selected_campaign_name = st.selectbox("Select a campaign to view performance:", campaign_names)
        
        selected_campaign_id = df_campaigns[df_campaigns['name'] == selected_campaign_name].iloc[0]['id']

        # Mock Data Generation (for demonstration)
        st.sidebar.subheader("Generate Mock Performance Data")
        if st.sidebar.button("Generate Random Data"):
            emails_sent = random.randint(1000, 10000)
            emails_opened = random.randint(emails_sent // 5, emails_sent // 2)
            emails_clicked = random.randint(emails_opened // 5, emails_opened // 2)

            add_performance_data(selected_campaign_id, 'emails_sent', emails_sent)
            add_performance_data(selected_campaign_id, 'emails_opened', emails_opened)
            add_performance_data(selected_campaign_id, 'emails_clicked', emails_clicked)
            st.success("Mock performance data generated!")
            st.experimental_rerun()

        # Display Performance Metrics
        performance_data = get_performance_data_by_campaign(selected_campaign_id)
        if performance_data:
            df_performance = pd.DataFrame(performance_data)
            df_performance['metric_name'] = df_performance['metric_name'].str.replace('_', ' ').str.title()
            
            # Use columns to display key metrics
            sent, opened, clicked = st.columns(3)
            with sent:
                sent_value = df_performance[df_performance['metric_name'] == 'Emails Sent']['value'].sum()
                st.metric("Emails Sent", f"{sent_value:,}")
            with opened:
                opened_value = df_performance[df_performance['metric_name'] == 'Emails Opened']['value'].sum()
                st.metric("Emails Opened", f"{opened_value:,}")
            with clicked:
                clicked_value = df_performance[df_performance['metric_name'] == 'Emails Clicked']['value'].sum()
                st.metric("Emails Clicked", f"{clicked_value:,}")

            # Visualize performance trends
            st.subheader("Performance Trends")
            st.line_chart(df_performance.pivot_table(index='metric_name', values='value', aggfunc='sum'))
            st.bar_chart(df_performance, x='metric_name', y='value')
        else:
            st.info("No performance data available for this campaign. Generate some using the sidebar.")

# --- Business Insights Section ---
elif page == "Business Insights":
    st.header("Business Insights")
    st.write("Leveraging data to provide key business insights.")

    col1, col2, col3 = st.columns(3)
    with col1:
        total_campaigns = get_total_campaign_count()
        st.metric("Total Campaigns", total_campaigns)
    with col2:
        total_customers = get_total_customers_count()
        st.metric("Total Customers", total_customers)
    with col3:
        total_emails_sent = get_total_emails_sent()
        st.metric("Total Emails Sent", f"{total_emails_sent:,}")

    st.subheader("Campaign Budget Analysis")
    col4, col5, col6 = st.columns(3)
    with col4:
        avg_budget = get_avg_campaign_budget()
        st.metric("Avg. Campaign Budget", f"${avg_budget:,.2f}")
    with col5:
        max_budget = get_max_campaign_budget()
        st.metric("Max Campaign Budget", f"${max_budget:,.2f}")
    with col6:
        min_budget = get_min_campaign_budget()
        st.metric("Min Campaign Budget", f"${min_budget:,.2f}")
