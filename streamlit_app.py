
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import folium
from streamlit_folium import folium_static

###############################################
# 1. LOAD DATA
###############################################
def load_data():
    """Load the main property dataset from CSV."""
    file_path = "zillow_data_cleaned.csv"  # Adjust path if needed
    df = pd.read_csv(file_path)
    return df

def load_users():
    """Load user (agent/broker) data from CSV."""
    user_df = pd.read_csv("users.csv")  # Adjust path if needed
    return user_df

###############################################
# 2. MAIN APP
###############################################
def main():
    st.title("Q-Task Rentals Prospecting")
    
    # --- Load Data ---
    df = load_data()
    user_df = load_users()
    
    # Force only EXTRACTED_YEAR = 2024 and EXTRACTED_MONTH = 4 (April 2024)
    df = df[(df["EXTRACTED_YEAR"] == 2024) & (df["EXTRACTED_MONTH"] == 4)]
    
    ###############################################
    # USER DROPDOWN (Agent/Broker Selection)
    ###############################################
    st.sidebar.header("Select Agent")
    agent_names = user_df["NAME"].tolist()
    selected_agent_name = st.sidebar.selectbox("Choose an Agent", agent_names)
    selected_agent = user_df[user_df["NAME"] == selected_agent_name].iloc[0]
    
    ###############################################
    # FILTERS (ZIP Codes, Price Range, Beds)
    ###############################################
    st.subheader("Filters")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        selected_zip = st.multiselect(
            "Select up to 5 ZIP Codes:",
            options=sorted(df["ZIPCODEZIL"].unique()),
            default=[],
            help="Choose up to 5 ZIP codes to target."
        )
        if len(selected_zip) > 5:
            st.warning("You have selected more than 5 ZIP codes. Please reduce your selection to 5 or fewer.")
            st.stop()
    
    with col2:
        # Define price options: options start at 2000 in 500 increments up to 4000,
        # with additional options for less than 2000 and greater than 4000.
        price_options = [
            "Less than 2000", 
            "2000-2500", 
            "2500-3000", 
            "3000-3500", 
            "3500-4000", 
            "Greater than 4000"
        ]
        selected_price_range = st.selectbox("Select Price Range", price_options, index=1)
    
    with col3:
        if not df.empty and "BEDS" in df.columns:
            bed_options = sorted(df["BEDS"].dropna().unique())
        else:
            bed_options = []
        selected_beds = st.multiselect("Select Number of Beds:", options=bed_options, default=[])
    
    # Apply filters
    filtered_df = df.copy()
    
    # Apply price filter based on selected_price_range
    if selected_price_range == "Less than 2000":
        filtered_df = filtered_df[filtered_df["PRICE"] < 2000]
    elif selected_price_range == "2000-2500":
        filtered_df = filtered_df[(filtered_df["PRICE"] >= 2000) & (filtered_df["PRICE"] < 2500)]
    elif selected_price_range == "2500-3000":
        filtered_df = filtered_df[(filtered_df["PRICE"] >= 2500) & (filtered_df["PRICE"] < 3000)]
    elif selected_price_range == "3000-3500":
        filtered_df = filtered_df[(filtered_df["PRICE"] >= 3000) & (filtered_df["PRICE"] < 3500)]
    elif selected_price_range == "3500-4000":
        filtered_df = filtered_df[(filtered_df["PRICE"] >= 3500) & (filtered_df["PRICE"] < 4000)]
    elif selected_price_range == "Greater than 4000":
        filtered_df = filtered_df[filtered_df["PRICE"] >= 4000]
    
    # Apply ZIP code and beds filters
    if selected_zip:
        filtered_df = filtered_df[filtered_df["ZIPCODEZIL"].isin(selected_zip)]
    if selected_beds:
        filtered_df = filtered_df[filtered_df["BEDS"].isin(selected_beds)]
    
    ###############################################
    # Display Key Statistics
    ###############################################
    st.subheader("Key Statistics")
    stat_cols = st.columns(6)
    if filtered_df.empty:
        st.warning("No data matches your filter criteria (April 2024).")
        return
    
    total_props = len(filtered_df)
    studio_count = len(filtered_df[filtered_df["BEDS"] == 0])
    one_bed_count = len(filtered_df[filtered_df["BEDS"] == 1])
    two_bed_count = len(filtered_df[filtered_df["BEDS"] == 2])
    three_bed_count = len(filtered_df[filtered_df["BEDS"] == 3])
    more3_bed_count = len(filtered_df[filtered_df["BEDS"] > 3])
    
    mean_price = round(filtered_df["PRICE"].mean(), 2)
    median_price = round(filtered_df["PRICE"].median(), 2)
    mode_price = round(filtered_df["PRICE"].mode()[0], 2) if not filtered_df["PRICE"].mode().empty else "N/A"
    
    metrics = [
        ("Total Properties", total_props),
        ("Studios", studio_count),
        ("1-Bed", one_bed_count),
        ("2-Bed", two_bed_count),
        ("3-Bed", three_bed_count),
        (">3 Bed", more3_bed_count),
        ("Mean Price", mean_price),
        ("Median Price", median_price),
        ("Mode Price", mode_price),
    ]
    for i, (label, value) in enumerate(metrics):
        with stat_cols[i % 6]:
            st.metric(label, value)
    
    ###############################################
    # Visualizations
    ###############################################
    st.subheader("Visualizations")
    vis_col1, vis_col2, vis_col3, vis_col4 = st.columns(4)
    
    with vis_col1:
        fig1 = px.histogram(filtered_df, x="ZIPCODEZIL", title="Properties per ZIP Code")
        st.plotly_chart(fig1, use_container_width=True)
    
    with vis_col2:
        fig2 = px.histogram(filtered_df, x="PRICE", title="Price Distribution", nbins=20)
        st.plotly_chart(fig2, use_container_width=True)
    
    with vis_col3:
        if "LIVINGAREA" in filtered_df.columns:
            fig3 = px.scatter(filtered_df, x="LIVINGAREA", y="PRICE", title="Living Area vs Price", size="BEDS")
            st.plotly_chart(fig3, use_container_width=True)
        else:
            st.write("LIVINGAREA column not found.")
    
    with vis_col4:
        if "BEDS" in filtered_df.columns:
            fig4 = px.box(filtered_df, x="BEDS", y="PRICE", title="Bedrooms vs Price")
            st.plotly_chart(fig4, use_container_width=True)
        else:
            st.write("BEDS column not found.")
    
    extra_col1, extra_col2, extra_col3, extra_col4 = st.columns(4)
    with extra_col1:
        if "CONDITION" in filtered_df.columns:
            fig5 = px.pie(filtered_df, names="CONDITION", title="Property Condition Distribution")
            st.plotly_chart(fig5, use_container_width=True)
        else:
            st.write("CONDITION column not found.")
    
    with extra_col2:
        if "OWNER_TYPE" in filtered_df.columns:
            fig6 = px.histogram(filtered_df, x="OWNER_TYPE", title="Owner Type Distribution")
            st.plotly_chart(fig6, use_container_width=True)
        else:
            st.write("OWNER_TYPE column not found.")
    
    with extra_col3:
        if "EXTRACTED_MONTH" in filtered_df.columns and "PRICE" in filtered_df.columns:
            monthly_price = filtered_df.groupby("EXTRACTED_MONTH")["PRICE"].mean().reset_index()
            fig7 = px.line(monthly_price, x="EXTRACTED_MONTH", y="PRICE", title="Monthly Rental Trends")
            st.plotly_chart(fig7, use_container_width=True)
        else:
            st.write("EXTRACTED_MONTH or PRICE column not found.")
    
    with extra_col4:
        if "YEARBUILT" in filtered_df.columns:
            fig8 = px.scatter(filtered_df, x="YEARBUILT", y="PRICE", title="Year Built vs Price")
            st.plotly_chart(fig8, use_container_width=True)
        else:
            st.write("YEARBUILT column not found.")
    
    ###############################################
    # Map Visualization (Exclude Properties Without Locations)
    ###############################################
    if "LATITUDE" in filtered_df.columns and "LONGITUDE" in filtered_df.columns:
        map_df = filtered_df.dropna(subset=["LATITUDE", "LONGITUDE"])
        st.subheader("Property Map")
        if map_df.empty:
            st.write("No properties with valid location data to display on the map.")
        else:
            map_center = [map_df["LATITUDE"].mean(), map_df["LONGITUDE"].mean()]
            map_ = folium.Map(location=map_center, zoom_start=12)
            for _, row in map_df.iterrows():
                folium.Marker(
                    [row["LATITUDE"], row["LONGITUDE"]],
                    popup=f"{row.get('FULLADDRESS', 'N/A')}<br>Price: ${row['PRICE']}",
                    tooltip=row.get('FULLADDRESS', 'Property')
                ).add_to(map_)
            folium_static(map_)
    else:
        st.write("LATITUDE and LONGITUDE columns not found for map visualization.")
    
    ###############################################
    # DAILY PROSPECTING CALLS (20 Calls per Agent)
    ###############################################
    st.subheader("Daily Prospecting Calls")
    
    # Filter to only properties with OWNER_TYPE = "IND"
    if "OWNER_TYPE" in filtered_df.columns:
        ind_df = filtered_df[filtered_df["OWNER_TYPE"] == "IND"]
    else:
        st.warning("No 'OWNER_TYPE' column found; skipping call generation.")
        return
    
    # Button to generate 20 calls for the selected agent
    if st.button("Generate 20 Calls"):
        if ind_df.empty:
            st.warning("No properties with OWNER_TYPE = 'IND' found in the filtered dataset.")
        else:
            # Sample up to 20 calls from the IND owners
            calls_to_make = ind_df.sample(n=min(20, len(ind_df)), random_state=42).copy()
            
            # Assign the selected agent's details to each call
            calls_to_make["AssignedAgentID"] = selected_agent["ID"]
            calls_to_make["AssignedAgentName"] = selected_agent["NAME"]
            calls_to_make["AssignedAgentEmail"] = selected_agent["EMAIL"]
            calls_to_make["AssignedAgentPhone"] = selected_agent["PHONE"]
            
            # Store the calls in session state for persistence
            st.session_state["calls_for_today"] = calls_to_make
            st.success("Successfully generated 20 calls for the selected agent!")
    
    # Display calls if generated
    if "calls_for_today" in st.session_state:
        calls_for_today = st.session_state["calls_for_today"]
        st.write("Here are your 20 calls for today:")
        
        # Columns to display: including OWNER1FN and OWNER1LN
        columns_to_show = [
            "OWNER1FN",
            "OWNER1LN",
            "FULLADDRESS",
            "PRICE",
            "OWNER_TYPE",
            "AssignedAgentID",
            "AssignedAgentName",
            "AssignedAgentEmail",
            "AssignedAgentPhone"
        ]
        existing_cols = [col for col in columns_to_show if col in calls_for_today.columns]
        st.dataframe(calls_for_today[existing_cols])
        
        # Updated call outcomes: each call is shown in two columns:
        # left (0.33 width) for the status selectbox, and right (0.67 width) for a script/notes box.
        st.write("Update call outcomes:")
        for idx, row in calls_for_today.iterrows():
            col_status, col_script = st.columns([0.33, 0.67])
            with col_status:
                call_id = f"call_status_{idx}"
                new_status = st.selectbox(
                    "Status",
                    ["Pending", "Called", "Voicemail", "Not Interested", "Interested"],
                    key=call_id
                )
                calls_for_today.loc[idx, "CallStatus"] = new_status
            with col_script:
                script_key = f"script_{idx}"
                script_text = st.text_area("Script/Notes", key=script_key)
                calls_for_today.loc[idx, "CallScript"] = script_text
        
        st.session_state["calls_for_today"] = calls_for_today
        
        # Display aggregated statistics on call outcomes
        st.subheader("Call Outcome Statistics")
        if "CallStatus" in calls_for_today.columns:
            outcome_counts = calls_for_today["CallStatus"].value_counts()
            for outcome, count in outcome_counts.items():
                st.write(f"{outcome}: {count}")
        
        # Download button to export call list as CSV
        st.download_button(
            label="Download Call List as CSV",
            data=calls_for_today.to_csv(index=False),
            file_name="calls_for_today.csv",
            mime="text/csv"
        )

###############################################
# 3. RUN APP
###############################################
if __name__ == "__main__":
    main()
