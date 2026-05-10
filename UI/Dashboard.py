import os
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import requests
from datetime import datetime, timedelta
import pytz
import json
from streamlit_option_menu import option_menu
import asyncio
from threading import Thread
import time
import glob

# data directories
report_path = "../project_reports/data/"
traffic_data = "../project_reports/shared/traffic_agg_5min"

# Page configuration
st.set_page_config(
    page_title="Smart City Traffic Analytics - Colombo",
    page_icon="🚦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better UI
st.markdown("""
<style>
    /* Main container styling */
    .main {
        padding: 0rem 1rem;
    }
    
    /* Metric cards styling */
    .metric-card {
        background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    
    /* Alert cards */
    .alert-critical {
        background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
        padding: 1rem;
        border-radius: 10px;
        margin: 0.5rem 0;
        color: white;
    }
    
    .alert-warning {
        background: linear-gradient(135deg, #fa709a 0%, #fee140 100%);
        padding: 1rem;
        border-radius: 10px;
        margin: 0.5rem 0;
    }
    
    /* Junction status cards */
    .junction-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 10px;
        margin: 0.5rem 0;
        color: white;
        text-align: center;
    }
    
    /* Custom header */
    .custom-header {
        background: linear-gradient(90deg, #0f2027, #203a43, #2c5364);
        padding: 2rem;
        border-radius: 10px;
        color: white;
        margin-bottom: 2rem;
        text-align: center;
    }
    
    /* Progress indicators */
    .congestion-bar {
        height: 10px;
        border-radius: 5px;
        background: linear-gradient(90deg, #00c853, #ffd600, #ff3d00);
        margin: 0.5rem 0;
    }
    
    /* Status indicators */
    .status-indicator {
        width: 10px;
        height: 10px;
        border-radius: 50%;
        display: inline-block;
        margin-right: 5px;
    }
    
    .status-critical {
        background-color: #ff3d00;
        box-shadow: 0 0 5px #ff3d00;
    }
    
    .status-warning {
        background-color: #ffd600;
        box-shadow: 0 0 5px #ffd600;
    }
    
    .status-normal {
        background-color: #00c853;
        box-shadow: 0 0 5px #00c853;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'alerts' not in st.session_state:
    st.session_state.alerts = []
if 'last_update' not in st.session_state:
    st.session_state.last_update = datetime.now()
if 'traffic_data' not in st.session_state:
    st.session_state.traffic_data = {}

# Sample data generator
def generate_sample_data():
    """Generate sample traffic data for demonstration"""
    junctions = ['J1', 'J2', 'J3', 'J4']
    data = []

    fetch_traffic_data()
    
    for junction in junctions:
        # Generate realistic traffic patterns based on time of day
        current_hour = datetime.now().hour
        
        if 7 <= current_hour <= 9:  # Morning peak
            vehicles = np.random.randint(180, 250)
            speed = np.random.uniform(10, 25)
        elif 17 <= current_hour <= 19:  # Evening peak
            vehicles = np.random.randint(200, 280)
            speed = np.random.uniform(8, 20)
        else:
            vehicles = np.random.randint(30, 120)
            speed = np.random.uniform(25, 60)
        
        congestion_index = vehicles / max(speed, 1)
        status = "Critical" if speed < 15 else "Warning" if speed < 25 else "Normal"
        
        data.append({
            'sensor_id': junction,
            'vehicle_count': vehicles,
            'avg_speed': round(speed, 1),
            'congestion_index': round(congestion_index, 2),
            'status': status,
            'timestamp': datetime.now().isoformat()
        })
    
    return data

def fetch_traffic_data():
    """Fetching traffic data from HDFS"""
    
    files = glob.glob(
        "/opt/shared/traffic_agg_5min/*.parquet"
    )

    if not files:
        return pd.DataFrame()
    
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    
    print(f"Found traffic data files: {df.head()}")

    return df

def fetch_report_data(report_name):
    """Simulate fetching report data from HDFS or Spark"""
    if report_name == "peak_hour_report":
        return get_peak_hour_report()
    elif report_name == "traffic_volume_by_hour":
        return get_traffic_volume_by_hour()
    else:
        return pd.DataFrame()

# Function to fetch peak hour report
def get_peak_hour_report(date=None):
    """
    Fetch peak hour analysis report from locally stored CSV files
    generated by Spark/Airflow.
    """

    # Default to yesterday
    if date is None:
        date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    #file_date = date.replace("-", "_")

    files = glob.glob(report_path + f"*peak_hour_report_*{date}*.csv")

    if not files:
        st.warning(f"No peak hour report found for {date}")
        return pd.DataFrame()

    latest_file = max(files)

    peak_data = pd.read_csv(latest_file)

    return peak_data

# Function to fetch traffic volume by hour
def get_traffic_volume_by_hour():
    """Fetch traffic volume data for 24 hours"""

    date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    files = glob.glob(report_path + f"*traffic_volume_vs_time_*{date}*.csv")

    if not files:
        st.warning(f"No traffic volume report found for {date}")
        return pd.DataFrame()

    latest_file = max(files)

    traffic_volume_data = pd.read_csv(latest_file)
    
    # for hour in hours:
    #     for junction in ['J1', 'J2', 'J3', 'J4']:
    #         if hour in [8, 9, 17, 18]:
    #             volume = np.random.randint(150, 250)
    #             speed = np.random.uniform(10, 20)
    #         else:
    #             volume = np.random.randint(30, 100)
    #             speed = np.random.uniform(25, 50)
            
    #         data.append({
    #             'hour_of_day': hour,
    #             'sensor_id': junction,
    #             'total_volume': volume,
    #             'avg_speed': round(speed, 1)
    #         })
    
    # return pd.DataFrame(data)

    return traffic_volume_data

# Sidebar navigation
with st.sidebar:
    st.image("https://img.icons8.com/color/96/000000/traffic-jam.png", width=80)
    st.title("🚦 Traffic Analytics")
    st.markdown("---")
    
    selected = option_menu(
        menu_title="Navigation",
        options=["Dashboard", "Junction Analysis", "Real-time Map", "Reports", "Alerts", "System Health"],
        icons=["house", "graph-up", "map", "file-text", "bell", "heartbeat"],
        menu_icon="cast",
        default_index=0,
        styles={
            "container": {"padding": "0!important", "background-color": "#141414CF"},
            "icon": {"color": "#ff5722", "font-size": "20px"},
            "nav-link": {"font-size": "16px", "text-align": "left", "margin": "0px"},
            "nav-link-selected": {"background-color": "#ff5722"},
        }
    )
    
    st.markdown("---")
    st.markdown("### System Status")
    st.markdown("🟢 **Kafka**: Connected")
    st.markdown("🟢 **Spark**: Active")
    st.markdown("🟢 **HDFS**: Available")
    st.markdown("🟢 **Airflow**: Running")
    
    st.markdown("---")
    st.markdown("### Last Update")
    last_update_placeholder = st.empty()
    last_update_placeholder.info(f"{st.session_state.last_update.strftime('%Y-%m-%d %H:%M:%S')}")

# Header
st.markdown("""
<div class="custom-header">
    <h1>🚦 Smart City Traffic Intelligence Platform</h1>
    <p>Colombo Metropolitan Area | Real-time Analytics & Peak Hour Detection</p>
</div>
""", unsafe_allow_html=True)

# Main content based on navigation
if selected == "Dashboard":
    st.markdown("## 📊 Live Traffic Dashboard")
    
    # Auto-refresh data every 10 seconds
    if st.button("🔄 Refresh Data", key="refresh_btn"):
        st.session_state.traffic_data = fetch_traffic_data()
        st.session_state.last_update = datetime.now()
        st.rerun()
    
    # Load current traffic data
    if not st.session_state.traffic_data:
        st.session_state.traffic_data = fetch_traffic_data()
    
    current_data = pd.DataFrame(st.session_state.traffic_data)
    
    # KPI Row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_vehicles = current_data['total_vehicles'].sum()
        st.metric("🚗 Total Vehicles (Last Hour)", f"{total_vehicles:,}", delta="+12%")
    
    with col2:
        avg_speed = current_data['avg_speed_window'].mean()
        st.metric("⚡ Average Speed", f"{avg_speed:.1f} km/h", delta="-5%", delta_color="inverse")
    
    with col3:
        critical_count = len(current_data[current_data['status'] == 'Critical'])
        st.metric("⚠️ Critical Junctions", critical_count, delta="+2", delta_color="inverse")
    
    with col4:
        congestion_level = current_data['congestion_index'].mean()
        st.metric("📈 Congestion Index", f"{congestion_level:.1f}", delta="+8%", delta_color="inverse")
    
    st.markdown("---")
    
    # Charts Row
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Current Junction Status")
        
        # Create gauge charts for each junction
        for _, row in current_data.iterrows():
            status_color = "#ff3d00" if row['status'] == 'Critical' else "#ffd600" if row['status'] == 'Warning' else "#00c853"
            
            fig = go.Figure(go.Indicator(
                mode="gauge+number+delta",
                value=row['congestion_index'],
                title={'text': f"{row['sensor_id']} - {row['status']}"},
                delta={'reference': 30},
                gauge={
                    'axis': {'range': [None, 100]},
                    'bar': {'color': status_color},
                    'steps': [
                        {'range': [0, 30], 'color': "#00c853"},
                        {'range': [30, 60], 'color': "#ffd600"},
                        {'range': [60, 100], 'color': "#ff3d00"}
                    ],
                    'threshold': {
                        'line': {'color': "red", 'width': 4},
                        'thickness': 0.75,
                        'value': 60
                    }
                }
            ))
            fig.update_layout(height=250, margin=dict(l=20, r=20, t=50, b=20))
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Traffic Metrics Comparison")
        
        # Bar chart for vehicle count and speed
        fig = make_subplots(rows=2, cols=1, subplot_titles=("Vehicle Count by Junction", "Average Speed by Junction"))
        
        fig.add_trace(
            go.Bar(x=current_data['sensor_id'], y=current_data['total_vehicles'], 
                   name="Vehicles", marker_color='#ff5722'),
            row=1, col=1
        )
        
        fig.add_trace(
            go.Bar(x=current_data['sensor_id'], y=current_data['avg_speed_window'], 
                   name="Speed (km/h)", marker_color='#2196f3'),
            row=2, col=1
        )
        
        fig.update_layout(height=600, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
    
    # Alerts Section
    st.markdown("---")
    st.subheader("🚨 Active Alerts")
    
    alerts_df = current_data[current_data['status'] == 'Critical']
    
    if len(alerts_df) > 0:
        for _, alert in alerts_df.iterrows():
            st.markdown(f"""
            <div class="alert-critical">
                <strong>⚠️ CRITICAL ALERT - {alert['sensor_id']}</strong><br>
                Speed: {alert['avg_speed']} km/h | Vehicles: {alert['vehicle_count']} | Congestion Index: {alert['congestion_index']}<br>
                <small>Recommendation: Immediate police deployment required at peak hour</small>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.success("✅ No active critical alerts at this time")
    
    # Hourly forecast
    st.markdown("---")
    st.subheader("📈 Hourly Traffic Forecast")
    
    hourly_data = get_traffic_volume_by_hour()
    hourly_agg = hourly_data.groupby('hour_of_day')['total_volume'].sum().reset_index()
    
    fig = px.line(hourly_agg, x='hour_of_day', y='total_volume', 
                  title="Total Traffic Volume by Hour (24h Forecast)",
                  labels={'hour_of_day': 'Hour', 'total_volume': 'Number of Vehicles'})
    fig.update_traces(line_color='#ff5722', line_width=3)
    fig.add_hrect(y0=200, y1=300, line_width=0, fillcolor="red", opacity=0.2, 
                  annotation_text="Peak Hours", annotation_position="top left")
    st.plotly_chart(fig, use_container_width=True)

elif selected == "Junction Analysis":
    st.markdown("## 🔍 Junction Performance Analysis")
    
    # Date selector
    col1, col2 = st.columns([2, 1])
    with col1:
        analysis_date = st.date_input("Select Analysis Date", datetime.now() - timedelta(days=1))
    with col2:
        junction_filter = st.multiselect("Select Junctions", ['J1', 'J2', 'J3', 'J4'], default=['J1', 'J2', 'J3', 'J4'])
    
    # Get peak hour report
    peak_data = get_peak_hour_report(str(analysis_date))
    
    # Filter data
    filtered_peak = peak_data[peak_data['sensor_id'].isin(junction_filter)]
    
    # Display peak hour analysis
    st.subheader("📊 Peak Hour Analysis")
    
    # Metric cards for each junction
    cols = st.columns(len(filtered_peak))
    for idx, (_, row) in enumerate(filtered_peak.iterrows()):
        with cols[idx]:
            color = "#ff3d00" if row['needs_intervention'] == 'YES' else "#00c853"
            st.markdown(f"""
            <div class="junction-card" style="background: {color}">
                <h3>{row['sensor_id']}</h3>
                <p>Peak Hour: <strong>{row['peak_hour']}</strong></p>
                <p>Vehicles: <strong>{row['vehicles_hour']}</strong></p>
                <p>Speed: <strong>{row['avg_speed_at_peak']} km/h</strong></p>
                <p>Intervention: <strong>{row['needs_intervention']}</strong></p>
            </div>
            """, unsafe_allow_html=True)
    
    # Detailed table
    st.subheader("Detailed Metrics")
    st.dataframe(
        filtered_peak.style.background_gradient(subset=['vehicles_hour'], cmap='RdYlGn'),
        use_container_width=True
    )
    
    # Hourly traffic patterns
    st.subheader("Hourly Traffic Patterns")
    volume_data = get_traffic_volume_by_hour()
    filtered_volume = volume_data[volume_data['sensor_id'].isin(junction_filter)]
    
    # Create heatmap
    pivot_data = filtered_volume.pivot(index='hour_of_day', columns='sensor_id', values='total_volume')
    
    fig = px.imshow(pivot_data.T, 
                    labels=dict(x="Hour of Day", y="Junction", color="Vehicle Count"),
                    title="Traffic Volume Heatmap",
                    color_continuous_scale="RdYlGn_r")
    fig.update_layout(height=500)
    st.plotly_chart(fig, use_container_width=True)
    
    # Recommendations
    st.subheader("💡 Recommendations")
    critical_junctions = filtered_peak[filtered_peak['needs_intervention'] == 'YES']
    
    for _, junction in critical_junctions.iterrows():
        st.warning(f"""
        **{junction['sensor_id']} - Immediate Action Required**
        - Peak hour at {junction['peak_hour']} with {junction['vehicles_hour']} vehicles
        - Average speed critically low at {junction['avg_speed_at_peak']} km/h
        - 🚔 Deploy 2-3 traffic officers during peak hours
        - 📍 Consider adjusting traffic signal timings
        """)

elif selected == "Real-time Map":
    st.markdown("## 🗺️ Real-time Traffic Map")
    
    # Sample coordinates for junctions
    junction_locations = {
        'J1': {'lat': 6.9271, 'lon': 79.8612, 'name': 'Colombo City Center'},
        'J2': {'lat': 6.9145, 'lon': 79.8567, 'name': 'Galle Face Junction'},
        'J3': {'lat': 6.9021, 'lon': 79.8625, 'name': 'Kollupitiya Junction'},
        'J4': {'lat': 6.8916, 'lon': 79.8579, 'name': 'Bambalapitiya Junction'}
    }
    
    # Get current data
    current_data = pd.DataFrame(st.session_state.traffic_data)
    
    # Create map data
    map_data = []
    for _, row in current_data.iterrows():
        loc = junction_locations.get(row['sensor_id'])
        if loc:
            map_data.append({
                'lat': loc['lat'],
                'lon': loc['lon'],
                'name': loc['name'],
                'vehicles': row['vehicle_count'],
                'speed': row['avg_speed'],
                'status': row['status'],
                'size': row['congestion_index'] * 10
            })
    
    map_df = pd.DataFrame(map_data)
    
    # Create interactive map with plotly
    fig = px.scatter_mapbox(map_df, 
                            lat="lat", 
                            lon="lon", 
                            size="size",
                            color="status",
                            hover_name="name",
                            hover_data=["vehicles", "speed"],
                            color_discrete_map={"Critical": "red", "Warning": "orange", "Normal": "green"},
                            zoom=12,
                            height=600,
                            title="Live Junction Status Map")
    
    fig.update_layout(mapbox_style="open-street-map")
    fig.update_layout(margin={"r":0,"t":30,"l":0,"b":0})
    st.plotly_chart(fig, use_container_width=True)
    
    # Junction details
    st.subheader("Junction Details")
    cols = st.columns(4)
    for idx, (_, row) in enumerate(map_df.iterrows()):
        with cols[idx]:
            status_icon = "🔴" if row['status'] == 'Critical' else "🟡" if row['status'] == 'Warning' else "🟢"
            st.info(f"""
            **{status_icon} {row['name']}** ({row['name'].split()[-2] if len(row['name'].split()) > 1 else row['name']})
            - 🚗 Vehicles: {row['vehicles']}
            - ⚡ Speed: {row['speed']} km/h
            - 📊 Status: {row['status']}
            """)

elif selected == "Reports":
    st.markdown("## 📄 Traffic Analytics Reports")
    
    report_type = st.selectbox("Select Report Type", 
                               ["Peak Hour Analysis", "Traffic Volume Report", "Congestion Trends", "Police Intervention Report"])
    
    if report_type == "Peak Hour Analysis":
        st.subheader("Daily Peak Hour Analysis Report")
        
        start_date = st.date_input("Start Date", datetime.now() - timedelta(days=7))
        end_date = st.date_input("End Date", datetime.now())
        
        # Generate weekly peak data
        weekly_data = []
        for i in range(7):
            date = start_date + timedelta(days=i)
            if date <= end_date:
                peak = get_peak_hour_report(str(date))
                peak['date'] = date
                weekly_data.append(peak)
        
        if weekly_data:
            weekly_df = pd.concat(weekly_data, ignore_index=True)
            
            # Display trends
            fig = px.line(weekly_df, x='date', y='vehicles_hour', color='sensor_id',
                          title="Weekly Peak Hour Vehicle Trends",
                          labels={'vehicles_hour': 'Vehicles at Peak', 'date': 'Date'})
            st.plotly_chart(fig, use_container_width=True)
            
            # Export option
            csv = weekly_df.to_csv(index=False)
            st.download_button(
                label="📥 Download Report (CSV)",
                data=csv,
                file_name=f"peak_hour_report_{start_date}_to_{end_date}.csv",
                mime="text/csv"
            )
    
    elif report_type == "Traffic Volume Report":
        st.subheader("Traffic Volume Analysis")
        
        volume_data = get_traffic_volume_by_hour()
        
        # Pivot table
        pivot_table = volume_data.pivot_table(index='hour_of_day', columns='sensor_id', values='total_volume', aggfunc='sum')
        st.dataframe(pivot_table, use_container_width=True)
        
        # Visualization
        fig = px.area(volume_data, x='hour_of_day', y='total_volume', color='sensor_id',
                      title="Traffic Volume Distribution by Hour",
                      labels={'total_volume': 'Vehicle Count', 'hour_of_day': 'Hour'})
        st.plotly_chart(fig, use_container_width=True)

elif selected == "Alerts":
    st.markdown("## 🔔 Alert Management Console")
    
    # Alert filters
    col1, col2, col3 = st.columns(3)
    with col1:
        severity_filter = st.selectbox("Severity", ["All", "Critical", "Warning", "Info"])
    with col2:
        junction_filter = st.multiselect("Junction", ['J1', 'J2', 'J3', 'J4'], default=['J1', 'J2', 'J3', 'J4'])
    with col3:
        time_filter = st.selectbox("Time Range", ["Last Hour", "Last 24 Hours", "Last Week", "All Time"])
    
    # Generate sample alerts
    alerts_data = []
    current_data = pd.DataFrame(st.session_state.traffic_data)
    
    for _, row in current_data.iterrows():
        if row['status'] == 'Critical':
            alerts_data.append({
                'timestamp': datetime.now(),
                'junction': row['sensor_id'],
                'severity': 'Critical',
                'message': f"Speed critically low at {row['avg_speed']} km/h",
                'vehicles': row['vehicle_count'],
                'speed': row['avg_speed'],
                'recommendation': "Immediate police deployment required"
            })
        elif row['status'] == 'Warning':
            alerts_data.append({
                'timestamp': datetime.now(),
                'junction': row['sensor_id'],
                'severity': 'Warning',
                'message': f"Moderate congestion detected",
                'vehicles': row['vehicle_count'],
                'speed': row['avg_speed'],
                'recommendation': "Monitor closely"
            })
    
    alerts_df = pd.DataFrame(alerts_data)
    
    # Display alerts
    if len(alerts_df) > 0:
        for _, alert in alerts_df.iterrows():
            if severity_filter == "All" or alert['severity'] == severity_filter:
                if alert['junction'] in junction_filter:
                    if alert['severity'] == 'Critical':
                        st.error(f"""
                        **🚨 CRITICAL ALERT - {alert['junction']}** at {alert['timestamp'].strftime('%H:%M:%S')}
                        - {alert['message']}
                        - 🚗 Vehicles: {alert['vehicles']}
                        - ⚡ Speed: {alert['speed']} km/h
                        - 💡 Recommendation: {alert['recommendation']}
                        """)
                    else:
                        st.warning(f"""
                        **⚠️ WARNING - {alert['junction']}** at {alert['timestamp'].strftime('%H:%M:%S')}
                        - {alert['message']}
                        - 🚗 Vehicles: {alert['vehicles']}
                        - ⚡ Speed: {alert['speed']} km/h
                        """)
    else:
        st.success("✅ No alerts at this time")
    
    # Acknowledge all button
    if st.button("Acknowledge All Alerts", type="primary"):
        st.success("All alerts acknowledged")
        st.rerun()

elif selected == "System Health":
    st.markdown("## 🏥 System Health Dashboard")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Pipeline Status")
        st.markdown("""
        - **Kafka Producer**: Running
        - **Spark Streaming**: Active
        - **HDFS Storage**: Available
        - **Airflow DAG**: Scheduled
        """)
        
        st.subheader("Recent DAG Executions")
        dag_data = pd.DataFrame({
            'DAG ID': ['traffic_daily_peak_hour', 'traffic_streaming', 'data_quality_check'],
            'Last Run': ['2026-02-01 01:00:00', '2026-02-01 00:05:00', '2026-02-01 02:00:00'],
            'Status': ['Success', 'Running', 'Success'],
            'Duration': ['2m 34s', '45s', '1m 12s']
        })
        st.dataframe(dag_data, use_container_width=True)
    
    with col2:
        st.subheader("Resource Utilization")
        
        # CPU Usage Gauge
        fig = go.Figure(go.Indicator(
            mode="gauge+number",
            value=68,
            title={'text': "CPU Usage"},
            gauge={'axis': {'range': [None, 100]},
                   'bar': {'color': "#ff5722"},
                   'steps': [
                       {'range': [0, 50], 'color': "#00c853"},
                       {'range': [50, 80], 'color': "#ffd600"},
                       {'range': [80, 100], 'color': "#ff3d00"}]}
        ))
        fig.update_layout(height=250)
        st.plotly_chart(fig, use_container_width=True)
        
        # Memory Usage
        fig = go.Figure(go.Indicator(
            mode="gauge+number",
            value=54,
            title={'text': "Memory Usage"},
            gauge={'axis': {'range': [None, 100]},
                   'bar': {'color': "#2196f3"}}
        ))
        fig.update_layout(height=250)
        st.plotly_chart(fig, use_container_width=True)
    
    st.subheader("Data Pipeline Throughput")
    
    # Throughput chart
    throughput_data = pd.DataFrame({
        'hour': list(range(24)),
        'messages_processed': np.random.randint(100, 500, 24),
        'avg_latency_ms': np.random.randint(50, 200, 24)
    })
    
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Bar(x=throughput_data['hour'], y=throughput_data['messages_processed'], 
                         name="Messages Processed", marker_color='#ff5722'), secondary_y=False)
    fig.add_trace(go.Scatter(x=throughput_data['hour'], y=throughput_data['avg_latency_ms'], 
                             name="Avg Latency (ms)", line=dict(color='#2196f3', width=3)), secondary_y=True)
    fig.update_layout(title="Stream Processing Metrics", xaxis_title="Hour", height=400)
    st.plotly_chart(fig, use_container_width=True)

# Auto-refresh mechanism
def auto_refresh():
    """Auto-refresh data every 30 seconds"""
    while True:
        time.sleep(30)
        st.session_state.traffic_data = generate_sample_data()
        st.session_state.last_update = datetime.now()
        st.rerun()

# Start auto-refresh thread
if 'refresh_thread_started' not in st.session_state:
    refresh_thread = Thread(target=auto_refresh, daemon=True)
    refresh_thread.start()
    st.session_state.refresh_thread_started = True

# Footer
st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #666; padding: 1rem;">
    <p>🚦 Smart City Traffic Analytics Platform | Powered by Apache Spark, Kafka, Airflow</p>
    <p>Data refreshes every 30 seconds | Last updated: {}</p>
</div>
""".format(st.session_state.last_update.strftime('%Y-%m-%d %H:%M:%S')), unsafe_allow_html=True)