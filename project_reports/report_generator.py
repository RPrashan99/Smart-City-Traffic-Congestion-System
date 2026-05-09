import pandas as pd
import matplotlib.pyplot as plt
import glob

from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
    Image
)

from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch

#Find all CSV files in the current directory
peak_files = glob.glob("data/*peak_hour_report*.csv")
traffic_files = glob.glob("data/*traffic_volume_vs_time*.csv")

#check if the files exist
if not peak_files:
    raise FileNotFoundError("No peak_hour_report CSV file found")

if not traffic_files:
    raise FileNotFoundError("No traffic_volume_vs_time CSV file found")

# LOAD CSV FILES
peak_df = pd.read_csv(peak_files[0])
traffic_df = pd.read_csv(traffic_files[0])

# CREATE CHARTS
# Traffic Volume Chart
plt.figure(figsize=(6, 4))

plt.bar(
    traffic_df["sensor_id"],
    traffic_df["total_volume"]
)

plt.xlabel("Sensor ID")
plt.ylabel("Traffic Volume")
plt.title("Traffic Volume by Junction")

volume_chart_path = "traffic_volume_chart.png"

plt.savefig(
    volume_chart_path,
    bbox_inches="tight"
)

plt.close()

# Average Speed Chart
plt.figure(figsize=(6, 4))

plt.plot(
    traffic_df["sensor_id"],
    traffic_df["avg_speed"],
    marker="o"
)

plt.xlabel("Sensor ID")
plt.ylabel("Average Speed (km/h)")
plt.title("Average Speed by Junction")

speed_chart_path = "avg_speed_chart.png"

plt.savefig(
    speed_chart_path,
    bbox_inches="tight"
)

plt.close()

# CREATE PDF REPORT
pdf_path = "traffic_analytics_report.pdf"

doc = SimpleDocTemplate(
    pdf_path,
    pagesize=letter
)

styles = getSampleStyleSheet()

elements = []

# TITLE
title = Paragraph(
    "Traffic Analytics Report",
    styles["Title"]
)

elements.append(title)
elements.append(Spacer(1, 0.2 * inch))

# PEAK HOUR TABLE
elements.append(
    Paragraph(
        "Peak Hour Report",
        styles["Heading2"]
    )
)

peak_table_data = [
    peak_df.columns.tolist()
] + peak_df.values.tolist()

peak_table = Table(peak_table_data)

peak_table.setStyle(TableStyle([
    ("BACKGROUND", (0, 0), (-1, 0), colors.grey),
    ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
    ("GRID", (0, 0), (-1, -1), 1, colors.black),
    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
    ("BACKGROUND", (0, 1), (-1, -1), colors.beige),
]))

elements.append(peak_table)
elements.append(Spacer(1, 0.3 * inch))

# TRAFFIC VOLUME TABLE
elements.append(
    Paragraph(
        "Traffic Volume vs Time of Day",
        styles["Heading2"]
    )
)

traffic_table_data = [
    traffic_df.columns.tolist()
] + traffic_df.values.tolist()

traffic_table = Table(traffic_table_data)

traffic_table.setStyle(TableStyle([
    ("BACKGROUND", (0, 0), (-1, 0), colors.grey),
    ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
    ("GRID", (0, 0), (-1, -1), 1, colors.black),
    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
    ("BACKGROUND", (0, 1), (-1, -1), colors.lightgrey),
]))

elements.append(traffic_table)
elements.append(Spacer(1, 0.3 * inch))

# ADD CHARTS
elements.append(
    Paragraph(
        "Traffic Volume by Junction",
        styles["Heading2"]
    )
)

elements.append(
    Image(
        volume_chart_path,
        width=5.5 * inch,
        height=3.5 * inch
    )
)

elements.append(Spacer(1, 0.2 * inch))

elements.append(
    Paragraph(
        "Average Speed by Junction",
        styles["Heading2"]
    )
)

elements.append(
    Image(
        speed_chart_path,
        width=5.5 * inch,
        height=3.5 * inch
    )
)

# BUILD PDF
doc.build(elements)

print("PDF report generated successfully!")