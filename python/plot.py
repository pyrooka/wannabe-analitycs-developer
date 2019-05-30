#!/usr/bin/env python3

import os
from datetime import datetime

import plotly
import plotly.graph_objs as go
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import transfer


def main():
    # Connect to the DB.
    db_url = os.getenv("TARGET_DB_URL", default="postgresql://liligo:liligo@localhost:5432/liligo")
    db_engine = create_engine(db_url, echo=False)
    Session = sessionmaker(bind=db_engine)
    session = Session()

    # Get all the data from the DB.
    try:
        res = session.query(transfer.Events).order_by(transfer.Events.day_last).all()
    except Exception as e:
        print("Cannot get data from the DB.", e)
        return

    if not res:
        print("No data.")
        return

    # Prepare data for plotting.
    by_metric = {}
    for row in res:
        metric_name = row.metric.name
        if metric_name not in by_metric:
            by_metric[metric_name] = { "x": [], "y": [] }

        by_metric[metric_name]["x"].append(datetime.date(row.day_last))
        by_metric[metric_name]["y"].append(row.count)

    # Create data for each chart.
    data_line, data_bar, data_sum = [], [], {}
    for key, value in by_metric.items():
        # Line chart.
        data_line.append(
            go.Scatter(
                x = value["x"],
                y = value["y"],
                mode = "lines",
                name = key))

        # Bar chart.
        data_bar.append(
            go.Bar(
                x = value["x"],
                y = value["y"],
                name = key))

        # Overall bar chart.
        data_sum[key] = sum(value["y"])

    # Create the subplots.
    plots = plotly.tools.make_subplots(rows=2, cols=2, specs=[[{}, {}], [{"colspan": 2}, None]],
                                       subplot_titles=("Metrics count","Metrics count per day (1)", "Metrics count per day (2)"))
    plots["layout"].update(title="Web analytics report")

    plots.append_trace(
        go.Bar(
            x=list(data_sum.keys()),
            y=list(data_sum.values()),
            showlegend=False),
            1, 1)

    for db in data_bar:
        plots.append_trace(db, 1, 2)

    for dl in data_line:
        plots.append_trace(dl, 2, 1)

    # Then save it.
    plotly.offline.plot(plots, filename="report.html")

    session.close()


if __name__ == "__main__":
    main()
