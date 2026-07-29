#!/usr/bin/env python3
"""Creates a Kibana dashboard that puts the derived metrics next to the source stream.

Every row asks the same question twice: the left panel answers it from the derived metrics, the right
panel answers it from the raw data stream. The numbers should match; the cost of getting them should
not.

The panels are ES|QL Lens visualisations embedded by value, so the dashboard is self-contained apart
from the two data views it references.
"""

import argparse
import base64
import json
import urllib.error
import urllib.request

DASHBOARD_ID = "derived-metrics-demo-dashboard"
LAYER = "layer_0"
# Accessor ids are arbitrary; they only have to match between the columns and the visualisation.
METRIC_COL = "metric_col"
X_COL = "x_col"
Y_COL = "y_col"


def esql_layer(data_view_id, query, columns):
    return {
        "index": data_view_id,
        "query": {"esql": query},
        # Lens applies the dashboard time range to this field.
        "timeField": "@timestamp",
        "columns": columns,
        "allColumns": columns,
    }


def metric_panel(title, data_view_id, query, value_field, subtitle):
    columns = [{"columnId": METRIC_COL, "fieldName": value_field, "meta": {"type": "number"}}]
    return {
        "title": title,
        "visualizationType": "lnsMetric",
        "references": [
            {"type": "index-pattern", "id": data_view_id, "name": f"indexpattern-datasource-layer-{LAYER}"}
        ],
        "state": {
            "datasourceStates": {"textBased": {"layers": {LAYER: esql_layer(data_view_id, query, columns)}}},
            "internalReferences": [],
            "filters": [],
            "query": {"language": "kuery", "query": ""},
            "visualization": {
                "layerId": LAYER,
                "layerType": "data",
                "metricAccessor": METRIC_COL,
                "subtitle": subtitle,
                "showBar": False,
            },
            "adHocDataViews": {},
        },
    }


def line_panel(title, data_view_id, query, x_field, y_field, colour):
    columns = [
        {"columnId": X_COL, "fieldName": x_field, "meta": {"type": "date"}},
        {"columnId": Y_COL, "fieldName": y_field, "meta": {"type": "number"}},
    ]
    return {
        "title": title,
        "visualizationType": "lnsXY",
        "references": [
            {"type": "index-pattern", "id": data_view_id, "name": f"indexpattern-datasource-layer-{LAYER}"}
        ],
        "state": {
            "datasourceStates": {"textBased": {"layers": {LAYER: esql_layer(data_view_id, query, columns)}}},
            "internalReferences": [],
            "filters": [],
            "query": {"language": "kuery", "query": ""},
            "visualization": {
                "legend": {"isVisible": False, "position": "right"},
                "valueLabels": "hide",
                "fittingFunction": "Linear",
                "emphasizeFitting": True,
                "hideEndzones": True,
                "preferredSeriesType": "line",
                "axisTitlesVisibilitySettings": {"x": False, "yLeft": False, "yRight": False},
                "tickLabelsVisibilitySettings": {"x": True, "yLeft": True, "yRight": True},
                "gridlinesVisibilitySettings": {"x": True, "yLeft": True, "yRight": True},
                "labelsOrientation": {"x": 0, "yLeft": 0, "yRight": 0},
                "yLeftExtent": {"mode": "full"},
                "layers": [
                    {
                        "layerId": LAYER,
                        "layerType": "data",
                        "seriesType": "line",
                        "xAccessor": X_COL,
                        "accessors": [Y_COL],
                        "yConfig": [{"forAccessor": Y_COL, "color": colour}],
                    }
                ],
            },
            "adHocDataViews": {},
        },
    }


# Derived is blue, source is grey, consistently down the dashboard.
DERIVED_COLOUR = "#16C5C0"
SOURCE_COLOUR = "#9AA1AA"


# Simple value panels need far less room than a chart.
METRIC_HEIGHT = 5
CHART_HEIGHT = 13


def row(height, left, right=None):
    """A dashboard row. right=None means the metric has no source-side equivalent, so it spans full width."""
    return (height, left, right)


def build_rows(derived_dv, source_dv, derived, source):
    """Left is always derived, right always source, so every row reads as one question asked twice."""
    b = "BUCKET(@timestamp, 10 second)"

    def d_line(title, where, expr, field, extra=""):
        return line_panel(
            f"{title}  ·  DERIVED", derived_dv,
            f'FROM {derived} | WHERE metric.name == "{where}" | STATS {expr} BY bucket = {b}{extra}',
            "bucket", field, DERIVED_COLOUR)

    def s_line(title, expr, field, where=""):
        clause = f"| WHERE {where} " if where else ""
        return line_panel(
            f"{title}  ·  SOURCE", source_dv,
            f"FROM {source} {clause}| STATS {expr} BY bucket = {b}",
            "bucket", field, SOURCE_COLOUR)

    return [
        # --- the headline: the same count, and what it cost to be able to answer it ---
        row(METRIC_HEIGHT,
            metric_panel("Documents observed  ·  DERIVED", derived_dv,
                f'FROM {derived} | WHERE metric.name == "ingest.docs.count" | STATS docs = SUM(metric.value)',
                "docs", "summed from ingest.docs.count"),
            metric_panel("Documents written  ·  SOURCE", source_dv,
                f"FROM {source} | STATS docs = COUNT(*)",
                "docs", "counted from the raw stream")),
        row(METRIC_HEIGHT,
            metric_panel("Documents stored to answer that  ·  DERIVED", derived_dv,
                f"FROM {derived} | STATS documents = COUNT(*)",
                "documents", "this is what derived metrics cost"),
            metric_panel("Documents stored to answer that  ·  SOURCE", source_dv,
                f"FROM {source} | STATS documents = COUNT(*)",
                "documents", "every document ever written")),

        # --- built-in ingest metrics ---
        row(CHART_HEIGHT,
            d_line("Ingest rate, docs/sec", "ingest.docs.rate", "docs_per_sec = SUM(metric.value)", "docs_per_sec"),
            s_line("Ingest rate, docs/sec", "docs_per_sec = TO_DOUBLE(COUNT(*)) / 10", "docs_per_sec")),
        row(CHART_HEIGHT,
            d_line("Ingest throughput, MB/sec", "ingest.bytes.rate",
                   "mb_per_sec = SUM(metric.value) / 1048576", "mb_per_sec"),
            None),  # the size of _source is not queryable from the stream itself
        row(METRIC_HEIGHT,
            metric_panel("Failed writes  ·  DERIVED", derived_dv,
                f'FROM {derived} | WHERE metric.name == "ingest.failures.count" '
                "| STATS failures = SUM(metric.value) | EVAL failures = COALESCE(failures, 0.0)",
                "failures", "ingest.failures.count — a failed write leaves nothing behind to count"),
            None),

        # --- user counters ---
        row(CHART_HEIGHT,
            d_line("HTTP requests", "http.requests", "requests = SUM(metric.value)", "requests"),
            s_line("HTTP requests", "requests = COUNT(*)", "requests", "http.request.method IS NOT NULL")),
        row(CHART_HEIGHT,
            d_line("5xx errors", "http.errors", "errors = SUM(metric.value)", "errors"),
            s_line("5xx errors", "errors = COUNT(*)", "errors", "http.response.status_code >= 500")),
        row(CHART_HEIGHT,
            d_line("4xx client errors", "http.client.errors", "errors = SUM(metric.value)", "errors"),
            s_line("4xx client errors", "errors = COUNT(*)", "errors",
                   "http.response.status_code >= 400 AND http.response.status_code < 500")),
        row(CHART_HEIGHT,
            d_line("Response payload, MB", "http.response.bytes", "mb = SUM(metric.value) / 1048576", "mb"),
            s_line("Response payload, MB", "mb = TO_DOUBLE(SUM(http.response.body.bytes)) / 1048576", "mb")),

        # --- user gauges ---
        row(CHART_HEIGHT,
            d_line("Peak queue depth", "queue.depth.max", "peak = MAX(metric.value)", "peak"),
            s_line("Peak queue depth", "peak = MAX(queue.depth)", "peak")),
        row(CHART_HEIGHT,
            d_line("Queue depth at close of interval", "queue.depth.last", "depth = MAX(metric.value)", "depth"),
            None),  # "last value per interval" has no cheap equivalent over raw documents
        # Weighted by the document count of the same series. A plain AVG of the avg gauge weights every
        # interval equally and lands far too low, because the busy intervals are the slow ones.
        row(CHART_HEIGHT,
            line_panel("Mean latency, ms (weighted)  ·  DERIVED", derived_dv,
                f'FROM {derived} | WHERE metric.name IN ("event.duration.avg", "ingest.docs.count") '
                '| STATS avg_v = MAX(CASE(metric.name == "event.duration.avg", metric.value, null)), '
                'cnt = MAX(CASE(metric.name == "ingest.docs.count", metric.value, null)) '
                f"BY bucket = {b}, svc = dimensions.service.name, reg = dimensions.cloud.region "
                "| WHERE avg_v IS NOT NULL AND cnt IS NOT NULL "
                "| STATS avg_ms = SUM(avg_v * cnt) / SUM(cnt) / 1000000 BY bucket",
                "bucket", "avg_ms", DERIVED_COLOUR),
            s_line("Mean latency, ms", "avg_ms = AVG(event.duration) / 1000000", "avg_ms")),
    ]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kibana", default="http://localhost:5601")
    parser.add_argument("--user", default="elastic-admin")
    parser.add_argument("--password", default="elastic-password")
    parser.add_argument("--data-stream", default="logs-derived-demo-default")
    args = parser.parse_args()

    token = base64.b64encode(f"{args.user}:{args.password}".encode()).decode()
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Basic {token}",
        "kbn-xsrf": "true",
    }

    def call(method, path, body=None):
        request = urllib.request.Request(
            f"{args.kibana}{path}",
            data=json.dumps(body).encode() if body is not None else None,
            headers=headers,
            method=method,
        )
        with urllib.request.urlopen(request, timeout=60) as response:
            return json.load(response)

    source = args.data_stream
    derived = f"derived-metrics-{args.data_stream}"

    found = call("GET", "/api/saved_objects/_find?type=index-pattern&per_page=100")
    by_title = {o["attributes"]["title"]: o["id"] for o in found["saved_objects"]}
    missing = [t for t in (source, derived) if t not in by_title]
    if missing:
        raise SystemExit(f"missing data views for {missing}; run ./demo.sh setup first")

    panels, references = [], []
    y = 0
    for index, (height, left, right) in enumerate(build_rows(by_title[derived], by_title[source], derived, source)):
        sides = (("l", left), ("r", right)) if right is not None else (("l", left),)
        for side, attributes in sides:
            panel_id = f"{index}{side}"
            width = 24 if right is not None else 48
            panels.append({
                "type": "lens",
                "panelIndex": panel_id,
                "gridData": {"x": 0 if side == "l" else 24, "y": y, "w": width, "h": height, "i": panel_id},
                "embeddableConfig": {"attributes": attributes, "enhancements": {}},
                "title": attributes["title"],
            })
            # By-value panels hoist their references to the dashboard, prefixed with the panel id.
            for reference in attributes["references"]:
                references.append({**reference, "name": f"{panel_id}:{reference['name']}"})
        y += height

    dashboard = {
        "attributes": {
            "title": "Derived metrics — derived vs source",
            "description": (
                "Left: answered from derived metrics. Right: answered from the raw data stream. "
                "The derived side trails by one unflushed interval (~13s). "
                "Full-width panels are metrics the source stream cannot answer."
            ),
            "panelsJSON": json.dumps(panels),
            "optionsJSON": json.dumps({"hidePanelTitles": False, "useMargins": True, "syncColors": False}),
            "timeRestore": True,
            "timeFrom": "now-15m",
            # Anchored at now. The derived side will trail the source by whatever landed in the
            # interval that has not been flushed yet — interval + grace + flush, ~13s here. See the
            # README for why that gap is constant in time but not in documents.
            "timeTo": "now",
            "refreshInterval": {"pause": False, "value": 10000},
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({"query": {"query": "", "language": "kuery"}, "filter": []})
            },
        },
        "references": references,
    }

    try:
        call("POST", f"/api/saved_objects/dashboard/{DASHBOARD_ID}?overwrite=true", dashboard)
    except urllib.error.HTTPError as e:
        raise SystemExit(f"failed to create the dashboard: {e.read().decode()[:500]}")

    print(f"    dashboard: {args.kibana}/app/dashboards#/view/{DASHBOARD_ID}")


if __name__ == "__main__":
    main()
