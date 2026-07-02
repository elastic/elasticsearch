/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.core.esql.action.EsqlResponse;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Listens for an {@link EsqlQueryResponse}, converts its columnar result into the
 * Prometheus JSON format, and sends it as a {@link RestResponse}.
 * <p>
 * Supports two query modes:
 * <ul>
 *   <li>{@link QueryMode#RANGE} — {@code resultType: "matrix"}, each series has a {@code values} array.</li>
 *   <li>{@link QueryMode#INSTANT} — {@code resultType: "vector"}, each series has a single {@code value} pair
 *       (the last sample, since rows arrive ascending by timestamp).</li>
 * </ul>
 *
 * <p>The ES|QL PROMQL command runs with {@code collapsed=true}, so each row in the response
 * represents one complete time series with multi-valued {@code value} and {@code step} columns.
 * Example collapsed response shape:
 * <pre>{@code
 * value       | _timeseries                                      | step
 * [1.0, 2.0]  | {"__name__":"http_requests_total","job":"api"}   | [1710000000000, 1710000060000]
 * [3.0, 4.0]  | {"__name__":"http_requests_total","job":"web"}   | [1710000000000, 1710000060000]
 * }</pre>
 *
 * @see <a href="https://prometheus.io/docs/prometheus/latest/querying/api/">Prometheus HTTP API</a>
 */
class PrometheusQueryResponseListener implements ActionListener<EsqlQueryResponse> {

    private static final Logger logger = LogManager.getLogger(PrometheusQueryResponseListener.class);

    // Column names expected in the ES|QL PROMQL response.
    static final String VALUE_COLUMN = "value";
    static final String STEP_PARAM = "step";

    // Fixed column indices produced by the PROMQL command + EVAL step = TO_LONG(step).
    // EVAL appends the new step column at the end, so dimension columns occupy indices 1..N-2.
    private static final int VALUE_COL_IDX = 0;
    private static final int DIMENSION_COL_START_IDX = 1;

    enum QueryMode {
        RANGE,
        INSTANT
    }

    private final RestChannel channel;
    private final String resultType;
    private final QueryMode mode;
    private final int limit;

    PrometheusQueryResponseListener(RestChannel channel, String resultType, QueryMode mode, int limit) {
        this.channel = channel;
        this.resultType = resultType;
        this.mode = mode;
        this.limit = limit;
    }

    @Override
    public void onResponse(EsqlQueryResponse queryResponse) {
        // Do not close queryResponse here - the transport framework's respondAndRelease handles decRef.
        // If we close it manually, it will cause an AssertionError ("invalid decRef call: already closed")
        // and crash the node.
        try {
            EsqlResponse response = queryResponse.response();
            XContentBuilder builder = convertToPrometheusJson(response, resultType, mode, limit);
            channel.sendResponse(new RestResponse(RestStatus.OK, builder));
        } catch (Exception e) {
            sendErrorResponse(e);
        }
    }

    @Override
    public void onFailure(Exception e) {
        sendErrorResponse(e);
    }

    private void sendErrorResponse(Exception e) {
        logger.debug("PromQL {} request failed", mode == QueryMode.RANGE ? "query_range" : "query", e);
        PrometheusErrorResponse.send(channel, e, logger);
    }

    /**
     * Converts an ES|QL response into a Prometheus-compatible JSON response.
     *
     * <p>Each row is one collapsed time series produced by {@code TimeSeriesCollapseOperator}.
     * Column layout (EVAL appends the converted step at the end):
     * <ol>
     *   <li>Column 0: {@code value} ({@code double} or {@code List<Double>}) — one value per step</li>
     *   <li>Columns 1..N-2: either a single {@code _timeseries} keyword column (JSON labels)
     *       or individual dimension/label columns (single-valued)</li>
     *   <li>Column N-1 (last): {@code step} ({@code long} or {@code List<Long>}, epoch milliseconds)</li>
     * </ol>
     */
    static XContentBuilder convertToPrometheusJson(EsqlResponse response, String resultType, QueryMode mode) throws IOException {
        return convertToPrometheusJson(response, resultType, mode, Integer.MAX_VALUE);
    }

    static XContentBuilder convertToPrometheusJson(EsqlResponse response, String resultType, QueryMode mode, int limit) throws IOException {
        List<? extends ColumnInfo> columns = response.columns();
        if (columns.size() < 1 || VALUE_COLUMN.equals(columns.get(VALUE_COL_IDX).name()) == false) {
            throw new IllegalStateException("PROMQL response is missing required 'value' column at index " + VALUE_COL_IDX);
        }
        final int stepColIdx = columns.size() - 1;
        if (columns.size() < 2 || STEP_PARAM.equals(columns.get(stepColIdx).name()) == false) {
            throw new IllegalStateException("PROMQL response is missing required 'step' column at last index " + stepColIdx);
        }
        // Column 1 is either _timeseries (a JSON blob) or the first of the individual dimension columns
        final boolean useSeriesCol = columns.size() > 2 && MetadataAttribute.TIMESERIES.equals(columns.get(DIMENSION_COL_START_IDX).name());

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.field("status", "success");
        builder.startObject("data");
        builder.field("resultType", resultType);
        boolean truncated;
        if ("scalar".equals(resultType)) {
            writeScalarResult(builder, response, columns, stepColIdx);
            truncated = false;
        } else {
            builder.startArray("result");
            truncated = writeResultArray(builder, response, mode, limit, columns, stepColIdx, useSeriesCol);
            builder.endArray(); // result
        }
        builder.endObject(); // data
        if (truncated) {
            builder.startArray("warnings");
            builder.value("results truncated due to limit");
            builder.endArray();
        }
        builder.endObject(); // root
        return builder;
    }

    private static void writeScalarResult(
        XContentBuilder builder,
        EsqlResponse response,
        List<? extends ColumnInfo> columns,
        int stepColIdx
    ) throws IOException {
        for (Iterable<Object> row : response.rows()) {
            Object[] values = toArray(row, columns.size());
            List<Object> valueList = toList(values[VALUE_COL_IDX]);
            List<Object> stepList = toList(values[stepColIdx]);
            if (valueList == null || stepList == null || valueList.isEmpty()) {
                continue;
            }
            if (valueList.size() != stepList.size()) {
                throw new IllegalStateException(
                    "PROMQL response has misaligned collapsed step/value columns: step count ["
                        + stepList.size()
                        + "], value count ["
                        + valueList.size()
                        + "]"
                );
            }
            int last = valueList.size() - 1;
            builder.startArray("result");
            builder.value(parseTimestamp(stepList.get(last)));
            builder.value(formatSampleValue(valueList.get(last)));
            builder.endArray();
            return;
        }
        builder.startArray("result");
        builder.endArray();
    }

    private static boolean writeResultArray(
        XContentBuilder builder,
        EsqlResponse response,
        QueryMode mode,
        int limit,
        List<? extends ColumnInfo> columns,
        int stepColIdx,
        boolean useSeriesCol
    ) throws IOException {
        int seriesCount = 0;

        for (Iterable<Object> row : response.rows()) {
            if (seriesCount++ == limit) {
                return true;
            }

            Object[] values = toArray(row, columns.size());

            // Both value and step are multi-valued (one entry per step) due to TimeSeriesCollapse.
            // A single-step series produces a plain scalar instead of a List.
            List<Object> valueList = toList(values[VALUE_COL_IDX]);
            List<Object> stepList = toList(values[stepColIdx]);

            if (valueList == null || stepList == null || valueList.isEmpty()) {
                continue; // series with no data in range
            }
            if (valueList.size() != stepList.size()) {
                throw new IllegalStateException(
                    "PROMQL response has misaligned collapsed step/value columns: step count ["
                        + stepList.size()
                        + "], value count ["
                        + valueList.size()
                        + "]"
                );
            }

            builder.startObject();
            buildMetricLabels(builder, useSeriesCol, values, stepColIdx, columns);
            buildMetricValues(mode, builder, valueList, stepList);
            builder.endObject(); // result entry
        }
        return false;
    }

    private static void buildMetricLabels(
        XContentBuilder builder,
        boolean useSeriesCol,
        Object[] values,
        int stepColIdx,
        List<? extends ColumnInfo> columns
    ) throws IOException {
        // metric labels
        builder.startObject("metric");
        if (useSeriesCol) {
            String seriesJson = values[DIMENSION_COL_START_IDX] != null ? values[DIMENSION_COL_START_IDX].toString() : "{}";
            writeMetricFromSeriesJson(builder, seriesJson);
        } else {
            for (int i = DIMENSION_COL_START_IDX; i < stepColIdx; i++) {
                // Omit null labels (e.g. a null-filled missing BY label) rather than emitting "". PromQL distinguishes
                // an absent label from one whose value is empty; this mirrors writeMetricFields on the _timeseries path.
                if (values[i] != null) {
                    builder.field(columns.get(i).name(), values[i].toString());
                }
            }
        }
        builder.endObject(); // metric
    }

    private static void buildMetricValues(QueryMode mode, XContentBuilder builder, List<Object> valueList, List<Object> stepList)
        throws IOException {
        assert timestampsAreAscending(stepList) : "PROMQL response step timestamps must be ascending";
        if (mode == QueryMode.RANGE) {
            // values — parallel arrays of (timestamp_seconds, value_string)
            builder.startArray("values");
            for (int i = 0; i < valueList.size(); i++) {
                builder.startArray();
                builder.value(parseTimestamp(stepList.get(i)));
                builder.value(formatSampleValue(valueList.get(i)));
                builder.endArray();
            }
            builder.endArray(); // values
        } else {
            // Instant query: emit the last sample (rows arrive in ascending timestamp order)
            // This is a temporary approximation for a range query and there may be multiple samples.
            // The proper implementation will guarantee that there will be just one sample.
            int last = valueList.size() - 1;
            builder.startArray("value");
            builder.value(parseTimestamp(stepList.get(last)));
            builder.value(formatSampleValue(valueList.get(last)));
            builder.endArray(); // value
        }
    }

    private static boolean timestampsAreAscending(List<Object> stepList) {
        double previousTimestamp = Double.NEGATIVE_INFINITY;
        for (Object step : stepList) {
            double timestamp = parseTimestamp(step);
            if (timestamp < previousTimestamp) {
                return false;
            }
            previousTimestamp = timestamp;
        }
        return true;
    }

    private static Object[] toArray(Iterable<Object> row, int size) {
        Object[] arr = new Object[size];
        int i = 0;
        for (Object val : row) {
            if (i < size) {
                arr[i++] = val;
            }
        }
        return arr;
    }

    /**
     * Converts a single value or {@code List} to a {@code List<Object>}.
     * Returns {@code null} if {@code value} is {@code null}.
     */
    @SuppressWarnings("unchecked")
    private static List<Object> toList(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof List<?> list) {
            return (List<Object>) list;
        }
        return List.of(value);
    }

    /**
     * Converts a timestamp from the ES|QL response into Unix epoch seconds.
     * The step column is cast to {@code LONG} (epoch milliseconds) via {@code TO_LONG(step)} in the ES|QL query.
     */
    private static double parseTimestamp(Object value) {
        if (value instanceof Number n) {
            return n.doubleValue() / 1000.0;
        }
        throw new IllegalStateException(
            "PROMQL response step column must be Number (epoch millis); got [" + (value == null ? "null" : value.getClass().getName()) + "]"
        );
    }

    /**
     * Formats a sample value for the Prometheus JSON response.
     * Prometheus represents values as strings, with special handling for NaN and Infinity.
     */
    static String formatSampleValue(Object value) {
        if (value == null) {
            return "NaN";
        }
        if (value instanceof Double d) {
            if (Double.isNaN(d)) {
                return "NaN";
            } else if (Double.isInfinite(d)) {
                return d > 0 ? "+Inf" : "-Inf";
            }
            return d.toString();
        }
        return value.toString();
    }

    /**
     * Writes metric labels from a {@code _timeseries} JSON value.
     * <ul>
     *   <li>The {@code labels} namespace is unwrapped without a prefix:
     *       {@code {"labels":{"__name__":"up","job":"prometheus"}}} → fields {@code __name__}, {@code job}.</li>
     *   <li>All other namespaces are flattened recursively with dot-separated paths:
     *       {@code {"attributes":{"resource":{"service.name":"foo"}}}} → field {@code attributes.resource.service.name}.</li>
     * </ul>
     */
    static void writeMetricFromSeriesJson(XContentBuilder builder, String seriesJson) throws IOException {
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, seriesJson)) {
            Map<String, Object> root = parser.map();
            Object labelsObj = root.remove("labels");
            if (labelsObj instanceof Map<?, ?> labels) {
                writeMetricFields(builder, "", labels);
            }
            writeMetricFields(builder, "", root);
        }
    }

    private static void writeMetricFields(XContentBuilder builder, String prefix, Map<?, ?> map) throws IOException {
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            String key = prefix + entry.getKey();
            if (entry.getValue() instanceof Map<?, ?> nested) {
                writeMetricFields(builder, key + ".", nested);
            } else if (entry.getValue() != null) {
                builder.field(key, entry.getValue().toString());
            }
        }
    }
}
