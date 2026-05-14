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
import java.util.ArrayList;
import java.util.LinkedHashMap;
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
    private final QueryMode mode;
    private final int limit;

    PrometheusQueryResponseListener(RestChannel channel, QueryMode mode, int limit) {
        this.channel = channel;
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
            XContentBuilder builder = convertToPrometheusJson(response, mode, limit);
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
     * <p>The ES|QL PROMQL command, combined with {@code | EVAL step = TO_LONG(step)}, produces
     * rows with the following column order (EVAL appends the converted step at the end):
     * <ol>
     *   <li>Column 0: value ({@code double})</li>
     *   <li>Columns 1..N-2: either a single {@code _timeseries} keyword column (JSON labels)
     *       or individual dimension/label columns</li>
     *   <li>Column N-1 (last): step ({@code long}, epoch milliseconds)</li>
     * </ol>
     */
    static XContentBuilder convertToPrometheusJson(EsqlResponse response, QueryMode mode) throws IOException {
        return convertToPrometheusJson(response, mode, Integer.MAX_VALUE);
    }

    static XContentBuilder convertToPrometheusJson(EsqlResponse response, QueryMode mode, int limit) throws IOException {
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

        Map<String, SeriesData> seriesMap = new LinkedHashMap<>();
        boolean truncated = false;

        for (Iterable<Object> row : response.rows()) {
            Object[] values = toArray(row, columns.size());

            String seriesKey;
            Map<String, String> metric;

            if (useSeriesCol) {
                seriesKey = values[DIMENSION_COL_START_IDX] != null ? values[DIMENSION_COL_START_IDX].toString() : "{}";
                metric = null;
            } else {
                StringBuilder keyBuilder = new StringBuilder();
                metric = new LinkedHashMap<>();
                for (int i = DIMENSION_COL_START_IDX; i < stepColIdx; i++) {
                    String label = columns.get(i).name();
                    String value = values[i] != null ? values[i].toString() : "";
                    metric.put(label, value);
                    keyBuilder.append(label).append('\0').append(value).append('\0');
                }
                seriesKey = keyBuilder.toString();
            }

            SeriesData series = seriesMap.get(seriesKey);
            if (series == null) {
                if (seriesMap.size() >= limit) {
                    truncated = true;
                    continue;
                }
                series = new SeriesData(useSeriesCol ? seriesKey : null, metric);
                seriesMap.put(seriesKey, series);
            }
            series.values.add(new double[] { parseTimestamp(values[stepColIdx]) });
            series.stringValues.add(formatSampleValue(values[VALUE_COL_IDX]));
        }

        return buildSuccessJson(seriesMap, mode, truncated);
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
     * Converts a timestamp from the ES|QL response into Unix epoch seconds.
     * The step column is cast to {@code LONG} (epoch milliseconds) via {@code TO_LONG(step)} in the ES|QL query.
     */
    private static double parseTimestamp(Object value) {
        if (value instanceof Number n) {
            return n.doubleValue() / 1000.0;
        }
        return 0;
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

    private static XContentBuilder buildSuccessJson(Map<String, SeriesData> seriesMap, QueryMode mode, boolean truncated)
        throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.field("status", "success");
        builder.startObject("data");
        builder.field("resultType", mode == QueryMode.RANGE ? "matrix" : "vector");
        builder.startArray("result");

        for (SeriesData series : seriesMap.values()) {
            builder.startObject();

            builder.startObject("metric");
            if (series.rawSeriesJson != null) {
                writeMetricFromSeriesJson(builder, series.rawSeriesJson);
            } else if (series.labels != null) {
                for (Map.Entry<String, String> entry : series.labels.entrySet()) {
                    builder.field(entry.getKey(), entry.getValue());
                }
            }
            builder.endObject(); // metric

            if (mode == QueryMode.RANGE) {
                builder.startArray("values");
                for (int i = 0; i < series.values.size(); i++) {
                    builder.startArray();
                    builder.value(series.values.get(i)[0]);
                    builder.value(series.stringValues.get(i));
                    builder.endArray();
                }
                builder.endArray(); // values
            } else {
                // Instant query: emit the last sample (rows arrive ascending by timestamp)
                int last = series.values.size() - 1;
                builder.startArray("value");
                builder.value(series.values.get(last)[0]);
                builder.value(series.stringValues.get(last));
                builder.endArray(); // value
            }

            builder.endObject(); // result entry
        }

        builder.endArray(); // result
        builder.endObject(); // data
        if (truncated) {
            builder.startArray("warnings");
            builder.value("results truncated due to limit");
            builder.endArray();
        }
        builder.endObject(); // root
        return builder;
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

    static class SeriesData {
        final String rawSeriesJson;
        final Map<String, String> labels;
        final List<double[]> values = new ArrayList<>();
        final List<String> stringValues = new ArrayList<>();

        SeriesData(String rawSeriesJson, Map<String, String> labels) {
            this.rawSeriesJson = rawSeriesJson;
            this.labels = labels;
        }
    }
}
