/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
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
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.action.ResponseValueUtils;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.time.ZoneId;
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
 * <p>PromQL {@code query_range} responses can legitimately have hundreds of thousands of rows
 * (one per time series), each with a step-count-sized multi-valued {@code value}/{@code step}
 * entry. This class therefore reads the {@code value}/{@code step} columns directly from the
 * response's columnar {@link Page}/{@link Block} structures (the same primitive-value access
 * pattern already used by ES|QL's own direct-from-Block JSON writer, {@code PositionToXContent})
 * instead of the generic, per-row {@code EsqlResponse.rows()} abstraction — profiling showed that
 * the generic path's per-cell {@code ArrayList<Object>} allocation for these two always-multi-valued
 * columns dominates this endpoint's memory cost at scale. Single-valued dimension/label columns
 * still go through {@code ResponseValueUtils}' type-to-value extraction table (it's the correct,
 * complete place to handle every ES|QL {@code DataType}'s wire representation, e.g. {@code ip}
 * fields are a binary encoding, not a plain string) — that per-row extraction was never the
 * measured hot path, only the multi-valued boxing was.
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
        // and crash the node. Reading its pages/blocks directly (without incRef/decRef/close) is safe: the
        // response holds a live reference for the duration of this synchronous callback, same as the
        // generic EsqlResponse.rows()/ResponseValueUtils path this class used to go through.
        try {
            XContentBuilder builder = convertToPrometheusJson(
                queryResponse.pages(),
                queryResponse.columns(),
                queryResponse.zoneId(),
                resultType,
                mode,
                limit
            );
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
    static XContentBuilder convertToPrometheusJson(
        List<Page> pages,
        List<ColumnInfoImpl> columns,
        ZoneId zoneId,
        String resultType,
        QueryMode mode
    ) throws IOException {
        return convertToPrometheusJson(pages, columns, zoneId, resultType, mode, Integer.MAX_VALUE);
    }

    static XContentBuilder convertToPrometheusJson(
        List<Page> pages,
        List<ColumnInfoImpl> columns,
        ZoneId zoneId,
        String resultType,
        QueryMode mode,
        int limit
    ) throws IOException {
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
            writeScalarResult(builder, pages, stepColIdx);
            truncated = false;
        } else {
            builder.startArray("result");
            truncated = writeResultArray(builder, pages, mode, limit, columns, zoneId, stepColIdx, useSeriesCol);
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

    private static void writeScalarResult(XContentBuilder builder, List<Page> pages, int stepColIdx) throws IOException {
        for (Page page : pages) {
            DoubleBlock valueBlock = (DoubleBlock) page.getBlock(VALUE_COL_IDX);
            LongBlock stepBlock = (LongBlock) page.getBlock(stepColIdx);
            for (int position = 0; position < page.getPositionCount(); position++) {
                Sample sample = readLastSample(valueBlock, stepBlock, position);
                if (sample == null) {
                    continue;
                }
                builder.startArray("result");
                builder.value(parseTimestamp(sample.stepMillis()));
                builder.value(formatSampleValue(sample.value()));
                builder.endArray();
                return;
            }
        }
        builder.startArray("result");
        builder.endArray();
    }

    private static boolean writeResultArray(
        XContentBuilder builder,
        List<Page> pages,
        QueryMode mode,
        int limit,
        List<ColumnInfoImpl> columns,
        ZoneId zoneId,
        int stepColIdx,
        boolean useSeriesCol
    ) throws IOException {
        int seriesCount = 0;
        BytesRef scratch = new BytesRef();
        StringBuilder valueBuffer = new StringBuilder(24);

        for (Page page : pages) {
            DoubleBlock valueBlock = (DoubleBlock) page.getBlock(VALUE_COL_IDX);
            LongBlock stepBlock = (LongBlock) page.getBlock(stepColIdx);
            int positionCount = page.getPositionCount();

            for (int position = 0; position < positionCount; position++) {
                if (seriesCount++ == limit) {
                    return true;
                }

                // Both value and step are multi-valued (one entry per step) due to TimeSeriesCollapse.
                // A single-step series produces a plain scalar instead of a List.
                if (valueBlock.isNull(position) || stepBlock.isNull(position)) {
                    continue; // series with no data in range
                }
                int count = valueBlock.getValueCount(position);
                int stepCount = stepBlock.getValueCount(position);
                if (count != stepCount) {
                    throw new IllegalStateException(
                        "PROMQL response has misaligned collapsed step/value columns: step count ["
                            + stepCount
                            + "], value count ["
                            + count
                            + "]"
                    );
                }
                if (count == 0) {
                    continue; // series with no data in range
                }
                int valueStart = valueBlock.getFirstValueIndex(position);
                int stepStart = stepBlock.getFirstValueIndex(position);
                assert timestampsAreAscending(stepBlock, stepStart, count) : "PROMQL response step timestamps must be ascending";

                builder.startObject();
                buildMetricLabels(builder, useSeriesCol, page, position, stepColIdx, columns, zoneId, scratch);
                buildMetricValues(mode, builder, valueBuffer, valueBlock, stepBlock, valueStart, stepStart, count);
                builder.endObject(); // result entry
            }
        }
        return false;
    }

    private static void buildMetricLabels(
        XContentBuilder builder,
        boolean useSeriesCol,
        Page page,
        int position,
        int stepColIdx,
        List<ColumnInfoImpl> columns,
        ZoneId zoneId,
        BytesRef scratch
    ) throws IOException {
        // metric labels
        builder.startObject("metric");
        if (useSeriesCol) {
            Block seriesBlock = page.getBlock(DIMENSION_COL_START_IDX);
            String seriesJson = "{}";
            if (seriesBlock.isNull(position) == false) {
                BytesRef val = ((BytesRefBlock) seriesBlock).getBytesRef(seriesBlock.getFirstValueIndex(position), scratch);
                seriesJson = val.utf8ToString();
            }
            writeMetricFromSeriesJson(builder, seriesJson);
        } else {
            for (int i = DIMENSION_COL_START_IDX; i < stepColIdx; i++) {
                Block labelBlock = page.getBlock(i);
                // Omit null labels (e.g. a null-filled missing BY label) rather than emitting "". PromQL distinguishes
                // an absent label from one whose value is empty; this mirrors writeMetricFields on the _timeseries path.
                if (labelBlock.isNull(position)) {
                    continue;
                }
                builder.field(columns.get(i).name());
                writeLabelValue(builder, labelBlock, columns.get(i).type(), position, zoneId, scratch);
            }
        }
        builder.endObject(); // metric
    }

    /**
     * Writes a single-valued dimension/label column's value at {@code position} as a Prometheus label string.
     * PromQL {@code by}-labels are almost always {@code keyword}-typed: that case writes the UTF-8 bytes straight
     * into the builder, avoiding a {@code utf8ToString()} allocation per label per row (profiling showed this was
     * a measurable cost once done correctly for every row of a wide-cardinality result). Other types (IP, VERSION,
     * DATETIME, etc. — all still backed by BytesRefBlock/LongBlock, not plain strings) fall back to ES|QL's own
     * type-to-value extraction table for correctness; that fallback is not a hot path since it only runs for the
     * rare non-keyword dimension column, not for every row's multi-valued value/step data.
     */
    private static void writeLabelValue(XContentBuilder builder, Block block, DataType type, int position, ZoneId zoneId, BytesRef scratch)
        throws IOException {
        int valueIndex = block.getFirstValueIndex(position);
        if (type == DataType.KEYWORD || type == DataType.TEXT) {
            BytesRef val = ((BytesRefBlock) block).getBytesRef(valueIndex, scratch);
            builder.utf8Value(val.bytes, val.offset, val.length);
        } else {
            Object value = ResponseValueUtils.valueExtractorFor(type, zoneId).extract(block, valueIndex, scratch);
            builder.value(value.toString());
        }
    }

    private static void buildMetricValues(
        QueryMode mode,
        XContentBuilder builder,
        StringBuilder valueBuffer,
        DoubleBlock valueBlock,
        LongBlock stepBlock,
        int valueStart,
        int stepStart,
        int count
    ) throws IOException {
        if (mode == QueryMode.RANGE) {
            // values — parallel arrays of (timestamp_seconds, value_string)
            builder.startArray("values");
            for (int i = 0; i < count; i++) {
                builder.startArray();
                builder.value(parseTimestamp(stepBlock.getLong(stepStart + i)));
                writeSampleValue(builder, valueBuffer, valueBlock.getDouble(valueStart + i));
                builder.endArray();
            }
            builder.endArray(); // values
        } else {
            // Instant query: emit the last sample (rows arrive in ascending timestamp order)
            // This is a temporary approximation for a range query and there may be multiple samples.
            // The proper implementation will guarantee that there will be just one sample.
            int last = count - 1;
            builder.startArray("value");
            builder.value(parseTimestamp(stepBlock.getLong(stepStart + last)));
            writeSampleValue(builder, valueBuffer, valueBlock.getDouble(valueStart + last));
            builder.endArray(); // value
        }
    }

    /**
     * Writes a finite/NaN/Infinite sample value without a per-value {@code String} allocation for the common
     * (finite) case. {@code valueBuffer} must be one instance reused across an entire query's result set (created
     * once in {@link #writeResultArray}, not per value) — {@link StringBuilder#append(double)} writes digits
     * directly into the {@code StringBuilder}'s own backing array on JDK 25+, so reusing one instance means that
     * backing array only ever grows once (to fit the largest possible formatted double), not once per value.
     * {@link XContentBuilder#value(CharSequence)} then writes straight from it with no further copy.
     */
    private static void writeSampleValue(XContentBuilder builder, StringBuilder valueBuffer, double value) throws IOException {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            // Rare path; formatSampleValue returns interned constant strings ("NaN"/"+Inf"/"-Inf").
            builder.value(formatSampleValue(value));
            return;
        }
        valueBuffer.setLength(0);
        valueBuffer.append(value);
        builder.value(valueBuffer);
    }

    private record Sample(double value, long stepMillis) {}

    /** Returns the last (value, step) sample for a position's collapsed series, or {@code null} if it has no data. */
    private static Sample readLastSample(DoubleBlock valueBlock, LongBlock stepBlock, int position) {
        if (valueBlock.isNull(position) || stepBlock.isNull(position)) {
            return null;
        }
        int count = valueBlock.getValueCount(position);
        int stepCount = stepBlock.getValueCount(position);
        if (count != stepCount) {
            throw new IllegalStateException(
                "PROMQL response has misaligned collapsed step/value columns: step count [" + stepCount + "], value count [" + count + "]"
            );
        }
        if (count == 0) {
            return null;
        }
        int lastOffset = count - 1;
        double value = valueBlock.getDouble(valueBlock.getFirstValueIndex(position) + lastOffset);
        long stepMillis = stepBlock.getLong(stepBlock.getFirstValueIndex(position) + lastOffset);
        return new Sample(value, stepMillis);
    }

    private static boolean timestampsAreAscending(LongBlock stepBlock, int start, int count) {
        long previousTimestamp = Long.MIN_VALUE;
        int end = start + count;
        for (int i = start; i < end; i++) {
            long timestamp = stepBlock.getLong(i);
            if (timestamp < previousTimestamp) {
                return false;
            }
            previousTimestamp = timestamp;
        }
        return true;
    }

    /**
     * Converts a timestamp from the ES|QL response into Unix epoch seconds.
     * The step column is cast to {@code LONG} (epoch milliseconds) via {@code TO_LONG(step)} in the ES|QL query.
     */
    private static double parseTimestamp(long millis) {
        return millis / 1000.0;
    }

    /**
     * Formats a sample value for the Prometheus JSON response.
     * Prometheus represents values as strings, with special handling for NaN and Infinity.
     */
    static String formatSampleValue(double value) {
        if (Double.isNaN(value)) {
            return "NaN";
        } else if (Double.isInfinite(value)) {
            return value > 0 ? "+Inf" : "-Inf";
        }
        return Double.toString(value);
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
