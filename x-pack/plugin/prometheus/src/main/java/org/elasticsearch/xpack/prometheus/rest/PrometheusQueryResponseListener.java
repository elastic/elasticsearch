/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.ChunkedRestResponseBodyPart;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContent;
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
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.IntConsumer;

/**
 * Listens for an {@link EsqlQueryResponse}, converts its columnar result into the
 * Prometheus JSON format, and streams it as a chunked {@link RestResponse}.
 * <p>
 * The body is never built as one in-memory buffer: a contiguous buffer for a large result grows past the 2 GB
 * array limit and kills the node with an {@link OutOfMemoryError}. Each series is serialized in slices of at most
 * {@link #SAMPLES_PER_CHUNK} samples, so a chunk stays small even for a single series with many samples. Each page of
 * the result is released as soon as it has been serialized, so memory goes down while the body is sent.
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
    /** The {@code _timeseries} object holding the labels of a Prometheus series, which are named without its prefix. */
    private static final String LABELS_NAMESPACE = "labels";

    // Fixed column indices produced by the PROMQL command + EVAL step = TO_LONG(step).
    // EVAL appends the new step column at the end, so dimension columns occupy indices 1..N-2.
    private static final int VALUE_COL_IDX = 0;
    private static final int DIMENSION_COL_START_IDX = 1;

    /**
     * The maximum number of samples serialized in one piece of the chunked body, about 20-40 KB of JSON. The REST layer
     * cuts the body into chunks between pieces, so this bounds how far a chunk can overshoot its size hint.
     */
    static final int SAMPLES_PER_CHUNK = 1024;

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
        // Do not close queryResponse here - the transport framework's respondAndRelease handles its own decRef.
        // The body is serialized lazily from the response's pages after this callback returns, so keep a reference
        // to the blocks of each page and release it once that page has been serialized (or the channel is closed).
        OwnedPages pages = OwnedPages.copyOf(queryResponse.pages());
        ChunkedToXContent body;
        try {
            body = toChunkedXContent(
                pages.pages(),
                queryResponse.columns(),
                queryResponse.zoneId(),
                resultType,
                mode,
                limit,
                pages::release
            );
        } catch (Exception e) {
            pages.close();
            sendErrorResponse(e);
            return;
        }
        try {
            channel.sendResponse(
                RestResponse.chunked(RestStatus.OK, ChunkedRestResponseBodyPart.fromXContent(body, channel.request(), channel), pages)
            );
        } catch (Exception e) {
            pages.close();
            sendErrorResponse(e);
        }
    }

    @Override
    public void onFailure(Exception e) {
        sendErrorResponse(e);
    }

    private void sendErrorResponse(Exception e) {
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
        XContentBuilder builder = XContentFactory.jsonBuilder();
        Iterator<? extends ToXContent> chunks = toChunkedXContent(pages, columns, zoneId, resultType, mode, limit, page -> {})
            .toXContentChunked(ToXContent.EMPTY_PARAMS);
        while (chunks.hasNext()) {
            chunks.next().toXContent(builder, ToXContent.EMPTY_PARAMS);
        }
        return builder;
    }

    /**
     * Same as {@link #convertToPrometheusJson} but as a {@link ChunkedToXContent}, so the body can be streamed to the client
     * instead of being built as one contiguous buffer. Everything that can reject the response (missing columns, misaligned
     * step/value counts) is checked eagerly here, before any byte is sent. {@code onPageWritten} is called with the index of
     * each page once its last row has been serialized, so the caller can release it.
     */
    static ChunkedToXContent toChunkedXContent(
        List<Page> pages,
        List<ColumnInfoImpl> columns,
        ZoneId zoneId,
        String resultType,
        QueryMode mode,
        int limit,
        IntConsumer onPageWritten
    ) {
        if (columns.size() < 1 || VALUE_COLUMN.equals(columns.get(VALUE_COL_IDX).name()) == false) {
            throw new IllegalStateException("PROMQL response is missing required 'value' column at index " + VALUE_COL_IDX);
        }
        final int stepColIdx = columns.size() - 1;
        if (columns.size() < 2 || STEP_PARAM.equals(columns.get(stepColIdx).name()) == false) {
            throw new IllegalStateException("PROMQL response is missing required 'step' column at last index " + stepColIdx);
        }
        // Column 1 is either _timeseries (a JSON blob) or the first of the individual dimension columns
        final boolean useSeriesCol = columns.size() > 2 && MetadataAttribute.TIMESERIES.equals(columns.get(DIMENSION_COL_START_IDX).name());

        ToXContent header = (b, p) -> b.startObject().field("status", "success").startObject("data").field("resultType", resultType);
        if ("scalar".equals(resultType)) {
            return params -> Iterators.concat(
                Iterators.single(header),
                ChunkedToXContentHelper.chunk((b, p) -> writeScalarResult(b, pages, stepColIdx)),
                ChunkedToXContentHelper.endObject(), // data
                ChunkedToXContentHelper.endObject() // root
            );
        }

        // The first `limit` rows are emitted; empty series count towards the limit.
        int totalRows = 0;
        final int[] emittedRows = new int[pages.size()];
        for (int i = 0; i < pages.size(); i++) {
            Page page = pages.get(i);
            DoubleBlock valueBlock = (DoubleBlock) page.getBlock(VALUE_COL_IDX);
            LongBlock stepBlock = (LongBlock) page.getBlock(stepColIdx);
            for (int position = 0; position < page.getPositionCount(); position++) {
                if (totalRows < limit) {
                    sampleCount(valueBlock, stepBlock, position);
                    emittedRows[i]++;
                }
                totalRows++;
            }
        }
        final boolean truncated = totalRows > limit;

        return params -> {
            BytesRef scratch = new BytesRef();
            StringBuilder valueBuffer = new StringBuilder(24);
            Iterator<ToXContent> series = Iterators.flatMap(Iterators.forRange(0, pages.size(), index -> index), index -> {
                Page page = pages.get(index);
                Iterator<Row> rows = Iterators.forRange(0, emittedRows[index], position -> new Row(page, position));
                return Iterators.concat(
                    Iterators.flatMap(
                        Iterators.filter(rows, row -> row.sampleCount(stepColIdx) > 0),
                        row -> seriesChunks(row, mode, columns, zoneId, stepColIdx, useSeriesCol, scratch, valueBuffer)
                    ),
                    // Every row of the page has been written into the body by now, so its blocks are no longer needed.
                    Iterators.single((b, p) -> {
                        onPageWritten.accept(index);
                        return b;
                    })
                );
            });
            return Iterators.concat(
                Iterators.single(header),
                ChunkedToXContentHelper.startArray("result"),
                series,
                ChunkedToXContentHelper.endArray(), // result
                ChunkedToXContentHelper.endObject(), // data
                truncated
                    ? ChunkedToXContentHelper.chunk((b, p) -> b.startArray("warnings").value("results truncated due to limit").endArray())
                    : Collections.emptyIterator(),
                ChunkedToXContentHelper.endObject() // root
            );
        };
    }

    /**
     * The pages of a response, each holding its own reference to its blocks, so that each page can be released as soon as
     * it has been serialized rather than when the whole response is released. Releasing is safe from any thread and more
     * than once: a page is released either after it is written or, at the latest, when the response is closed.
     */
    private static final class OwnedPages implements Releasable {
        private final List<Page> pages;
        private final AtomicReferenceArray<Page> unreleased;

        private OwnedPages(List<Page> pages) {
            this.pages = pages;
            this.unreleased = new AtomicReferenceArray<>(pages.toArray(Page[]::new));
        }

        static OwnedPages copyOf(List<Page> pages) {
            return new OwnedPages(pages.stream().map(Page::shallowCopy).toList());
        }

        List<Page> pages() {
            return pages;
        }

        void release(int index) {
            Page page = unreleased.getAndSet(index, null);
            if (page != null) {
                page.releaseBlocks();
            }
        }

        @Override
        public void close() {
            for (int i = 0; i < unreleased.length(); i++) {
                release(i);
            }
        }
    }

    /** One row (one collapsed series) of the ES|QL response. */
    private record Row(Page page, int position) {
        int sampleCount(int stepColIdx) {
            return PrometheusQueryResponseListener.sampleCount(
                (DoubleBlock) page.getBlock(VALUE_COL_IDX),
                (LongBlock) page.getBlock(stepColIdx),
                position
            );
        }
    }

    private static XContentBuilder writeScalarResult(XContentBuilder builder, List<Page> pages, int stepColIdx) throws IOException {
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
                return builder;
            }
        }
        builder.startArray("result");
        builder.endArray();
        return builder;
    }

    /**
     * The pieces of the chunked body for one series with samples: its labels, then its samples in slices of at most
     * {@link #SAMPLES_PER_CHUNK}, so that a series with many samples is spread over many chunks instead of one.
     */
    private static Iterator<ToXContent> seriesChunks(
        Row row,
        QueryMode mode,
        List<ColumnInfoImpl> columns,
        ZoneId zoneId,
        int stepColIdx,
        boolean useSeriesCol,
        BytesRef scratch,
        StringBuilder valueBuffer
    ) {
        Page page = row.page();
        int position = row.position();
        DoubleBlock valueBlock = (DoubleBlock) page.getBlock(VALUE_COL_IDX);
        LongBlock stepBlock = (LongBlock) page.getBlock(stepColIdx);
        int count = valueBlock.getValueCount(position);
        int valueStart = valueBlock.getFirstValueIndex(position);
        int stepStart = stepBlock.getFirstValueIndex(position);
        assert timestampsAreAscending(stepBlock, stepStart, count) : "PROMQL response step timestamps must be ascending";

        if (mode == QueryMode.INSTANT) {
            return ChunkedToXContentHelper.chunk((builder, params) -> {
                builder.startObject();
                buildMetricLabels(builder, useSeriesCol, page, position, stepColIdx, columns, zoneId, scratch);
                // Instant query: emit the last sample (rows arrive in ascending timestamp order)
                // This is a temporary approximation for a range query and there may be multiple samples.
                // The proper implementation will guarantee that there will be just one sample.
                int last = count - 1;
                builder.startArray("value");
                builder.value(parseTimestamp(stepBlock.getLong(stepStart + last)));
                writeSampleValue(builder, valueBuffer, valueBlock.getDouble(valueStart + last));
                builder.endArray(); // value
                return builder.endObject(); // result entry
            });
        }
        // values: parallel arrays of (timestamp_seconds, value_string)
        int slices = (count + SAMPLES_PER_CHUNK - 1) / SAMPLES_PER_CHUNK;
        return Iterators.concat(ChunkedToXContentHelper.chunk((builder, params) -> {
            builder.startObject();
            buildMetricLabels(builder, useSeriesCol, page, position, stepColIdx, columns, zoneId, scratch);
            return builder.startArray("values");
        }), Iterators.forRange(0, slices, slice -> (builder, params) -> {
            int from = slice * SAMPLES_PER_CHUNK;
            int to = Math.min(count, from + SAMPLES_PER_CHUNK);
            for (int i = from; i < to; i++) {
                builder.startArray();
                builder.value(parseTimestamp(stepBlock.getLong(stepStart + i)));
                writeSampleValue(builder, valueBuffer, valueBlock.getDouble(valueStart + i));
                builder.endArray();
            }
            return builder;
        }),
            ChunkedToXContentHelper.endArray(), // values
            ChunkedToXContentHelper.endObject() // result entry
        );
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
                // Omit null labels (e.g. a null-filled missing BY label) and empty ones (a label function that emptied a
                // label): Prometheus treats a label with an empty value as absent. This mirrors writeMetricFields on the
                // _timeseries path.
                if (labelBlock.isNull(position)) {
                    continue;
                }
                DataType type = columns.get(i).type();
                if (type == DataType.KEYWORD || type == DataType.TEXT) {
                    BytesRef val = ((BytesRefBlock) labelBlock).getBytesRef(labelBlock.getFirstValueIndex(position), scratch);
                    if (val.length == 0) {
                        continue;
                    }
                    builder.field(columns.get(i).name());
                    builder.utf8Value(val.bytes, val.offset, val.length);
                    continue;
                }
                builder.field(columns.get(i).name());
                writeLabelValue(builder, labelBlock, type, position, zoneId, scratch);
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

    /**
     * Writes a finite/NaN/Infinite sample value without a per-value {@code String} allocation for the common
     * (finite) case. {@code valueBuffer} must be one instance reused across an entire query's result set (created
     * once per response, not per value) — {@link StringBuilder#append(double)} writes digits
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
        int count = sampleCount(valueBlock, stepBlock, position);
        if (count == 0) {
            return null;
        }
        int lastOffset = count - 1;
        double value = valueBlock.getDouble(valueBlock.getFirstValueIndex(position) + lastOffset);
        long stepMillis = stepBlock.getLong(stepBlock.getFirstValueIndex(position) + lastOffset);
        return new Sample(value, stepMillis);
    }

    /**
     * Returns the number of (value, step) samples of a position's collapsed series, {@code 0} for a series with no data
     * in range. Both value and step are multi-valued (one entry per step) due to TimeSeriesCollapse.
     */
    private static int sampleCount(DoubleBlock valueBlock, LongBlock stepBlock, int position) {
        if (valueBlock.isNull(position) || stepBlock.isNull(position)) {
            return 0;
        }
        int count = valueBlock.getValueCount(position);
        int stepCount = stepBlock.getValueCount(position);
        if (count != stepCount) {
            throw new IllegalStateException(
                "PROMQL response has misaligned collapsed step/value columns: step count [" + stepCount + "], value count [" + count + "]"
            );
        }
        return count;
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
    public static double parseTimestamp(long millis) {
        return millis / 1000.0;
    }

    /** The response of a string literal instant query: Prometheus renders it as {@code [<unix_time>, "<string>"]}. */
    static XContentBuilder buildStringResult(Instant time, String value) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.field("status", "success");
        builder.startObject("data");
        builder.field("resultType", "string");
        builder.startArray("result");
        builder.value(parseTimestamp(time.toEpochMilli()));
        builder.value(value);
        builder.endArray();
        builder.endObject(); // data
        builder.endObject(); // root
        return builder;
    }

    /**
     * Formats a sample value for the Prometheus JSON response.
     * Prometheus represents values as strings, with special handling for NaN and Infinity.
     */
    public static String formatSampleValue(double value) {
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
        forEachEntryFlattened(parseLabelsJson(seriesJson, true), (name, value) -> {
            String labelValue = labelValue(value);
            if (labelValue != null) {
                builder.field(name, labelValue);
            }
        });
    }

    /**
     * The Prometheus value of a label with the given value: its string representation, or {@code null} if that is empty, as a label
     * with an empty value is absent in Prometheus.
     */
    @Nullable
    public static String labelValue(Object value) {
        String string = value.toString();
        return string.isEmpty() ? null : string;
    }

    /**
     * Parses a JSON object of labels, such as a {@code _timeseries} value. If {@code removeLabelsPrefix} is set, the {@code labels}
     * namespace is unwrapped: the labels of a Prometheus series are named without its prefix.
     */
    public static Map<String, Object> parseLabelsJson(String labelsJson, boolean removeLabelsPrefix) throws IOException {
        Map<String, Object> root;
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, labelsJson)) {
            root = parser.map();
        }
        if (removeLabelsPrefix == false) {
            return root;
        }
        Map<String, Object> dimensions = new LinkedHashMap<>();
        if (root.remove(LABELS_NAMESPACE) instanceof Map<?, ?> labels) {
            for (Map.Entry<?, ?> label : labels.entrySet()) {
                dimensions.put(label.getKey().toString(), label.getValue());
            }
        }
        dimensions.putAll(root);
        return dimensions;
    }

    /**
     * Calls {@code consumer} with the name and non-null value of each entry of {@code map}, with nested objects flattened into
     * dot-separated names.
     */
    public static void forEachEntryFlattened(Map<?, ?> map, CheckedBiConsumer<String, Object, IOException> consumer) throws IOException {
        forEachEntryFlattened("", map, consumer);
    }

    private static void forEachEntryFlattened(String prefix, Map<?, ?> map, CheckedBiConsumer<String, Object, IOException> consumer)
        throws IOException {
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            String name = prefix + entry.getKey();
            if (entry.getValue() instanceof Map<?, ?> nested) {
                forEachEntryFlattened(name + ".", nested, consumer);
            } else if (entry.getValue() != null) {
                consumer.accept(name, entry.getValue());
            }
        }
    }
}
