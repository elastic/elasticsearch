/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.datapoint;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.HistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.SummaryDataPoint;

import com.google.protobuf.ByteString;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.hash.BufferedMurmur3Hasher;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfBatchBuilder;
import org.elasticsearch.escf.EscfRowBuffer;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.sourcebatch.SourceBatchEncodeHelper;
import org.elasticsearch.sourcebatch.SourceValueType;
import org.elasticsearch.xcontent.XContentString;
import org.elasticsearch.xpack.oteldata.otlp.docbuilder.MappingHints;
import org.elasticsearch.xpack.oteldata.otlp.docbuilder.MetricDocumentBuilder;
import org.elasticsearch.xpack.oteldata.otlp.docbuilder.OTelDocumentBuilder;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.ToIntFunction;

import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MAX_SCALE;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent.BUCKET_COUNTS_FIELD;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent.BUCKET_INDICES_FIELD;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent.MAX_FIELD;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent.MIN_FIELD;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent.NEGATIVE_FIELD;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent.POSITIVE_FIELD;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent.SCALE_FIELD;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent.SUM_FIELD;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent.ZERO_COUNT_FIELD;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent.ZERO_FIELD;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent.ZERO_THRESHOLD_FIELD;

/**
 * Direct protobuf → ESCF converter for OTLP metrics. Converts an {@link ExportMetricsServiceRequest}
 * into {@link EscfBatch}es without an intermediate XContent (cbor/JSON) hop.
 *
 * <p>The converter mirrors the field order and structure of {@link MetricDocumentBuilder#buildMetricDocument}
 * and {@link org.elasticsearch.xpack.oteldata.otlp.docbuilder.OTelDocumentBuilder} but drives
 * {@link EscfRowBuffer} (via {@link EscfBatchBuilder}) instead of an {@link org.elasticsearch.xcontent.XContentBuilder}.
 *
 * <p>This is a unit-test-only POC — it is not wired into
 * {@link org.elasticsearch.xpack.oteldata.otlp.OTLPMetricsTransportAction}.
 * The design is intentionally co-located in the {@code datapoint} package so it can access the
 * package-private {@link TDigestConverter} and {@link RawHistogramConverter}.
 *
 * <p>The {@link Result} carries per-group metadata (tsid, dynamic templates) alongside the finalized
 * {@link EscfBatch}es; close the result to release recycler-backed column buffers.
 */
public final class MetricEscfConverter {

    private MetricEscfConverter() {}

    // Pre-encoded UTF-8 bytes for the small finite set of temporality strings, so
    // String.getBytes(UTF_8) is not called on the hot per-row path.
    private static final Map<String, XContentString.UTF8Bytes> TEMPORALITY_UTF8;
    static {
        Map<String, XContentString.UTF8Bytes> m = new HashMap<>();
        for (String s : new String[] { "cumulative", "delta", "unspecified" }) {
            m.put(s, utf8(s));
        }
        TEMPORALITY_UTF8 = Map.copyOf(m);
    }

    /**
     * Primitive long accumulator; avoids boxing when collecting histogram bucket counts and
     * centroid values from {@link TDigestConverter} / {@link RawHistogramConverter} callbacks.
     * Not thread-safe; allocate per conversion call.
     */
    private static final class LongAccumulator {
        private long[] buf = new long[16];
        private int size = 0;

        void addLong(long v) {
            if (size == buf.length) {
                buf = Arrays.copyOf(buf, buf.length * 2);
            }
            buf[size++] = v;
        }

        void addDouble(double v) {
            addLong(Double.doubleToRawLongBits(v));
        }

        /** The backing buffer; only {@code [0, size)} is meaningful and it may be larger. */
        long[] buffer() {
            return buf;
        }
    }

    /**
     * Per data point group: the tsid, dynamic template assignments, and position within the batch.
     */
    public record GroupResult(
        String targetIndex,
        int shardId,
        int rowIndex,
        BytesRef tsid,
        Map<String, String> dynamicTemplates,
        Map<String, Map<String, String>> dynamicTemplateParams
    ) {}

    /**
     * Conversion output. Call {@link #batch(String, int)} to retrieve the {@link EscfBatch} for a
     * given target index and shard; close the result when done to release column-buffer memory.
     */
    public static final class Result implements Releasable {

        private final List<GroupResult> groups;
        private final Map<String, Map<Integer, EscfBatch>> batchesByIndex;

        private Result(List<GroupResult> groups, Map<String, Map<Integer, EscfBatch>> batchesByIndex) {
            this.groups = groups;
            this.batchesByIndex = batchesByIndex;
        }

        /** Per-group results in the same order as {@link DataPointGroupingContext#consume} emits them. */
        public List<GroupResult> groups() {
            return groups;
        }

        /**
         * The finalized batch for {@code targetIndex} / {@code shardId}, or {@code null} if that
         * (target, shard) combination received no committed rows.
         */
        public EscfBatch batch(String targetIndex, int shardId) {
            Map<Integer, EscfBatch> byShardId = batchesByIndex.get(targetIndex);
            return byShardId == null ? null : byShardId.get(shardId);
        }

        @Override
        public void close() {
            for (Map<Integer, EscfBatch> byShardId : batchesByIndex.values()) {
                for (EscfBatch batch : byShardId.values()) {
                    batch.close();
                }
            }
        }
    }

    /**
     * Converts {@code request} into ESCF batches.
     *
     * <p>The {@code tsidToShard} function maps a tsid bytes-ref to a (zero-based) shard id for
     * partition assignment. Pass {@code t -> 0} to put all rows in one partition.
     *
     * @param defaultMappingHints  effective mapping hints (e.g. {@link MappingHints#DEFAULT_TDIGEST})
     * @param indexVersion         used to compute the tsid bytes
     * @param tsidToShard          maps a tsid to a partition key
     * @return the conversion result; caller is responsible for closing it
     */
    public static Result convert(
        ExportMetricsServiceRequest request,
        MappingHints defaultMappingHints,
        IndexVersion indexVersion,
        ToIntFunction<BytesRef> tsidToShard
    ) throws IOException {
        // Group data points (builds the per-group tsidBuilders via protobuf funnels).
        BufferedMurmur3Hasher hasher = new BufferedMurmur3Hasher(0);
        DataPointGroupingContext ctx = new DataPointGroupingContext(new BufferedByteStringAccessor(), defaultMappingHints);
        ctx.groupDataPoints(request);

        // One EscfBatchBuilder per target index.
        Map<String, EscfBatchBuilder> buildersByIndex = new LinkedHashMap<>();
        // Unique shard IDs per target in insertion order — avoids re-scanning groups at finalization.
        Map<String, Set<Integer>> shardsByIndex = new LinkedHashMap<>();
        List<GroupResult> groups = new ArrayList<>();

        ctx.consume(group -> {
            String targetIndex = group.targetIndex().index();
            EscfBatchBuilder batchBuilder = buildersByIndex.computeIfAbsent(targetIndex, k -> new EscfBatchBuilder());
            shardsByIndex.computeIfAbsent(targetIndex, k -> new LinkedHashSet<>());

            Map<String, String> dynamicTemplates = new HashMap<>();
            Map<String, Map<String, String>> dynamicTemplateParams = new HashMap<>();

            // Build the tsid — add _metric_names_hash dimension exactly as MetricDocumentBuilder does.
            String metricNamesHash = group.getMetricNamesHash(hasher);
            group.tsidBuilder().addStringDimension("_metric_names_hash", metricNamesHash);
            BytesRef tsid = group.tsidBuilder().buildTsid(indexVersion);
            int shardId = tsidToShard.applyAsInt(tsid);
            shardsByIndex.get(targetIndex).add(shardId);

            EscfRowBuffer row = batchBuilder.beginRow();
            buildRow(row, group, metricNamesHash, defaultMappingHints, dynamicTemplates, dynamicTemplateParams);
            int rowIndex = batchBuilder.commit(shardId);

            groups.add(new GroupResult(targetIndex, shardId, rowIndex, tsid, dynamicTemplates, dynamicTemplateParams));
        });

        // Finalize — build one EscfBatch per (targetIndex, shardId) combination.
        // Builders are always closed in the finally block; batches are released on any exception.
        Map<String, Map<Integer, EscfBatch>> batchesByIndex = new LinkedHashMap<>();
        try {
            for (Map.Entry<String, EscfBatchBuilder> entry : buildersByIndex.entrySet()) {
                String targetIndex = entry.getKey();
                EscfBatchBuilder builder = entry.getValue();
                Map<Integer, EscfBatch> byShardId = new LinkedHashMap<>();
                for (int shardId : shardsByIndex.get(targetIndex)) {
                    byShardId.put(shardId, builder.buildPartition(shardId));
                }
                batchesByIndex.put(targetIndex, byShardId);
            }
        } catch (Exception e) {
            for (Map<Integer, EscfBatch> byShardId : batchesByIndex.values()) {
                Releasables.close(byShardId.values());
            }
            throw e;
        } finally {
            Releasables.close(buildersByIndex.values());
        }

        return new Result(groups, batchesByIndex);
    }

    // ---- Row construction (mirrors MetricDocumentBuilder.buildMetricDocument) ----

    private static void buildRow(
        EscfRowBuffer row,
        DataPointGroupingContext.DataPointGroup group,
        String metricNamesHash,
        MappingHints defaultMappingHints,
        Map<String, String> dynamicTemplates,
        Map<String, Map<String, String>> dynamicTemplateParams
    ) throws IOException {
        // @timestamp
        row.longField("@timestamp", TimeUnit.NANOSECONDS.toMillis(group.getTimestampUnixNano()));

        // start_timestamp (optional)
        if (group.getStartTimestampUnixNano() != 0) {
            row.longField("start_timestamp", TimeUnit.NANOSECONDS.toMillis(group.getStartTimestampUnixNano()));
        }

        // resource{}
        buildResource(row, group);

        // data_stream{} (only if target is a data stream)
        buildDataStream(row, group);

        // scope{}
        buildScope(row, group);

        // attributes{} — data-point-level attributes (dropped_attributes_count always 0 for OTLP data points)
        buildAttributeList(row, group.dataPointAttributes(), 0);

        // unit (optional)
        if (group.unit() != null && group.unit().isEmpty() == false) {
            row.stringField(MetricDocumentBuilder.UNIT_FIELD, utf8(group.unit()));
        }

        // temporality (optional)
        String temporality = MetricDocumentBuilder.temporalityToString(group.temporality());
        if (temporality != null) {
            // Look up the pre-encoded bytes; fall back to a fresh encode only for an unknown value.
            // (getOrDefault would evaluate utf8(temporality) unconditionally, defeating the cache.)
            XContentString.UTF8Bytes temporalityUtf8 = TEMPORALITY_UTF8.get(temporality);
            if (temporalityUtf8 == null) {
                temporalityUtf8 = utf8(temporality);
            }
            row.stringField(MetricDocumentBuilder.TEMPORALITY_FIELD, temporalityUtf8);
        }

        // _metric_names_hash
        row.stringField("_metric_names_hash", utf8(metricNamesHash));

        // metrics{name: value, ...}
        long docCount = 0;
        row.startObject("metrics");
        List<DataPoint> dataPoints = group.dataPoints();
        for (DataPoint dataPoint : dataPoints) {
            MappingHints mappingHints = defaultMappingHints.withConfigFromAttributes(dataPoint.getAttributes());
            buildMetricValue(row, dataPoint, mappingHints, dynamicTemplates, dynamicTemplateParams, group.unit());
            if (mappingHints.docCount()) {
                docCount = dataPoint.getDocCount();
            }
        }
        row.endObject();

        // _doc_count (optional)
        if (docCount > 0) {
            row.longField("_doc_count", docCount);
        }
    }

    private static void buildResource(EscfRowBuffer row, DataPointGroupingContext.DataPointGroup group) throws IOException {
        row.startObject("resource");
        addFieldIfNotEmpty(row, "schema_url", group.resourceSchemaUrl());
        buildAttributeList(row, group.resource().getAttributesList(), group.resource().getDroppedAttributesCount());
        row.endObject();
    }

    private static void buildDataStream(EscfRowBuffer row, DataPointGroupingContext.DataPointGroup group) {
        TargetIndex targetIndex = group.targetIndex();
        if (targetIndex.isDataStream() == false) {
            return;
        }
        row.startObject("data_stream");
        row.stringField("type", utf8(targetIndex.type()));
        row.stringField("dataset", utf8(targetIndex.dataset()));
        row.stringField("namespace", utf8(targetIndex.namespace()));
        row.endObject();
    }

    private static void buildScope(EscfRowBuffer row, DataPointGroupingContext.DataPointGroup group) throws IOException {
        row.startObject("scope");
        addFieldIfNotEmpty(row, "schema_url", group.scopeSchemaUrl());
        addFieldIfNotEmpty(row, "name", group.scope().getNameBytes());
        addFieldIfNotEmpty(row, "version", group.scope().getVersionBytes());
        buildAttributeList(row, group.scope().getAttributesList(), group.scope().getDroppedAttributesCount());
        row.endObject();
    }

    /**
     * Writes the attributes sub-object, mirroring
     * {@link org.elasticsearch.xpack.oteldata.otlp.docbuilder.OTelDocumentBuilder#buildAttributes}.
     * Geo-location attribute merging is intentionally skipped for metrics (same as MetricDocumentBuilder).
     */
    private static void buildAttributeList(EscfRowBuffer row, List<KeyValue> attributes, int droppedAttributesCount) throws IOException {
        if (droppedAttributesCount > 0) {
            row.longField("dropped_attributes_count", droppedAttributesCount);
        }
        // Determine whether any non-ignored attributes will be written, so we can use emptyObject
        // when the attributes object would otherwise be empty. EscfRowBuffer.startObject/endObject
        // with no children does not emit any leaf, so we must use emptyObject to preserve the field.
        boolean hasNonIgnoredAttribute = false;
        for (KeyValue attribute : attributes) {
            if (OTelDocumentBuilder.isIgnoredAttribute(attribute.getKey()) == false) {
                hasNonIgnoredAttribute = true;
                break;
            }
        }
        if (hasNonIgnoredAttribute) {
            row.startObject("attributes");
            for (KeyValue attribute : attributes) {
                String key = attribute.getKey();
                if (OTelDocumentBuilder.isIgnoredAttribute(key) == false) {
                    anyValueToRow(row, key, attribute.getValue());
                }
            }
            row.endObject();
        } else {
            row.emptyObject("attributes");
        }
    }

    /**
     * Translates an {@link AnyValue} to an {@link EscfRowBuffer} field, mirroring
     * {@link org.elasticsearch.xpack.oteldata.otlp.docbuilder.OTelDocumentBuilder#buildAnyValue}.
     */
    private static void anyValueToRow(EscfRowBuffer row, String fieldName, AnyValue value) throws IOException {
        switch (value.getValueCase()) {
            case STRING_VALUE -> row.stringField(fieldName, new XContentString.UTF8Bytes(value.getStringValueBytes().toByteArray()));
            case BOOL_VALUE -> row.booleanField(fieldName, value.getBoolValue());
            case INT_VALUE -> row.longField(fieldName, value.getIntValue());
            case DOUBLE_VALUE -> row.doubleField(fieldName, value.getDoubleValue());
            case BYTES_VALUE -> {
                // Encode binary as base64 to match the JSON round-trip representation.
                byte[] base64Bytes = Base64.getEncoder().encode(value.getBytesValue().toByteArray());
                row.stringField(fieldName, new XContentString.UTF8Bytes(base64Bytes));
            }
            case ARRAY_VALUE -> {
                List<AnyValue> elements = value.getArrayValue().getValuesList();
                PackedAnyArray arr = packAndClassifyAnyValueArray(elements);
                row.arrayField(fieldName, arr.arrayType(), arr.packed());
            }
            case KVLIST_VALUE -> {
                List<KeyValue> kvList = value.getKvlistValue().getValuesList();
                if (kvList.isEmpty()) {
                    row.emptyObject(fieldName);
                } else {
                    row.startObject(fieldName);
                    for (KeyValue kv : kvList) {
                        anyValueToRow(row, kv.getKey(), kv.getValue());
                    }
                    row.endObject();
                }
            }
            case VALUE_NOT_SET -> row.nullField(fieldName);
        }
    }

    /**
     * Result of {@link #packAndClassifyAnyValueArray}: the packed inline-array bytes and the
     * corresponding array-type byte ({@link SourceValueType#FIXED_ARRAY} or
     * {@link SourceValueType#UNION_ARRAY}).
     */
    private record PackedAnyArray(byte[] packed, byte arrayType) {}

    /**
     * Packs a list of {@link AnyValue} elements into inline array bytes in a single pass,
     * returning both the packed bytes and the corresponding array-type byte. Uses FIXED_ARRAY
     * when all elements share the same scalar type, UNION_ARRAY otherwise.
     */
    private static PackedAnyArray packAndClassifyAnyValueArray(List<AnyValue> elements) throws IOException {
        int n = elements.size();
        if (n == 0) {
            byte[] empty = SourceBatchEncodeHelper.packUnionArray(new byte[0], new long[0], new Object[0], 0);
            return new PackedAnyArray(empty, SourceValueType.UNION_ARRAY);
        }
        byte[] types = new byte[n];
        long[] numerics = new long[n];
        Object[] vars = new Object[n];
        boolean forceUnion = false;
        for (int i = 0; i < n; i++) {
            AnyValue elem = elements.get(i);
            switch (elem.getValueCase()) {
                case STRING_VALUE -> {
                    types[i] = SourceValueType.STRING;
                    vars[i] = new XContentString.UTF8Bytes(elem.getStringValueBytes().toByteArray());
                }
                case BOOL_VALUE -> types[i] = elem.getBoolValue() ? SourceValueType.TRUE : SourceValueType.FALSE;
                case INT_VALUE -> {
                    long v = elem.getIntValue();
                    types[i] = (v >= Integer.MIN_VALUE && v <= Integer.MAX_VALUE) ? SourceValueType.INT : SourceValueType.LONG;
                    numerics[i] = v;
                }
                case DOUBLE_VALUE -> {
                    double v = elem.getDoubleValue();
                    float fv = (float) v;
                    types[i] = ((double) fv == v) ? SourceValueType.FLOAT : SourceValueType.DOUBLE;
                    numerics[i] = Double.doubleToRawLongBits(v);
                }
                case BYTES_VALUE -> {
                    types[i] = SourceValueType.STRING;
                    vars[i] = new XContentString.UTF8Bytes(Base64.getEncoder().encode(elem.getBytesValue().toByteArray()));
                }
                default -> {
                    // KVLIST_VALUE, ARRAY_VALUE, VALUE_NOT_SET: store as null, force union
                    types[i] = SourceValueType.NULL;
                    forceUnion = true;
                }
            }
            if (i > 0 && types[i] != types[0]) {
                forceUnion = true;
            }
        }
        if (forceUnion == false) {
            byte sharedType = types[0];
            // Only use FIXED_ARRAY for types that have a non-zero element payload.
            boolean hasData = switch (sharedType) {
                case SourceValueType.STRING, SourceValueType.INT, SourceValueType.LONG, SourceValueType.FLOAT, SourceValueType.DOUBLE ->
                    true;
                default -> false;
            };
            if (hasData) {
                return new PackedAnyArray(
                    SourceBatchEncodeHelper.packFixedArray(sharedType, numerics, vars, n),
                    SourceValueType.FIXED_ARRAY
                );
            }
        }
        return new PackedAnyArray(SourceBatchEncodeHelper.packUnionArray(types, numerics, vars, n), SourceValueType.UNION_ARRAY);
    }

    private static void buildMetricValue(
        EscfRowBuffer row,
        DataPoint dataPoint,
        MappingHints mappingHints,
        Map<String, String> dynamicTemplates,
        Map<String, Map<String, String>> dynamicTemplateParams,
        String unit
    ) throws IOException {
        String metricName = dataPoint.getMetricName();
        String metricFieldPath = "metrics." + metricName;

        if (dataPoint instanceof DataPoint.Number number) {
            buildNumberToRow(row, number, metricName);
        } else if (dataPoint instanceof DataPoint.ExponentialHistogram expHistogram) {
            buildExponentialHistogramToRow(row, expHistogram, metricName, mappingHints);
        } else if (dataPoint instanceof DataPoint.Histogram histogram) {
            buildHistogramToRow(row, histogram, metricName, mappingHints);
        } else if (dataPoint instanceof DataPoint.Summary summary) {
            buildSummaryToRow(row, summary, metricName);
        } else {
            throw new IllegalArgumentException("Unsupported DataPoint type: " + dataPoint.getClass());
        }

        // Track dynamic templates (mirrors MetricDocumentBuilder)
        String dynamicTemplate = dataPoint.getDynamicTemplate(mappingHints);
        if (dynamicTemplate != null) {
            dynamicTemplates.put(metricFieldPath, dynamicTemplate);
            if (unit != null && unit.isEmpty() == false) {
                dynamicTemplateParams.put(metricFieldPath, Map.of(MetricDocumentBuilder.UNIT_FIELD, unit));
            }
        }
    }

    private static void buildNumberToRow(EscfRowBuffer row, DataPoint.Number number, String metricName) {
        NumberDataPoint dp = number.dataPoint();
        switch (dp.getValueCase()) {
            case AS_DOUBLE -> row.doubleField(metricName, dp.getAsDouble());
            case AS_INT -> row.longField(metricName, dp.getAsInt());
            case VALUE_NOT_SET -> throw new IllegalStateException(
                "number data point without a value should have been filtered out: " + metricName
            );
        }
    }

    private static void buildExponentialHistogramToRow(
        EscfRowBuffer row,
        DataPoint.ExponentialHistogram expHistogram,
        String metricName,
        MappingHints mappingHints
    ) throws IOException {
        ExponentialHistogramDataPoint dp = expHistogram.dataPoint();
        switch (mappingHints.histogramMapping()) {
            case AGGREGATE_METRIC_DOUBLE -> buildAggregateMetricDoubleToRow(row, metricName, dp.getSum(), dp.getCount());
            case TDIGEST -> buildTDigestFromExpHistToRow(row, metricName, dp);
            case HISTOGRAM_RAW -> buildRawHistogramFromExpHistToRow(row, metricName, dp);
            case EXPONENTIAL_HISTOGRAM -> buildNativeExponentialHistogramToRow(row, metricName, dp);
        }
    }

    private static void buildHistogramToRow(EscfRowBuffer row, DataPoint.Histogram histogram, String metricName, MappingHints mappingHints)
        throws IOException {
        HistogramDataPoint dp = histogram.dataPoint();
        switch (mappingHints.histogramMapping()) {
            case AGGREGATE_METRIC_DOUBLE -> buildAggregateMetricDoubleToRow(row, metricName, dp.getSum(), dp.getCount());
            case TDIGEST -> buildTDigestFromHistToRow(row, metricName, dp);
            case HISTOGRAM_RAW -> buildRawHistogramFromHistToRow(row, metricName, dp);
            case EXPONENTIAL_HISTOGRAM -> buildExponentialFromHistToRow(row, metricName, histogram);
        }
    }

    private static void buildSummaryToRow(EscfRowBuffer row, DataPoint.Summary summary, String metricName) {
        SummaryDataPoint dp = summary.dataPoint();
        buildAggregateMetricDoubleToRow(row, metricName, dp.getSum(), dp.getCount());
    }

    private static void buildAggregateMetricDoubleToRow(EscfRowBuffer row, String metricName, double sum, long valueCount) {
        row.startObject(metricName);
        row.doubleField("sum", sum);
        row.longField("value_count", valueCount);
        row.endObject();
    }

    private static void buildTDigestFromExpHistToRow(EscfRowBuffer row, String metricName, ExponentialHistogramDataPoint dp)
        throws IOException {
        LongAccumulator counts = new LongAccumulator();
        LongAccumulator values = new LongAccumulator();
        TDigestConverter.counts(dp, counts::addLong);
        TDigestConverter.centroidValues(dp, values::addDouble);
        row.startObject(metricName);
        row.longArrayField("counts", counts.buffer(), counts.size);
        row.doubleArrayField("values", values.buffer(), values.size);
        row.endObject();
    }

    private static void buildTDigestFromHistToRow(EscfRowBuffer row, String metricName, HistogramDataPoint dp) throws IOException {
        LongAccumulator counts = new LongAccumulator();
        LongAccumulator values = new LongAccumulator();
        TDigestConverter.counts(dp, counts::addLong);
        TDigestConverter.centroidValues(dp, values::addDouble);
        row.startObject(metricName);
        row.longArrayField("counts", counts.buffer(), counts.size);
        row.doubleArrayField("values", values.buffer(), values.size);
        row.endObject();
    }

    private static void buildRawHistogramFromExpHistToRow(EscfRowBuffer row, String metricName, ExponentialHistogramDataPoint dp)
        throws IOException {
        LongAccumulator counts = new LongAccumulator();
        LongAccumulator values = new LongAccumulator();
        RawHistogramConverter.counts(dp, counts::addLong);
        RawHistogramConverter.values(dp, values::addDouble);
        row.startObject(metricName);
        row.longArrayField("counts", counts.buffer(), counts.size);
        row.doubleArrayField("values", values.buffer(), values.size);
        row.endObject();
    }

    private static void buildRawHistogramFromHistToRow(EscfRowBuffer row, String metricName, HistogramDataPoint dp) throws IOException {
        LongAccumulator counts = new LongAccumulator();
        LongAccumulator values = new LongAccumulator();
        RawHistogramConverter.counts(dp, counts::addLong);
        RawHistogramConverter.values(dp, values::addDouble);
        row.startObject(metricName);
        row.longArrayField("counts", counts.buffer(), counts.size);
        row.doubleArrayField("values", values.buffer(), values.size);
        row.endObject();
    }

    /**
     * Writes a native OTLP exponential histogram as ESCF sub-fields, mirroring
     * {@link ExponentialHistogramConverter#buildExponentialHistogram(ExponentialHistogramDataPoint, org.elasticsearch.xcontent.XContentBuilder)}.
     */
    private static void buildNativeExponentialHistogramToRow(EscfRowBuffer row, String metricName, ExponentialHistogramDataPoint dp) {
        row.startObject(metricName);
        row.longField(SCALE_FIELD, dp.getScale());
        if (dp.getZeroCount() > 0) {
            row.startObject(ZERO_FIELD);
            row.longField(ZERO_COUNT_FIELD, dp.getZeroCount());
            if (dp.getZeroThreshold() != 0) {
                row.doubleField(ZERO_THRESHOLD_FIELD, dp.getZeroThreshold());
            }
            row.endObject();
        }
        if (dp.hasNegative()) {
            writeExponentialBucketsToRow(row, NEGATIVE_FIELD, dp.getNegative());
        }
        if (dp.hasPositive()) {
            writeExponentialBucketsToRow(row, POSITIVE_FIELD, dp.getPositive());
        }
        if (dp.hasSum()) {
            row.doubleField(SUM_FIELD, dp.getSum());
        }
        if (dp.hasMin()) {
            row.doubleField(MIN_FIELD, dp.getMin());
        }
        if (dp.hasMax()) {
            row.doubleField(MAX_FIELD, dp.getMax());
        }
        row.endObject();
    }

    private static void writeExponentialBucketsToRow(EscfRowBuffer row, String fieldName, ExponentialHistogramDataPoint.Buckets buckets) {
        int n = buckets.getBucketCountsCount();
        LongAccumulator indices = new LongAccumulator();
        LongAccumulator counts = new LongAccumulator();
        for (int i = 0; i < n; i++) {
            long count = buckets.getBucketCounts(i);
            if (count != 0) {
                indices.addLong(buckets.getOffset() + i);
                counts.addLong(count);
            }
        }
        if (indices.size == 0) {
            return;
        }
        row.startObject(fieldName);
        row.longArrayField(BUCKET_INDICES_FIELD, indices.buffer(), indices.size);
        row.longArrayField(BUCKET_COUNTS_FIELD, counts.buffer(), counts.size);
        row.endObject();
    }

    /**
     * Writes an explicit-bucket {@link HistogramDataPoint} as a native exponential_histogram, mirroring
     * {@link ExponentialHistogramConverter#buildExponentialHistogram(HistogramDataPoint, io.opentelemetry.proto.metrics.v1.AggregationTemporality, org.elasticsearch.xcontent.XContentBuilder, ExponentialHistogramConverter.BucketBuffer)}.
     * The re-bucketing algorithm is shared via {@link ExponentialHistogramConverter#computeBuckets}; only
     * the emit differs (columnar rows here vs. XContent there).
     */
    private static void buildExponentialFromHistToRow(EscfRowBuffer row, String metricName, DataPoint.Histogram histogram) {
        HistogramDataPoint dp = histogram.dataPoint();
        ExponentialHistogramConverter.BucketBuffer scratch = new ExponentialHistogramConverter.BucketBuffer();
        row.startObject(metricName);
        row.longField(SCALE_FIELD, MAX_SCALE);
        if (ExponentialHistogramConverter.computeBuckets(dp, histogram.metric().getHistogram().getAggregationTemporality(), scratch)) {
            writeComputedExponentialBucketsToRow(row, scratch);
            if (dp.hasSum()) {
                row.doubleField(SUM_FIELD, dp.getSum());
            }
            if (dp.hasMin()) {
                row.doubleField(MIN_FIELD, dp.getMin());
            }
            if (dp.hasMax()) {
                row.doubleField(MAX_FIELD, dp.getMax());
            }
        }
        row.endObject();
    }

    /** Emits the zero/negative/positive buckets from {@code b} in the same order as {@code BucketBuffer.writeBuckets}. */
    private static void writeComputedExponentialBucketsToRow(EscfRowBuffer row, ExponentialHistogramConverter.BucketBuffer b) {
        if (b.zeroCount() > 0) {
            row.startObject(ZERO_FIELD);
            row.longField(ZERO_COUNT_FIELD, b.zeroCount());
            row.endObject();
        }
        int negative = b.negativeSize();
        if (negative > 0) {
            // Negatives are stored highest-to-lowest index; emit in inverse to get lowest-to-highest.
            long[] indices = new long[negative];
            long[] counts = new long[negative];
            for (int i = 0; i < negative; i++) {
                indices[i] = b.negativeIndex(negative - 1 - i);
                counts[i] = b.negativeCount(negative - 1 - i);
            }
            row.startObject(NEGATIVE_FIELD);
            row.longArrayField(BUCKET_INDICES_FIELD, indices, negative);
            row.longArrayField(BUCKET_COUNTS_FIELD, counts, negative);
            row.endObject();
        }
        int positive = b.positiveSize();
        if (positive > 0) {
            long[] indices = new long[positive];
            long[] counts = new long[positive];
            for (int i = 0; i < positive; i++) {
                indices[i] = b.positiveIndex(i);
                counts[i] = b.positiveCount(i);
            }
            row.startObject(POSITIVE_FIELD);
            row.longArrayField(BUCKET_INDICES_FIELD, indices, positive);
            row.longArrayField(BUCKET_COUNTS_FIELD, counts, positive);
            row.endObject();
        }
    }

    private static XContentString.UTF8Bytes utf8(String s) {
        return new XContentString.UTF8Bytes(s.getBytes(StandardCharsets.UTF_8));
    }

    private static void addFieldIfNotEmpty(EscfRowBuffer row, String fieldName, ByteString byteString) {
        if (byteString != null && byteString.isEmpty() == false) {
            row.stringField(fieldName, new XContentString.UTF8Bytes(byteString.toByteArray()));
        }
    }
}
