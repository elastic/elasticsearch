/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.RamUsageEstimates;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.util.LongLongHash;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Operator for the ESQL {@code TS_INFO} command.
 * <p>
 * Returns one row per (metric, time-series) combination — more fine-grained than
 * {@link MetricsInfoOperator} which aggregates across time series. Includes all columns
 * from {@code METRICS_INFO} plus a {@code dimensions} column containing a JSON-encoded
 * representation of dimension key/values.
 * <p>
 * Operates in two modes (INITIAL/FINAL), mirroring the pattern used by {@link MetricsInfoOperator}.
 * <p>
 * INITIAL emits its buffered series early whenever their tracked memory reaches the partial emit threshold.
 * The same series can then appear in several INITIAL pages. FINAL merges those rows again, so it must buffer
 * everything until it has seen all of its input.
 * <p>
 * Both modes emit their rows in pages of roughly the target page size rather than in one page.
 *
 * <h2>Output columns (both modes)</h2>
 * <ul>
 *   <li>{@code metric_name} – keyword (single-valued)</li>
 *   <li>{@code data_stream} – keyword (multi-valued); data stream or standalone index names</li>
 *   <li>{@code unit} – keyword (multi-valued when backing indices differ; may be null)</li>
 *   <li>{@code metric_type} – keyword (multi-valued when definitions differ)</li>
 *   <li>{@code field_type} – keyword (multi-valued when definitions differ)</li>
 *   <li>{@code dimension_fields} – keyword (multi-valued); union of dimension keys</li>
 *   <li>{@code dimensions} – keyword (single-valued); JSON object of dimension key/values</li>
 * </ul>
 *
 * <h2>State and memory</h2>
 * State is keyed by series: one metric with one set of dimension values. FINAL can hold about a million series, so
 * the keys are not Java objects. See {@link SeriesTable}. Each series records the data streams and standalone indices
 * it was seen in, with the metric's metadata in each. Output combines the index abstractions of a series that share
 * metadata, one series at a time, so no second full-size structure is built.
 * <p>
 * FINAL keeps one canonical copy of each index abstraction name and metadata list in {@link MetadataValues}, because
 * those repeat across most of its series. INITIAL holds at most one batch of the partial emit threshold, so it does not
 * pool them. Its metadata lists reference the strings of the mappings, which the operator does not own.
 */
public class TsInfoOperator implements Operator {

    public static final int NUM_BLOCKS = 7;
    public static final int NO_LIMIT = Integer.MAX_VALUE;

    /**
     * Default tracked memory at which INITIAL emits its buffered rows early. A larger value merges more rows
     * on the data node, at the cost of more data node memory per driver.
     */
    public static final ByteSizeValue DEFAULT_PARTIAL_EMIT_THRESHOLD = ByteSizeValue.ofMb(4);

    /** FINAL must see every row of a key before merging, so it never emits early. */
    private static final long NEVER_EMIT_PARTIAL = Long.MAX_VALUE;

    /** The value of {@code cursor} while the operator is not emitting. */
    private static final long NOT_EMITTING = -1;

    private static final BytesRef NO_DIMENSIONS_JSON = new BytesRef("{}");

    /**
     * The series' metric metadata in one data stream or standalone index. The lists are sorted and immutable, so they
     * are replaced, never mutated. In FINAL the name and lists come from {@link MetadataValues}.
     */
    private static final class IndexAbstractionMetadata {
        final String name;
        List<String> units = List.of();
        List<String> fieldTypes = List.of();
        List<String> metricTypes = List.of();
        /** Bytes charged for the name and lists this entry owns. Always zero in FINAL, which pools them. */
        long ownedBytes;

        IndexAbstractionMetadata(String name) {
            this.name = name;
        }
    }

    /** Index abstractions of one series with equal metadata produce one output row. */
    private record FieldMetadata(List<String> units, List<String> fieldTypes, List<String> metricTypes) {}

    /** The index abstraction list of a new series, with capacity one. Its slot in {@link SeriesTable} is charged there. */
    private static final long SERIES_BYTES = RamUsageEstimates.ARRAY_LIST_SHALLOW_SIZE + RamUsageEstimator.alignObjectSize(
        RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + RamUsageEstimator.NUM_BYTES_OBJECT_REF
    );

    /** An {@link ArrayList} grows by half its capacity, so two slots per element cover its backing array. */
    private static final long INDEX_ABSTRACTION_BYTES = RamUsageEstimator.shallowSizeOfInstance(IndexAbstractionMetadata.class) + 2L
        * RamUsageEstimator.NUM_BYTES_OBJECT_REF;

    /** Column indices in the 7-column output. */
    private static final int COL_METRIC_NAME = 0;
    private static final int COL_DATA_STREAM = 1;
    private static final int COL_UNIT = 2;
    private static final int COL_METRIC_TYPE = 3;
    private static final int COL_FIELD_TYPE = 4;
    private static final int COL_DIMENSION_FIELDS = 5;
    private static final int COL_DIMENSIONS = 6;

    /**
     * Rows vary widely in size, so the row count of a page is unknown up front.
     * Builders start at this many positions and grow.
     */
    private static final int INITIAL_PAGE_POSITIONS = 128;

    /**
     * Factory for INITIAL mode (data nodes).
     *
     * @param fieldLookup          on-demand lookup for metric field metadata
     * @param dataStreamsByIndex   backing index names mapped to their parent data streams, including cluster qualifiers
     * @param metadataSourceChannel channel index for {@code _timeseries_metadata} block
     * @param indexChannel          channel index for {@code _index} block
     * @param partialEmitThreshold  tracked memory of buffered rows at which they are emitted early
     * @param targetPageSize        estimated block memory at which an output page is closed
     */
    public record Factory(
        MetricsInfoOperator.MetricFieldLookup fieldLookup,
        Map<String, String> dataStreamsByIndex,
        int metadataSourceChannel,
        int indexChannel,
        ByteSizeValue partialEmitThreshold,
        ByteSizeValue targetPageSize
    ) implements OperatorFactory {
        public Factory {
            requirePositive("partialEmitThreshold", partialEmitThreshold);
            requirePositive("targetPageSize", targetPageSize);
        }

        @Override
        public Operator get(DriverContext driverContext) {
            return new TsInfoOperator(
                Mode.INITIAL,
                driverContext,
                fieldLookup,
                dataStreamsByIndex,
                metadataSourceChannel,
                indexChannel,
                null,
                NO_LIMIT,
                partialEmitThreshold.getBytes(),
                targetPageSize.getBytes()
            );
        }

        @Override
        public String describe() {
            return "TsInfoOperator[mode=INITIAL, metadataSourceChannel="
                + metadataSourceChannel
                + ", indexChannel="
                + indexChannel
                + ", partialEmitThreshold="
                + partialEmitThreshold
                + "]";
        }
    }

    /**
     * Factory for FINAL mode (coordinator): merges 7-column pages from multiple data nodes.
     *
     * @param channels       the 7 input channel indices for
     *                       [metric_name, data_stream, unit, metric_type, field_type, dimension_fields, dimensions]
     * @param targetPageSize estimated block memory at which an output page is closed
     */
    public record FinalFactory(int[] channels, int limit, ByteSizeValue targetPageSize) implements OperatorFactory {
        public FinalFactory {
            requirePositive("targetPageSize", targetPageSize);
        }

        @Override
        public Operator get(DriverContext driverContext) {
            return new TsInfoOperator(
                Mode.FINAL,
                driverContext,
                null,
                Map.of(),
                -1,
                -1,
                channels,
                limit,
                NEVER_EMIT_PARTIAL,
                targetPageSize.getBytes()
            );
        }

        @Override
        public String describe() {
            return "TsInfoOperator[mode=FINAL" + (limit == NO_LIMIT ? "" : ", limit=" + limit) + "]";
        }
    }

    private static void requirePositive(String name, ByteSizeValue value) {
        if (value.getBytes() <= 0) {
            throw new IllegalArgumentException(name + " must be positive; got [" + value + "]");
        }
    }

    public enum Mode {
        INITIAL,
        FINAL
    }

    /**
     * The series of one batch. Metric names and dimension JSON are stored once each in a {@link BytesRefHash}, and a
     * series is the pair of their ids in a {@link LongLongHash}. Ids are dense and in insertion order, so per-series
     * and per-dimensions state lives in arrays indexed by id. Everything here is allocated through {@link BigArrays},
     * which charges the breaker. Owned by the operator and used from its driver thread only. The operator closes it.
     */
    private static final class SeriesTable implements Releasable, Accountable {
        /** The hashes grow before an insert once full, even for a known key, so a tiny table would grow on lookups. */
        private static final int INITIAL_HASH_CAPACITY = 16;

        private final BigArrays bigArrays;
        private final BytesRefHash metricNames;
        private final BytesRefHash dimensions;
        /** (metric name id, dimensions id) to series id. */
        private final LongLongHash series;
        /** The index abstractions of each series. Cleared once the series is emitted. */
        private ObjectArray<List<IndexAbstractionMetadata>> indexAbstractions;
        /** The dimension field keys of each dimensions id. They follow from the dimension JSON, so series share them. */
        private ObjectArray<List<String>> dimensionFieldKeys;

        SeriesTable(BigArrays bigArrays) {
            this.bigArrays = bigArrays;
            BytesRefHash metricNames = null;
            BytesRefHash dimensions = null;
            LongLongHash series = null;
            ObjectArray<List<IndexAbstractionMetadata>> indexAbstractions = null;
            boolean success = false;
            try {
                metricNames = new BytesRefHash(INITIAL_HASH_CAPACITY, bigArrays);
                dimensions = new BytesRefHash(INITIAL_HASH_CAPACITY, bigArrays);
                series = new LongLongHash(INITIAL_HASH_CAPACITY, bigArrays);
                indexAbstractions = bigArrays.newObjectArray(1);
                this.dimensionFieldKeys = bigArrays.newObjectArray(1);
                success = true;
            } finally {
                if (success == false) {
                    Releasables.close(metricNames, dimensions, series, indexAbstractions);
                }
            }
            this.metricNames = metricNames;
            this.dimensions = dimensions;
            this.series = series;
            this.indexAbstractions = indexAbstractions;
        }

        long size() {
            return series.size();
        }

        /** Returns the id of {@code json}, or {@code -1 - id} if it was already known. */
        long addDimensions(BytesRef json) {
            long id = dimensions.add(json);
            if (id >= 0) {
                dimensionFieldKeys = bigArrays.grow(dimensionFieldKeys, id + 1);
            }
            return id;
        }

        /** Returns the id of the series, or {@code -1 - id} if it was already known. */
        long addSeries(BytesRef metricName, long dimensionsId) {
            long id = series.add(hashId(metricNames.add(metricName)), dimensionsId);
            if (id >= 0) {
                indexAbstractions = bigArrays.grow(indexAbstractions, id + 1);
            }
            return id;
        }

        /** Returns the id of an existing series, or -1. */
        long findSeries(BytesRef metricName, BytesRef dimensionsJson) {
            long metricNameId = metricNames.find(metricName);
            long dimensionsId = dimensions.find(dimensionsJson);
            return metricNameId < 0 || dimensionsId < 0 ? -1 : series.find(metricNameId, dimensionsId);
        }

        long dimensionsId(long seriesId) {
            return series.getKey2(seriesId);
        }

        BytesRef metricName(long seriesId, BytesRef scratch) {
            return metricNames.get(series.getKey1(seriesId), scratch);
        }

        BytesRef dimensionsJson(long seriesId, BytesRef scratch) {
            return dimensions.get(dimensionsId(seriesId), scratch);
        }

        @Override
        public long ramBytesUsed() {
            return metricNames.ramBytesUsed() + dimensions.ramBytesUsed() + series.ramBytesUsed() + indexAbstractions.ramBytesUsed()
                + dimensionFieldKeys.ramBytesUsed();
        }

        @Override
        public void close() {
            Releasables.close(metricNames, dimensions, series, indexAbstractions, dimensionFieldKeys);
        }

        private static long hashId(long id) {
            return id < 0 ? -1 - id : id;
        }
    }

    /** Null until the first value of a batch arrives, and again once the batch is emitted. */
    private SeriesTable table;
    /** FINAL only: index abstraction names and metadata lists, shared by all series. INITIAL does not use it. */
    private MetadataValues values;
    private int entriesAccumulated;

    private final Mode mode;
    private final BlockFactory blockFactory;
    private final BigArrays bigArrays;
    private final CircuitBreaker breaker;
    /** Bytes charged for the objects of the series, beyond what {@link #table} and {@link #values} charge. */
    private long trackedBytes;

    /** Fields used only in INITIAL mode. */
    private final MetricsInfoOperator.MetricFieldLookup fieldLookup;
    private final Map<String, String> dataStreamsByIndex;
    private final int metadataSourceChannel;
    private final int indexChannel;
    /** FINAL-mode field: input channel indices for the 7 output columns. Null in INITIAL mode. */
    private final int[] finalChannels;
    /** Maximum number of distinct series. Only FINAL mode sets a limit. */
    private final int limit;
    private final long partialEmitThresholdBytes;
    private final long targetPageBytes;

    /**
     * The id of the next series to emit, or {@link #NOT_EMITTING}. While emitting the operator takes no input, so the
     * memory tracked for these series is not mixed with memory for new ones.
     */
    private long cursor = NOT_EMITTING;

    private boolean finished = false;
    private boolean finalOutputStarted = false;

    private int pagesReceived;
    private long rowsReceived;
    private long rowsEmitted;

    private TsInfoOperator(
        Mode mode,
        DriverContext driverContext,
        MetricsInfoOperator.MetricFieldLookup fieldLookup,
        Map<String, String> dataStreamsByIndex,
        int metadataSourceChannel,
        int indexChannel,
        int[] channels,
        int limit,
        long partialEmitThresholdBytes,
        long targetPageBytes
    ) {
        this.mode = mode;
        this.blockFactory = driverContext.blockFactory();
        this.bigArrays = driverContext.bigArrays();
        this.breaker = blockFactory.breaker();
        this.fieldLookup = fieldLookup;
        this.dataStreamsByIndex = dataStreamsByIndex;
        this.metadataSourceChannel = metadataSourceChannel;
        this.indexChannel = indexChannel;
        this.finalChannels = channels;
        this.limit = limit;
        this.partialEmitThresholdBytes = partialEmitThresholdBytes;
        this.targetPageBytes = targetPageBytes;
        this.values = new MetadataValues(breaker, "TsInfoOperator");
    }

    @Override
    public boolean needsInput() {
        return finished == false && cursor == NOT_EMITTING && partialEmitThresholdReached() == false;
    }

    private boolean partialEmitThresholdReached() {
        return seriesCount() > 0 && trackedBytes + values.trackedBytes() + table.ramBytesUsed() >= partialEmitThresholdBytes;
    }

    private long seriesCount() {
        return table == null ? 0 : table.size();
    }

    private SeriesTable table() {
        if (table == null) {
            table = new SeriesTable(bigArrays);
        }
        return table;
    }

    @Override
    public void addInput(Page page) {
        pagesReceived++;
        rowsReceived += page.getPositionCount();
        if (mode == Mode.FINAL) {
            addInputFinal(page);
        } else {
            addInputInitial(page);
        }
    }

    /** INITIAL mode: extract per-time-series metric metadata from _timeseries_metadata and _index blocks. */
    private void addInputInitial(Page page) {
        try {
            BytesRefBlock metadataSource = page.getBlock(metadataSourceChannel);
            BytesRefBlock indexBlock = page.getBlock(indexChannel);

            BytesRef indexScratch = new BytesRef();
            BytesRef sourceScratch = new BytesRef();

            for (int p = 0; p < page.getPositionCount(); p++) {
                if (metadataSource.isNull(p)) {
                    continue;
                }
                if (indexBlock.isNull(p)) {
                    continue;
                }

                String indexName = indexBlock.getBytesRef(p, indexScratch).utf8ToString();
                String indexAbstractionName = dataStreamsByIndex.getOrDefault(indexName, indexName);
                Map<String, Object> metadata = parseMetadataSource(metadataSource, p, sourceScratch);
                if (metadata == null) {
                    continue;
                }

                NavigableMap<String, String> dimensionKeyValues = new TreeMap<>(); // sorted for deterministic JSON
                collectFields(metadata, null, indexName, indexAbstractionName, dimensionKeyValues);
            }
        } finally {
            page.releaseBlocks();
        }
    }

    /**
     * Recursively walks the parsed {@code _timeseries_metadata} JSON in two passes:
     * <ol>
     *   <li>First pass: classifies each leaf — metric fields are skipped (but their keys
     *       are noted), non-metric leaves are collected as dimension key-values.</li>
     *   <li>Second pass (at the root level): records each metric in its series,
     *       keyed by (metricName, dimensionsJson), under indexAbstractionName.</li>
     * </ol>
     * The field lookup is checked <em>before</em> inspecting the value type so that nested
     * metric types (histogram, exponential_histogram, tdigest) are not recursed into.
     */
    @SuppressWarnings("unchecked")
    private void collectFields(
        Map<String, Object> metadata,
        String prefix,
        String indexName,
        String indexAbstractionName,
        NavigableMap<String, String> dimensionKeyValues
    ) {
        for (Map.Entry<String, Object> entry : metadata.entrySet()) {
            String key = prefix == null ? entry.getKey() : prefix + "." + entry.getKey();
            Object value = entry.getValue();

            MetricFieldInfo fieldInfo = fieldLookup.lookup(indexName, key);
            if (fieldInfo == null) {
                if (value instanceof Map<?, ?> nested) {
                    collectFields((Map<String, Object>) nested, key, indexName, indexAbstractionName, dimensionKeyValues);
                } else {
                    dimensionKeyValues.put(key, value == null ? null : value.toString());
                }
            }
        }

        if (prefix == null) {
            long dimensionsId = initialDimensionsId(buildDimensionsJson(dimensionKeyValues), dimensionKeyValues.navigableKeySet());
            collectMetrics(metadata, null, indexName, indexAbstractionName, dimensionsId);
        }
    }

    /**
     * Returns the id of the dimension JSON. The keys of a new JSON are recorded once, since every series with that JSON
     * has the same keys. The operator owns those strings, so they are charged.
     */
    private long initialDimensionsId(String dimensionsJson, SortedSet<String> keys) {
        long id = table().addDimensions(new BytesRef(dimensionsJson));
        if (id < 0) {
            return -1 - id;
        }
        long bytes = MetadataValues.listBytes(keys.size());
        for (String key : keys) {
            bytes += RamUsageEstimator.sizeOf(key);
        }
        trackBytes(bytes);
        table.dimensionFieldKeys.set(id, List.copyOf(keys));
        return id;
    }

    /**
     * Second pass: records each metric field in its series.
     * The field lookup is checked before inspecting the value type so that nested
     * metric types (histogram, exponential_histogram, tdigest) are recognized correctly.
     */
    @SuppressWarnings("unchecked")
    private void collectMetrics(
        Map<String, Object> metadata,
        String prefix,
        String indexName,
        String indexAbstractionName,
        long dimensionsId
    ) {
        for (Map.Entry<String, Object> entry : metadata.entrySet()) {
            String key = prefix == null ? entry.getKey() : prefix + "." + entry.getKey();
            Object value = entry.getValue();

            MetricFieldInfo fieldInfo = fieldLookup.lookup(indexName, key);
            if (fieldInfo != null) {
                long seriesId = addSeries(new BytesRef(fieldInfo.name()), dimensionsId);
                IndexAbstractionMetadata indexAbstraction = findIndexAbstraction(seriesId, indexAbstractionName);
                if (indexAbstraction == null) {
                    // A data stream name belongs to the shared lookup. A standalone index name was read from this row.
                    long nameBytes = dataStreamsByIndex.containsKey(indexName) ? 0 : RamUsageEstimator.sizeOf(indexAbstractionName);
                    indexAbstraction = addIndexAbstraction(seriesId, indexAbstractionName, nameBytes);
                }
                indexAbstraction.units = withMappingValue(indexAbstraction, indexAbstraction.units, fieldInfo.unit());
                indexAbstraction.fieldTypes = withMappingValue(indexAbstraction, indexAbstraction.fieldTypes, fieldInfo.fieldType());
                indexAbstraction.metricTypes = withMappingValue(indexAbstraction, indexAbstraction.metricTypes, fieldInfo.metricType());
            } else if (value instanceof Map<?, ?> nested) {
                collectMetrics((Map<String, Object>) nested, key, indexName, indexAbstractionName, dimensionsId);
            }
        }
    }

    /**
     * INITIAL: returns {@code current} plus a value from the mappings. The value belongs to the mappings, so only the
     * list is charged to the entry, and the list it replaces is released.
     */
    private List<String> withMappingValue(IndexAbstractionMetadata indexAbstraction, List<String> current, String value) {
        if (value == null || Collections.binarySearch(current, value) >= 0) {
            return current;
        }
        chargeOwned(indexAbstraction, MetadataValues.listBytes(current.size() + 1));
        SortedSet<String> merged = new TreeSet<>(current);
        merged.add(value);
        if (current.isEmpty() == false) {
            releaseOwned(indexAbstraction, MetadataValues.listBytes(current.size()));
        }
        return List.copyOf(merged);
    }

    private static String buildDimensionsJson(Map<String, String> dimensionKeyValues) {
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.startObject();
            for (Map.Entry<String, String> entry : dimensionKeyValues.entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
            builder.endObject();
            return Strings.toString(builder);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** FINAL mode: read the 7-column output from data nodes and accumulate it into the series table. */
    private void addInputFinal(Page page) {
        try {
            BytesRefBlock nameBlock = page.getBlock(finalChannels[COL_METRIC_NAME]);
            BytesRefBlock dsBlock = page.getBlock(finalChannels[COL_DATA_STREAM]);
            BytesRefBlock unitBlock = page.getBlock(finalChannels[COL_UNIT]);
            BytesRefBlock mtBlock = page.getBlock(finalChannels[COL_METRIC_TYPE]);
            BytesRefBlock ftBlock = page.getBlock(finalChannels[COL_FIELD_TYPE]);
            BytesRefBlock dfBlock = page.getBlock(finalChannels[COL_DIMENSION_FIELDS]);
            BytesRefBlock dimBlock = page.getBlock(finalChannels[COL_DIMENSIONS]);

            BytesRef nameScratch = new BytesRef();
            BytesRef dimensionsScratch = new BytesRef();
            BytesRef scratch = new BytesRef();
            for (int pos = 0; pos < page.getPositionCount(); pos++) {
                if (nameBlock.isNull(pos)) {
                    continue;
                }
                BytesRef metricName = nameBlock.getBytesRef(nameBlock.getFirstValueIndex(pos), nameScratch);
                BytesRef dimensionsJson = dimBlock.isNull(pos)
                    ? NO_DIMENSIONS_JSON
                    : dimBlock.getBytesRef(dimBlock.getFirstValueIndex(pos), dimensionsScratch);
                long seriesId = finalSeriesId(metricName, dimensionsJson);
                if (seriesId < 0) {
                    continue;
                }

                addFinalDimensionFieldKeys(table.dimensionsId(seriesId), values.sharedList(readMultiValue(dfBlock, pos, scratch)));
                List<String> units = values.sharedList(readMultiValue(unitBlock, pos, scratch));
                List<String> fieldTypes = values.sharedList(readMultiValue(ftBlock, pos, scratch));
                List<String> metricTypes = values.sharedList(readMultiValue(mtBlock, pos, scratch));
                for (String indexAbstractionName : readMultiValue(dsBlock, pos, scratch)) {
                    IndexAbstractionMetadata indexAbstraction = findIndexAbstraction(seriesId, indexAbstractionName);
                    if (indexAbstraction == null) {
                        indexAbstraction = addIndexAbstraction(seriesId, values.string(indexAbstractionName), 0);
                    }
                    indexAbstraction.units = values.sharedUnion(indexAbstraction.units, units);
                    indexAbstraction.fieldTypes = values.sharedUnion(indexAbstraction.fieldTypes, fieldTypes);
                    indexAbstraction.metricTypes = values.sharedUnion(indexAbstraction.metricTypes, metricTypes);
                }
            }
        } finally {
            page.releaseBlocks();
        }
    }

    /** Returns the series id, or -1 if the series is new and the operator already holds {@link #limit} series. */
    private long finalSeriesId(BytesRef metricName, BytesRef dimensionsJson) {
        if (seriesCount() >= limit) {
            return table == null ? -1 : table.findSeries(metricName, dimensionsJson);
        }
        long dimensionsId = table().addDimensions(dimensionsJson);
        return addSeries(metricName, dimensionsId < 0 ? -1 - dimensionsId : dimensionsId);
    }

    /**
     * Adopts the shared key list of the first row with this dimension JSON. The keys come from the JSON, so later rows
     * carry the same list. Only inconsistent input grows it, into a list owned by the dimensions entry.
     */
    private void addFinalDimensionFieldKeys(long dimensionsId, List<String> sharedKeys) {
        List<String> current = table.dimensionFieldKeys.get(dimensionsId);
        table.dimensionFieldKeys.set(
            dimensionsId,
            current == null || current.isEmpty() ? sharedKeys : values.unsharedUnion(current, sharedKeys)
        );
    }

    private static SortedSet<String> readMultiValue(BytesRefBlock block, int position, BytesRef scratch) {
        SortedSet<String> values = new TreeSet<>();
        if (block.isNull(position)) {
            return values;
        }
        int start = block.getFirstValueIndex(position);
        int count = block.getValueCount(position);
        for (int i = 0; i < count; i++) {
            values.add(block.getBytesRef(start + i, scratch).utf8ToString());
        }
        return values;
    }

    private void trackBytes(long delta) {
        breaker.addEstimateBytesAndMaybeBreak(delta, "TsInfoOperator");
        trackedBytes += delta;
    }

    private void untrackBytes(long delta) {
        breaker.addWithoutBreaking(-delta);
        trackedBytes -= delta;
    }

    private void chargeOwned(IndexAbstractionMetadata indexAbstraction, long bytes) {
        trackBytes(bytes);
        indexAbstraction.ownedBytes += bytes;
    }

    private void releaseOwned(IndexAbstractionMetadata indexAbstraction, long bytes) {
        untrackBytes(bytes);
        indexAbstraction.ownedBytes -= bytes;
    }

    /** Returns the id of the series, adding it if it is new. */
    private long addSeries(BytesRef metricName, long dimensionsId) {
        long id = table().addSeries(metricName, dimensionsId);
        if (id < 0) {
            return -1 - id;
        }
        trackBytes(SERIES_BYTES);
        table.indexAbstractions.set(id, new ArrayList<>(1));
        return id;
    }

    /** A series is usually in one index abstraction, so a linear scan is cheaper than a map. */
    private IndexAbstractionMetadata findIndexAbstraction(long seriesId, String name) {
        for (IndexAbstractionMetadata indexAbstraction : table.indexAbstractions.get(seriesId)) {
            if (indexAbstraction.name.equals(name)) {
                return indexAbstraction;
            }
        }
        return null;
    }

    /** Adds an index abstraction to the series, charging {@code nameBytes} for a name the entry owns. */
    private IndexAbstractionMetadata addIndexAbstraction(long seriesId, String name, long nameBytes) {
        trackBytes(INDEX_ABSTRACTION_BYTES);
        IndexAbstractionMetadata indexAbstraction = new IndexAbstractionMetadata(name);
        table.indexAbstractions.get(seriesId).add(indexAbstraction);
        entriesAccumulated++;
        chargeOwned(indexAbstraction, nameBytes);
        return indexAbstraction;
    }

    @Override
    public void finish() {
        finished = true;
    }

    @Override
    public boolean isFinished() {
        return finished && finalOutputStarted && cursor == NOT_EMITTING;
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        return cursor != NOT_EMITTING || partialEmitThresholdReached();
    }

    @Override
    public Page getOutput() {
        if (cursor == NOT_EMITTING) {
            if (partialEmitThresholdReached()) {
                cursor = 0;
            } else if (finished && finalOutputStarted == false) {
                finalOutputStarted = true;
                if (seriesCount() == 0) {
                    releaseState();
                    // Downstream gets at least one page even when there is no input
                    return rowsEmitted == 0 ? createEmptyPage() : null;
                }
                cursor = 0;
            } else {
                return null;
            }
        }
        Page output = nextPage();
        rowsEmitted += output.getPositionCount();
        if (cursor == table.size()) {
            cursor = NOT_EMITTING;
            // The driver can keep this operator open until the whole pipeline finishes, while the pages already hold the result.
            releaseState();
        }
        return output;
    }

    private Page createEmptyPage() {
        Block[] blocks = new Block[NUM_BLOCKS];
        boolean success = false;
        try {
            for (int i = 0; i < blocks.length; i++) {
                blocks[i] = blockFactory.newConstantBytesRefBlockWith(new BytesRef(""), 0);
            }
            Page page = new Page(0, blocks);
            success = true;
            return page;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(blocks);
            }
        }
    }

    /**
     * Writes the rows of the next series into a page. The page stops after the first series that brings the estimated
     * block memory to {@link #targetPageBytes}, so a page holds at least one series and a single wide series can exceed
     * the target. Each written series drops its index abstractions and returns their memory. The series table and the
     * values shared by several series stay until the last page.
     */
    private Page nextPage() {
        int initialPositions = (int) Math.min(table.size() - cursor, INITIAL_PAGE_POSITIONS);
        BytesRef nameScratch = new BytesRef();
        BytesRef dimensionsScratch = new BytesRef();
        try (
            BytesRefBlock.Builder nameBuilder = blockFactory.newBytesRefBlockBuilder(initialPositions);
            BytesRefBlock.Builder dsBuilder = blockFactory.newBytesRefBlockBuilder(initialPositions);
            BytesRefBlock.Builder unitBuilder = blockFactory.newBytesRefBlockBuilder(initialPositions);
            BytesRefBlock.Builder mtBuilder = blockFactory.newBytesRefBlockBuilder(initialPositions);
            BytesRefBlock.Builder ftBuilder = blockFactory.newBytesRefBlockBuilder(initialPositions);
            BytesRefBlock.Builder dfBuilder = blockFactory.newBytesRefBlockBuilder(initialPositions);
            BytesRefBlock.Builder dimBuilder = blockFactory.newBytesRefBlockBuilder(initialPositions)
        ) {
            Block.Builder[] builders = { nameBuilder, dsBuilder, unitBuilder, mtBuilder, ftBuilder, dfBuilder, dimBuilder };
            int rowCount = 0;
            do {
                long seriesId = cursor++;
                List<IndexAbstractionMetadata> indexAbstractions = table.indexAbstractions.getAndSet(seriesId, null);
                BytesRef metricName = table.metricName(seriesId, nameScratch);
                BytesRef dimensionsJson = table.dimensionsJson(seriesId, dimensionsScratch);
                List<String> dimensionFieldKeys = Objects.requireNonNullElse(
                    table.dimensionFieldKeys.get(table.dimensionsId(seriesId)),
                    List.of()
                );
                long seriesBytes = SERIES_BYTES;
                for (Map.Entry<FieldMetadata, List<String>> row : indexAbstractionsByMetadata(indexAbstractions).entrySet()) {
                    FieldMetadata metadata = row.getKey();
                    nameBuilder.appendBytesRef(metricName);
                    appendMultiValued(dsBuilder, row.getValue());
                    appendMultiValued(unitBuilder, metadata.units());
                    appendMultiValued(mtBuilder, metadata.metricTypes());
                    appendMultiValued(ftBuilder, metadata.fieldTypes());
                    appendMultiValued(dfBuilder, dimensionFieldKeys);
                    dimBuilder.appendBytesRef(dimensionsJson);
                    rowCount++;
                }
                for (IndexAbstractionMetadata indexAbstraction : indexAbstractions) {
                    seriesBytes += INDEX_ABSTRACTION_BYTES + indexAbstraction.ownedBytes;
                }
                untrackBytes(seriesBytes);
            } while (cursor < table.size() && estimatedBytes(builders) < targetPageBytes);
            return new Page(rowCount, Block.Builder.buildAll(builders));
        }
    }

    private static long estimatedBytes(Block.Builder[] builders) {
        long total = 0;
        for (Block.Builder builder : builders) {
            total += builder.estimatedBytes();
        }
        return total;
    }

    /**
     * Groups the index abstraction names of one series by metadata, one output row per group. The map lives only while
     * the series is written and is as large as the series' index abstraction count, so it is not charged to the breaker.
     */
    private static Map<FieldMetadata, List<String>> indexAbstractionsByMetadata(List<IndexAbstractionMetadata> indexAbstractions) {
        Map<FieldMetadata, List<String>> groups = new LinkedHashMap<>();
        for (IndexAbstractionMetadata indexAbstraction : indexAbstractions) {
            FieldMetadata metadata = new FieldMetadata(indexAbstraction.units, indexAbstraction.fieldTypes, indexAbstraction.metricTypes);
            groups.computeIfAbsent(metadata, k -> new ArrayList<>()).add(indexAbstraction.name);
        }
        for (List<String> names : groups.values()) {
            names.sort(null);
        }
        return groups;
    }

    private static void appendMultiValued(BytesRefBlock.Builder builder, List<String> values) {
        if (values.isEmpty()) {
            builder.appendNull();
        } else if (values.size() == 1) {
            builder.appendBytesRef(new BytesRef(values.getFirst()));
        } else {
            builder.beginPositionEntry();
            for (String v : values) {
                builder.appendBytesRef(new BytesRef(v));
            }
            builder.endPositionEntry();
        }
    }

    private Map<String, Object> parseMetadataSource(BytesRefBlock metadataSource, int position, BytesRef sourceScratch) {
        if (metadataSource == null || metadataSource.isNull(position)) {
            return null;
        }
        BytesRef bytes = metadataSource.getBytesRef(position, sourceScratch);
        try (
            var parser = XContentType.JSON.xContent()
                .createParser(XContentParserConfiguration.EMPTY, bytes.bytes, bytes.offset, bytes.length)
        ) {
            parser.nextToken();
            return parser.mapOrdered();
        } catch (Exception e) {
            throw new IllegalStateException("failed to parse _timeseries_metadata at position [" + position + "]", e);
        }
    }

    @Override
    public void close() {
        releaseState();
    }

    private void releaseState() {
        Releasables.close(table);
        table = null;
        values.close();
        values = new MetadataValues(breaker, "TsInfoOperator");
        breaker.addWithoutBreaking(-trackedBytes);
        trackedBytes = 0;
    }

    @Override
    public Operator.Status status() {
        return new Status(mode, pagesReceived, rowsReceived, rowsEmitted, entriesAccumulated);
    }

    @Override
    public String toString() {
        return switch (mode) {
            case INITIAL -> "TsInfoOperator[mode=INITIAL, partialEmitThreshold=" + ByteSizeValue.ofBytes(partialEmitThresholdBytes) + "]";
            case FINAL -> "TsInfoOperator[mode=FINAL" + (limit == NO_LIMIT ? "" : ", limit=" + limit) + "]";
        };
    }

    /**
     * @param entriesAccumulated entries created over the operator's lifetime. In INITIAL mode this includes
     *                           entries that were emitted early, so the same key can be counted more than once.
     */
    public record Status(Mode mode, int pagesReceived, long rowsReceived, long rowsEmitted, int entriesAccumulated)
        implements
            Operator.Status {

        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "ts_info",
            Status::new
        );
        public static final ProfileSchema<Status> PROFILE_SCHEMA = ProfileSchema.of(
            Status.class,
            (builder, status, params) -> status.writeProfile(builder)
        )
            .required("mode", ProfileSchema.string())
            .required("pages_received", ProfileSchema.integer())
            .required("rows_received", ProfileSchema.integer())
            .required("rows_emitted", ProfileSchema.integer())
            .required("entries_accumulated", ProfileSchema.integer())
            .build();

        private static final TransportVersion ESQL_TS_INFO_OPERATOR_STATUS = TransportVersion.fromName("esql_ts_info_operator_status");

        Status(StreamInput in) throws IOException {
            this(Mode.valueOf(in.readString()), in.readVInt(), in.readVLong(), in.readVLong(), in.readVInt());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(mode.name());
            out.writeVInt(pagesReceived);
            out.writeVLong(rowsReceived);
            out.writeVLong(rowsEmitted);
            out.writeVInt(entriesAccumulated);
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        private XContentBuilder writeProfile(XContentBuilder builder) throws IOException {
            builder.startObject();
            builder.field("mode", mode);
            builder.field("pages_received", pagesReceived);
            builder.field("rows_received", rowsReceived);
            builder.field("rows_emitted", rowsEmitted);
            builder.field("entries_accumulated", entriesAccumulated);
            return builder.endObject();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return PROFILE_SCHEMA.write(builder, this, params);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Status other = (Status) o;
            return mode == other.mode
                && pagesReceived == other.pagesReceived
                && rowsReceived == other.rowsReceived
                && rowsEmitted == other.rowsEmitted
                && entriesAccumulated == other.entriesAccumulated;
        }

        @Override
        public int hashCode() {
            return Objects.hash(mode, pagesReceived, rowsReceived, rowsEmitted, entriesAccumulated);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            assert false : "must not be called when overriding supportsVersion";
            throw new UnsupportedOperationException("must not be called when overriding supportsVersion");
        }

        @Override
        public boolean supportsVersion(TransportVersion version) {
            return version.supports(ESQL_TS_INFO_OPERATOR_STATUS);
        }
    }
}
