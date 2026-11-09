/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

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
 * INITIAL emits its buffered rows early whenever their tracked memory reaches the partial emit threshold.
 * The same (metric, data stream, dimensions) key can then appear in several INITIAL pages. FINAL merges
 * those rows again, so it must buffer everything until it has seen all of its input.
 * <p>
 * Both modes emit merged rows in pages of roughly the target page size rather than in one page.
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

    /**
     * Key for grouping per metric, parent data-stream or concrete index, and dimensions in INITIAL mode.
     */
    private record TsInfoKey(String metricName, String indexAbstractionName, String dimensionsJson) {}

    /**
     * Intermediate state grouped by metric, parent data-stream or concrete index, and dimensions.
     */
    private static class TsInfoEntry {
        final String metricName;
        final String indexAbstractionName;
        final String dimensionsJson;
        final Set<String> units = new HashSet<>();
        final Set<String> metricTypes = new HashSet<>();
        final Set<String> fieldTypes = new HashSet<>();
        final Set<String> dimensionFieldKeys = new HashSet<>();

        TsInfoEntry(String metricName, String indexAbstractionName, String dimensionsJson) {
            this.metricName = metricName;
            this.indexAbstractionName = indexAbstractionName;
            this.dimensionsJson = dimensionsJson;
        }
    }

    /**
     * Merged output row: indices and data streams with the same signature are combined.
     */
    private static class TsInfoRow {
        final String metricName;
        final String dimensionsJson;
        final Set<String> indexAbstractionNames = new HashSet<>();
        final Set<String> units;
        final Set<String> fieldTypes;
        final Set<String> metricTypes;
        final Set<String> dimensionFieldKeys = new HashSet<>();

        TsInfoRow(String metricName, String dimensionsJson, Set<String> units, Set<String> fieldTypes, Set<String> metricTypes) {
            this.metricName = metricName;
            this.dimensionsJson = dimensionsJson;
            this.units = units;
            this.fieldTypes = fieldTypes;
            this.metricTypes = metricTypes;
        }

        /** The memory reserved for this row while merging, see {@link #MERGED_ROW_SIZE} and {@link #MERGED_SET_VALUE_SIZE}. */
        long mergedBytes() {
            return MERGED_ROW_SIZE + (long) (indexAbstractionNames.size() + dimensionFieldKeys.size()) * MERGED_SET_VALUE_SIZE;
        }
    }

    /**
     * Signature for merging rows in FINAL mode.
     */
    private record TsSignature(
        String metricName,
        String dimensionsJson,
        Set<String> units,
        Set<String> fieldTypes,
        Set<String> metricTypes
    ) {}

    /** Column indices in the 7-column output. */
    private static final int COL_METRIC_NAME = 0;
    private static final int COL_DATA_STREAM = 1;
    private static final int COL_UNIT = 2;
    private static final int COL_METRIC_TYPE = 3;
    private static final int COL_FIELD_TYPE = 4;
    private static final int COL_DIMENSION_FIELDS = 5;
    private static final int COL_DIMENSIONS = 6;

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
                driverContext.blockFactory(),
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
                driverContext.blockFactory(),
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

    private record MetricDimensions(String metricName, String dimensionsJson) {}

    static final long METRIC_DIMENSIONS_SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(MetricDimensions.class);

    /** Shallow size of a TsInfoKey + TsInfoEntry pair (object headers and field slots only). */
    static final long ENTRY_SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(TsInfoKey.class) + RamUsageEstimator
        .shallowSizeOfInstance(TsInfoEntry.class);

    /*
     * The node classes of HashMap and LinkedHashMap are not accessible, so their sizes are estimated from their fields.
     * A load factor of 0.75 and doubling on resize leave up to about two table slots per node.
     */
    private static final long HASH_MAP_NODE_SIZE = RamUsageEstimator.alignObjectSize(
        RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + Integer.BYTES + 3L * RamUsageEstimator.NUM_BYTES_OBJECT_REF
    ) + 2L * RamUsageEstimator.NUM_BYTES_OBJECT_REF;
    private static final long LINKED_HASH_MAP_NODE_SIZE = RamUsageEstimator.alignObjectSize(
        RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + Integer.BYTES + 5L * RamUsageEstimator.NUM_BYTES_OBJECT_REF
    ) + 2L * RamUsageEstimator.NUM_BYTES_OBJECT_REF;
    /** A HashSet with the default table that its first value allocates. */
    private static final long HASH_SET_SIZE = RamUsageEstimator.shallowSizeOfInstance(HashSet.class) + RamUsageEstimator
        .shallowSizeOfInstance(HashMap.class) + RamUsageEstimator.alignObjectSize(
            RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + 16L * RamUsageEstimator.NUM_BYTES_OBJECT_REF
        );

    /**
     * Memory of one merged row besides the strings and sets it shares with the entries: the signature and row objects,
     * the row's own two sets, and its node in the map of rows by signature.
     */
    static final long MERGED_ROW_SIZE = RamUsageEstimator.shallowSizeOfInstance(TsSignature.class) + RamUsageEstimator
        .shallowSizeOfInstance(TsInfoRow.class) + 2 * HASH_SET_SIZE + LINKED_HASH_MAP_NODE_SIZE;

    /** Memory of one value in a set of a merged row. The value is a string shared with an entry. */
    static final long MERGED_SET_VALUE_SIZE = HASH_MAP_NODE_SIZE;

    public enum Mode {
        INITIAL,
        FINAL
    }

    private final Map<TsInfoKey, TsInfoEntry> entriesByKey = new LinkedHashMap<>();

    private final Mode mode;
    private final BlockFactory blockFactory;
    private final CircuitBreaker breaker;
    private long trackedBytes;

    /** Fields used only in INITIAL mode. */
    private final MetricsInfoOperator.MetricFieldLookup fieldLookup;
    private final Map<String, String> dataStreamsByIndex;
    private final int metadataSourceChannel;
    private final int indexChannel;
    /** FINAL-mode field: input channel indices for the 7 output columns. Null in INITIAL mode. */
    private final int[] finalChannels;
    private final int limit;
    private final Set<MetricDimensions> metricDimensions = new HashSet<>();
    private final long partialEmitThresholdBytes;
    private final long targetPageBytes;

    /**
     * Merged rows that are not emitted yet, or null. While set the operator takes no input, so the memory tracked
     * for these rows is not mixed with memory for new entries.
     */
    private MergedRows pendingRows;

    private boolean finished = false;
    private boolean finalOutputStarted = false;

    private int pagesReceived;
    private long rowsReceived;
    private long rowsEmitted;
    private int entriesAccumulated;

    private TsInfoOperator(
        Mode mode,
        BlockFactory blockFactory,
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
        this.blockFactory = blockFactory;
        this.breaker = blockFactory.breaker();
        this.fieldLookup = fieldLookup;
        this.dataStreamsByIndex = dataStreamsByIndex;
        this.metadataSourceChannel = metadataSourceChannel;
        this.indexChannel = indexChannel;
        this.finalChannels = channels;
        this.limit = limit;
        this.partialEmitThresholdBytes = partialEmitThresholdBytes;
        this.targetPageBytes = targetPageBytes;
    }

    @Override
    public boolean needsInput() {
        return finished == false && pendingRows == null && partialEmitThresholdReached() == false;
    }

    private boolean partialEmitThresholdReached() {
        return entriesByKey.isEmpty() == false && trackedBytes >= partialEmitThresholdBytes;
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

                // Collect dimension key-values and metric fields for this tsid
                Map<String, String> dimensionKeyValues = new TreeMap<>(); // sorted for deterministic JSON
                Set<String> dimensionKeys = new HashSet<>();
                Set<TsInfoEntry> touchedEntries = new HashSet<>();

                collectFields(metadata, null, indexName, indexAbstractionName, dimensionKeyValues, dimensionKeys, touchedEntries);

                // Assign dimension keys to all metrics touched by this tsid
                if (dimensionKeys.isEmpty() == false) {
                    for (TsInfoEntry entry : touchedEntries) {
                        trackSetAddAll(entry.dimensionFieldKeys, dimensionKeys);
                    }
                }
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
     *   <li>Second pass (at the root level): creates {@link TsInfoEntry} objects for each
     *       metric, keyed by (metricName, indexAbstractionName, dimensionsJson).</li>
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
        Map<String, String> dimensionKeyValues,
        Set<String> dimensionKeys,
        Set<TsInfoEntry> touchedEntries
    ) {
        for (Map.Entry<String, Object> entry : metadata.entrySet()) {
            String key = prefix == null ? entry.getKey() : prefix + "." + entry.getKey();
            Object value = entry.getValue();

            MetricFieldInfo fieldInfo = fieldLookup.lookup(indexName, key);
            if (fieldInfo == null) {
                if (value instanceof Map<?, ?> nested) {
                    collectFields(
                        (Map<String, Object>) nested,
                        key,
                        indexName,
                        indexAbstractionName,
                        dimensionKeyValues,
                        dimensionKeys,
                        touchedEntries
                    );
                } else {
                    dimensionKeys.add(key);
                    dimensionKeyValues.put(key, value == null ? null : value.toString());
                }
            }
        }

        if (prefix == null) {
            String dimensionsJson = buildDimensionsJson(dimensionKeyValues);
            collectMetrics(metadata, null, indexName, indexAbstractionName, dimensionsJson, touchedEntries);
        }
    }

    /**
     * Second pass: creates {@link TsInfoEntry} objects for each metric field.
     * The field lookup is checked before inspecting the value type so that nested
     * metric types (histogram, exponential_histogram, tdigest) are recognized correctly.
     */
    @SuppressWarnings("unchecked")
    private void collectMetrics(
        Map<String, Object> metadata,
        String prefix,
        String indexName,
        String indexAbstractionName,
        String dimensionsJson,
        Set<TsInfoEntry> touchedEntries
    ) {
        for (Map.Entry<String, Object> entry : metadata.entrySet()) {
            String key = prefix == null ? entry.getKey() : prefix + "." + entry.getKey();
            Object value = entry.getValue();

            MetricFieldInfo fieldInfo = fieldLookup.lookup(indexName, key);
            if (fieldInfo != null) {
                TsInfoEntry tsEntry = getOrCreateEntry(fieldInfo.name(), indexAbstractionName, dimensionsJson);
                touchedEntries.add(tsEntry);

                trackSetAdd(tsEntry.units, fieldInfo.unit());
                trackSetAdd(tsEntry.fieldTypes, fieldInfo.fieldType());
                trackSetAdd(tsEntry.metricTypes, fieldInfo.metricType());
            } else if (value instanceof Map<?, ?> nested) {
                collectMetrics((Map<String, Object>) nested, key, indexName, indexAbstractionName, dimensionsJson, touchedEntries);
            }
        }
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

    /** FINAL mode: read the 7-column output from data nodes and accumulate into entriesByKey. */
    private void addInputFinal(Page page) {
        try {
            BytesRefBlock nameBlock = page.getBlock(finalChannels[COL_METRIC_NAME]);
            BytesRefBlock dsBlock = page.getBlock(finalChannels[COL_DATA_STREAM]);
            BytesRefBlock unitBlock = page.getBlock(finalChannels[COL_UNIT]);
            BytesRefBlock mtBlock = page.getBlock(finalChannels[COL_METRIC_TYPE]);
            BytesRefBlock ftBlock = page.getBlock(finalChannels[COL_FIELD_TYPE]);
            BytesRefBlock dfBlock = page.getBlock(finalChannels[COL_DIMENSION_FIELDS]);
            BytesRefBlock dimBlock = page.getBlock(finalChannels[COL_DIMENSIONS]);

            BytesRef scratch = new BytesRef();
            for (int pos = 0; pos < page.getPositionCount(); pos++) {
                String metricName = readSingleValue(nameBlock, pos, scratch);
                if (metricName == null) {
                    continue;
                }

                String dimensionsJson = readSingleValue(dimBlock, pos, scratch);
                if (dimensionsJson == null) {
                    dimensionsJson = "{}";
                }
                MetricDimensions metricDims = new MetricDimensions(metricName, dimensionsJson);
                if (metricDimensions.contains(metricDims) == false) {
                    if (metricDimensions.size() >= limit) {
                        continue;
                    }
                    trackBytes(METRIC_DIMENSIONS_SHALLOW_SIZE);
                    metricDimensions.add(metricDims);
                }

                Set<String> indexAbstractionNames = readMultiValue(dsBlock, pos, scratch);
                Set<String> units = readMultiValue(unitBlock, pos, scratch);
                Set<String> fieldTypes = readMultiValue(ftBlock, pos, scratch);
                Set<String> metricTypes = readMultiValue(mtBlock, pos, scratch);
                Set<String> dimensionFields = readMultiValue(dfBlock, pos, scratch);

                for (String indexAbstractionName : indexAbstractionNames) {
                    TsInfoEntry entry = getOrCreateEntry(metricName, indexAbstractionName, dimensionsJson);
                    trackSetAddAll(entry.units, units);
                    trackSetAddAll(entry.fieldTypes, fieldTypes);
                    trackSetAddAll(entry.metricTypes, metricTypes);
                    trackSetAddAll(entry.dimensionFieldKeys, dimensionFields);
                }
            }
        } finally {
            page.releaseBlocks();
        }
    }

    private static String readSingleValue(BytesRefBlock block, int position, BytesRef scratch) {
        if (block.isNull(position)) {
            return null;
        }
        return block.getBytesRef(position, scratch).utf8ToString();
    }

    private static Set<String> readMultiValue(BytesRefBlock block, int position, BytesRef scratch) {
        Set<String> values = new HashSet<>();
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

    private void releaseTrackedBytes() {
        breaker.addWithoutBreaking(-trackedBytes);
        trackedBytes = 0;
    }

    private TsInfoEntry getOrCreateEntry(String metricName, String indexAbstractionName, String dimensionsJson) {
        TsInfoKey key = new TsInfoKey(metricName, indexAbstractionName, dimensionsJson);
        TsInfoEntry entry = entriesByKey.get(key);
        if (entry == null) {
            trackNewEntry(metricName, indexAbstractionName, dimensionsJson);
            entry = new TsInfoEntry(metricName, indexAbstractionName, dimensionsJson);
            entriesByKey.put(key, entry);
            entriesAccumulated++;
        }
        return entry;
    }

    private void trackNewEntry(String metricName, String indexAbstractionName, String dimensionsJson) {
        trackBytes(
            ENTRY_SHALLOW_SIZE + RamUsageEstimator.sizeOf(metricName) + RamUsageEstimator.sizeOf(indexAbstractionName) + RamUsageEstimator
                .sizeOf(dimensionsJson)
        );
    }

    private void trackSetAddAll(Set<String> set, Set<String> values) {
        for (String value : values) {
            if (set.add(value)) {
                trackBytes(RamUsageEstimator.sizeOf(value));
            }
        }
    }

    private void trackSetAdd(Set<String> set, String value) {
        if (value != null && set.add(value)) {
            trackBytes(RamUsageEstimator.sizeOf(value));
        }
    }

    private void untrackBytes(long delta) {
        breaker.addWithoutBreaking(-delta);
        trackedBytes -= delta;
    }

    /**
     * Merges the buffered entries into rows with the same signature. Each row and set value is reserved on the breaker
     * before it is created. Each entry is removed as soon as it is merged, and the memory of its own objects is returned.
     * Its strings and value sets are shared by the rows, so their memory stays reserved until the last row is emitted.
     */
    private MergedRows mergeBufferedEntries() {
        Map<TsSignature, TsInfoRow> bySignature = new LinkedHashMap<>();
        Iterator<TsInfoEntry> entries = entriesByKey.values().iterator();
        while (entries.hasNext()) {
            TsInfoEntry entry = entries.next();
            TsSignature sig = new TsSignature(entry.metricName, entry.dimensionsJson, entry.units, entry.fieldTypes, entry.metricTypes);
            TsInfoRow row = bySignature.get(sig);
            if (row == null) {
                trackBytes(MERGED_ROW_SIZE);
                row = new TsInfoRow(sig.metricName(), sig.dimensionsJson(), sig.units(), sig.fieldTypes(), sig.metricTypes());
                bySignature.put(sig, row);
            }
            addToMergedSet(row.indexAbstractionNames, entry.indexAbstractionName);
            for (String dimensionFieldKey : entry.dimensionFieldKeys) {
                addToMergedSet(row.dimensionFieldKeys, dimensionFieldKey);
            }
            entries.remove();
            untrackBytes(ENTRY_SHALLOW_SIZE);
        }
        untrackBytes(metricDimensions.size() * METRIC_DIMENSIONS_SHALLOW_SIZE);
        metricDimensions.clear();
        return new MergedRows(bySignature);
    }

    private void addToMergedSet(Set<String> set, String value) {
        if (set.contains(value) == false) {
            trackBytes(MERGED_SET_VALUE_SIZE);
            set.add(value);
        }
    }

    @Override
    public void finish() {
        finished = true;
    }

    @Override
    public boolean isFinished() {
        return finished && finalOutputStarted && pendingRows == null;
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        return pendingRows != null || partialEmitThresholdReached();
    }

    @Override
    public Page getOutput() {
        if (pendingRows == null) {
            if (partialEmitThresholdReached()) {
                pendingRows = mergeBufferedEntries();
            } else if (finished && finalOutputStarted == false) {
                finalOutputStarted = true;
                if (entriesByKey.isEmpty()) {
                    // Downstream gets at least one page even when there is no input
                    return rowsEmitted == 0 ? createEmptyPage() : null;
                }
                pendingRows = mergeBufferedEntries();
            } else {
                return null;
            }
        }
        Page output = pendingRows.nextPage();
        rowsEmitted += output.getPositionCount();
        if (pendingRows.hasNext() == false) {
            pendingRows = null;
            releaseTrackedBytes();
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
     * Merged rows emitted over several pages. Each page stops at the first row that brings the estimated block
     * memory to {@link #targetPageBytes}, so a page holds at least one row and a single wide row can exceed the target.
     * Each emitted row is removed and returns the memory reserved for it while merging, so it can be collected while
     * later pages are built.
     */
    private final class MergedRows {
        /**
         * Rows vary widely in size, so the row count of a page is unknown up front.
         * Builders start at this many positions and grow.
         */
        private static final int INITIAL_PAGE_POSITIONS = 128;

        private final Iterator<TsInfoRow> rows;
        private int remaining;

        MergedRows(Map<TsSignature, TsInfoRow> bySignature) {
            this.rows = bySignature.values().iterator();
            this.remaining = bySignature.size();
        }

        boolean hasNext() {
            return remaining > 0;
        }

        Page nextPage() {
            int initialPositions = Math.min(remaining, INITIAL_PAGE_POSITIONS);
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
                int positions = 0;
                do {
                    TsInfoRow row = rows.next();
                    rows.remove();
                    remaining--;
                    nameBuilder.appendBytesRef(new BytesRef(row.metricName));
                    appendMultiValued(dsBuilder, row.indexAbstractionNames);
                    appendMultiValued(unitBuilder, row.units);
                    appendMultiValued(mtBuilder, row.metricTypes);
                    appendMultiValued(ftBuilder, row.fieldTypes);
                    appendMultiValued(dfBuilder, row.dimensionFieldKeys);
                    dimBuilder.appendBytesRef(new BytesRef(row.dimensionsJson));
                    untrackBytes(row.mergedBytes());
                    positions++;
                } while (hasNext() && estimatedBytes(builders) < targetPageBytes);
                return new Page(positions, Block.Builder.buildAll(builders));
            }
        }

        private static long estimatedBytes(Block.Builder[] builders) {
            long total = 0;
            for (Block.Builder builder : builders) {
                total += builder.estimatedBytes();
            }
            return total;
        }
    }

    private static void appendMultiValued(BytesRefBlock.Builder builder, Set<String> values) {
        if (values == null || values.isEmpty()) {
            builder.appendNull();
        } else if (values.size() == 1) {
            builder.appendBytesRef(new BytesRef(values.iterator().next()));
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
        releaseTrackedBytes();
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
