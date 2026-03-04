/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
 *
 * <h2>Output columns (both modes)</h2>
 * <ul>
 *   <li>{@code metric_name} – keyword (single-valued)</li>
 *   <li>{@code data_stream} – keyword (multi-valued); data stream names</li>
 *   <li>{@code unit} – keyword (multi-valued when backing indices differ; may be null)</li>
 *   <li>{@code metric_type} – keyword (multi-valued when definitions differ)</li>
 *   <li>{@code field_type} – keyword (multi-valued when definitions differ)</li>
 *   <li>{@code dimension_fields} – keyword (multi-valued); union of dimension keys</li>
 *   <li>{@code dimensions} – keyword (single-valued); JSON object of dimension key/values</li>
 * </ul>
 */
public class TsInfoOperator implements Operator {

    public static final int NUM_BLOCKS = 7;

    /**
     * Key for grouping per (metric, data-stream, dimensions) in INITIAL mode.
     */
    private record TsInfoKey(String metricName, String dataStreamName, String dimensionsJson) {}

    /**
     * Intermediate state grouped by (metric, data-stream, dimensions).
     */
    private static class TsInfoEntry {
        final String metricName;
        final String dataStream;
        final String dimensionsJson;
        final Set<String> units = new HashSet<>();
        final Set<String> metricTypes = new HashSet<>();
        final Set<String> fieldTypes = new HashSet<>();
        final Set<String> dimensionFieldKeys = new HashSet<>();

        TsInfoEntry(String metricName, String dataStream, String dimensionsJson) {
            this.metricName = metricName;
            this.dataStream = dataStream;
            this.dimensionsJson = dimensionsJson;
        }
    }

    /**
     * Merged output row: multiple data streams with the same signature are combined.
     */
    private static class TsInfoRow {
        final String metricName;
        final String dimensionsJson;
        final Set<String> dataStreams = new HashSet<>();
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
     * @param metadataSourceChannel channel index for {@code _timeseries_metadata} block
     * @param indexChannel          channel index for {@code _index} block
     */
    public record Factory(MetricsInfoOperator.MetricFieldLookup fieldLookup, int metadataSourceChannel, int indexChannel)
        implements
            OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            return new TsInfoOperator(Mode.INITIAL, driverContext.blockFactory(), fieldLookup, metadataSourceChannel, indexChannel, null);
        }

        @Override
        public String describe() {
            return "TsInfoOperator[mode=INITIAL, metadataSourceChannel=" + metadataSourceChannel + ", indexChannel=" + indexChannel + "]";
        }
    }

    /**
     * Factory for FINAL mode (coordinator): merges 7-column pages from multiple data nodes.
     *
     * @param channels the 7 input channel indices for
     *                 [metric_name, data_stream, unit, metric_type, field_type, dimension_fields, dimensions]
     */
    public record FinalFactory(int[] channels) implements OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            return new TsInfoOperator(Mode.FINAL, driverContext.blockFactory(), null, -1, -1, channels);
        }

        @Override
        public String describe() {
            return "TsInfoOperator[mode=FINAL]";
        }
    }

    // TODO: Improve memory tracking
    static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(TsInfoKey.class) + RamUsageEstimator.shallowSizeOfInstance(
        TsInfoEntry.class
    );

    public enum Mode {
        INITIAL,
        FINAL
    }

    private final Map<TsInfoKey, TsInfoEntry> entriesByKey = new LinkedHashMap<>();

    private final Mode mode;
    private final BlockFactory blockFactory;
    private final CircuitBreaker breaker;
    private long trackedBytes;

    /** INITIAL-mode fields (null in FINAL mode). */
    private final MetricsInfoOperator.MetricFieldLookup fieldLookup;
    private final int metadataSourceChannel;
    private final int indexChannel;
    /** FINAL-mode field: input channel indices for the 7 output columns. Null in INITIAL mode. */
    private final int[] finalChannels;

    private boolean finished = false;
    private boolean outputProduced = false;

    private TsInfoOperator(
        Mode mode,
        BlockFactory blockFactory,
        MetricsInfoOperator.MetricFieldLookup fieldLookup,
        int metadataSourceChannel,
        int indexChannel,
        int[] channels
    ) {
        this.mode = mode;
        this.blockFactory = blockFactory;
        this.breaker = blockFactory.breaker();
        this.fieldLookup = fieldLookup;
        this.metadataSourceChannel = metadataSourceChannel;
        this.indexChannel = indexChannel;
        this.finalChannels = channels;
    }

    @Override
    public boolean needsInput() {
        return finished == false;
    }

    @Override
    public void addInput(Page page) {
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
                String dataStreamName = MetricsInfoOperator.resolveDataStreamName(indexName);
                Map<String, Object> metadata = parseMetadataSource(metadataSource, p, sourceScratch);
                if (metadata == null) {
                    continue;
                }

                // Collect dimension key-values and metric fields for this tsid
                Map<String, String> dimensionKeyValues = new TreeMap<>(); // sorted for deterministic JSON
                Set<String> dimensionKeys = new HashSet<>();
                Set<TsInfoEntry> touchedEntries = new HashSet<>();

                collectFields(metadata, null, indexName, dataStreamName, dimensionKeyValues, dimensionKeys, touchedEntries);

                // Assign dimension keys to all metrics touched by this tsid
                if (dimensionKeys.isEmpty() == false) {
                    for (TsInfoEntry entry : touchedEntries) {
                        entry.dimensionFieldKeys.addAll(dimensionKeys);
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
     *       metric, keyed by (metricName, dataStream, dimensionsJson).</li>
     * </ol>
     * The field lookup is checked <em>before</em> inspecting the value type so that nested
     * metric types (histogram, exponential_histogram, tdigest) are not recursed into.
     */
    @SuppressWarnings("unchecked")
    private void collectFields(
        Map<String, Object> metadata,
        String prefix,
        String indexName,
        String dataStreamName,
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
                        dataStreamName,
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
            collectMetrics(metadata, null, indexName, dataStreamName, dimensionsJson, touchedEntries);
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
        String dataStreamName,
        String dimensionsJson,
        Set<TsInfoEntry> touchedEntries
    ) {
        for (Map.Entry<String, Object> entry : metadata.entrySet()) {
            String key = prefix == null ? entry.getKey() : prefix + "." + entry.getKey();
            Object value = entry.getValue();

            MetricFieldInfo fieldInfo = fieldLookup.lookup(indexName, key);
            if (fieldInfo != null) {
                TsInfoKey infoKey = new TsInfoKey(fieldInfo.name(), dataStreamName, dimensionsJson);
                TsInfoEntry tsEntry = entriesByKey.get(infoKey);
                if (tsEntry == null) {
                    trackNewEntry();
                    tsEntry = new TsInfoEntry(infoKey.metricName(), infoKey.dataStreamName(), infoKey.dimensionsJson());
                    entriesByKey.put(infoKey, tsEntry);
                }
                touchedEntries.add(tsEntry);

                if (fieldInfo.unit() != null) {
                    tsEntry.units.add(fieldInfo.unit());
                }
                if (fieldInfo.fieldType() != null) {
                    tsEntry.fieldTypes.add(fieldInfo.fieldType());
                }
                if (fieldInfo.metricType() != null) {
                    tsEntry.metricTypes.add(fieldInfo.metricType());
                }
            } else if (value instanceof Map<?, ?> nested) {
                collectMetrics((Map<String, Object>) nested, key, indexName, dataStreamName, dimensionsJson, touchedEntries);
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

                Set<String> dataStreams = readMultiValue(dsBlock, pos, scratch);
                Set<String> units = readMultiValue(unitBlock, pos, scratch);
                Set<String> fieldTypes = readMultiValue(ftBlock, pos, scratch);
                Set<String> metricTypes = readMultiValue(mtBlock, pos, scratch);
                Set<String> dimensionFields = readMultiValue(dfBlock, pos, scratch);

                for (String ds : dataStreams) {
                    TsInfoKey key = new TsInfoKey(metricName, ds, dimensionsJson);
                    TsInfoEntry entry = entriesByKey.get(key);
                    if (entry == null) {
                        trackNewEntry();
                        entry = new TsInfoEntry(key.metricName(), key.dataStreamName(), key.dimensionsJson());
                        entriesByKey.put(key, entry);
                    }
                    entry.units.addAll(units);
                    entry.fieldTypes.addAll(fieldTypes);
                    entry.metricTypes.addAll(metricTypes);
                    entry.dimensionFieldKeys.addAll(dimensionFields);
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

    private void trackNewEntry() {
        breaker.addEstimateBytesAndMaybeBreak(SHALLOW_SIZE, "TsInfoOperator");
        trackedBytes += SHALLOW_SIZE;
    }

    private List<TsInfoRow> mergeRowsBySignature(Map<TsInfoKey, TsInfoEntry> entriesByKey) {
        Map<TsSignature, TsInfoRow> bySignature = new LinkedHashMap<>();

        for (TsInfoEntry entry : entriesByKey.values()) {
            TsSignature sig = new TsSignature(entry.metricName, entry.dimensionsJson, entry.units, entry.fieldTypes, entry.metricTypes);
            TsInfoRow row = bySignature.computeIfAbsent(
                sig,
                s -> new TsInfoRow(s.metricName(), s.dimensionsJson(), s.units(), s.fieldTypes(), s.metricTypes())
            );

            row.dataStreams.add(entry.dataStream);
            row.dimensionFieldKeys.addAll(entry.dimensionFieldKeys);
        }

        return new ArrayList<>(bySignature.values());
    }

    @Override
    public void finish() {
        finished = true;
    }

    @Override
    public boolean isFinished() {
        return finished && outputProduced;
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        return false;
    }

    @Override
    public Page getOutput() {
        if (finished == false || outputProduced) {
            return null;
        }

        outputProduced = true;

        if (entriesByKey.isEmpty()) {
            return createEmptyPage();
        }

        return createOutputPageFromRows(mergeRowsBySignature(entriesByKey));
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

    private Page createOutputPageFromRows(List<TsInfoRow> rows) {
        int rowCount = rows.size();

        try (
            BytesRefBlock.Builder nameBuilder = blockFactory.newBytesRefBlockBuilder(rowCount);
            BytesRefBlock.Builder dsBuilder = blockFactory.newBytesRefBlockBuilder(rowCount);
            BytesRefBlock.Builder unitBuilder = blockFactory.newBytesRefBlockBuilder(rowCount);
            BytesRefBlock.Builder mtBuilder = blockFactory.newBytesRefBlockBuilder(rowCount);
            BytesRefBlock.Builder ftBuilder = blockFactory.newBytesRefBlockBuilder(rowCount);
            BytesRefBlock.Builder dfBuilder = blockFactory.newBytesRefBlockBuilder(rowCount);
            BytesRefBlock.Builder dimBuilder = blockFactory.newBytesRefBlockBuilder(rowCount)
        ) {
            for (TsInfoRow row : rows) {
                nameBuilder.appendBytesRef(new BytesRef(row.metricName));
                appendMultiValued(dsBuilder, row.dataStreams);
                appendMultiValued(unitBuilder, row.units);
                appendMultiValued(mtBuilder, row.metricTypes);
                appendMultiValued(ftBuilder, row.fieldTypes);
                appendMultiValued(dfBuilder, row.dimensionFieldKeys);
                dimBuilder.appendBytesRef(new BytesRef(row.dimensionsJson));
            }

            return new Page(
                rowCount,
                Block.Builder.buildAll(nameBuilder, dsBuilder, unitBuilder, mtBuilder, ftBuilder, dfBuilder, dimBuilder)
            );
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
        breaker.addWithoutBreaking(-trackedBytes);
    }

    @Override
    public String toString() {
        return "TsInfoOperator[mode=" + mode + "]";
    }
}
