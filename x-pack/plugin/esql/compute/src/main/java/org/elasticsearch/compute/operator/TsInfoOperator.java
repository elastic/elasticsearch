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
import org.elasticsearch.common.lucene.RamUsageEstimates;
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
 * State is keyed by series: one metric with one set of dimension values. Each series records the
 * data streams and standalone indices it was seen in, with the metric's metadata in each. Output
 * combines the index abstractions of a series that share metadata, one series at a time, so no
 * second full-size structure is built. Names and metadata values repeat across most series, so
 * the operator keeps one canonical instance of each and charges the breaker for it once.
 */
public class TsInfoOperator implements Operator {

    public static final int NUM_BLOCKS = 7;
    public static final int NO_LIMIT = Integer.MAX_VALUE;

    /** One metric with one set of dimension values. Both strings are canonical. */
    private record SeriesKey(String metricName, String dimensionsJson) {}

    /**
     * Accumulated state of one series. The value lists are immutable and usually shared with other series
     * through {@link MetadataValues}, so they are replaced, never mutated.
     */
    private static final class Series {
        final SeriesKey key;
        List<String> dimensionFieldKeys = List.of();
        /** A series is usually in one index abstraction, so a linear scan is cheaper than a map. */
        final List<IndexAbstractionMetadata> indexAbstractions = new ArrayList<>(1);

        Series(SeriesKey key) {
            this.key = key;
        }
    }

    /** The series' metric metadata in one data stream or standalone index. The name and lists come from {@link MetadataValues}. */
    private static final class IndexAbstractionMetadata {
        final String name;
        List<String> units = List.of();
        List<String> fieldTypes = List.of();
        List<String> metricTypes = List.of();

        IndexAbstractionMetadata(String name) {
            this.name = name;
        }
    }

    /** Index abstractions of one series with equal metadata produce one output row. */
    private record FieldMetadata(List<String> units, List<String> fieldTypes, List<String> metricTypes) {}

    /** A new series with its map entry and an index abstraction list of capacity one. */
    private static final long SERIES_BYTES = MetadataValues.MAP_ENTRY_BYTES + RamUsageEstimator.shallowSizeOfInstance(SeriesKey.class)
        + RamUsageEstimator.shallowSizeOfInstance(Series.class) + RamUsageEstimates.ARRAY_LIST_SHALLOW_SIZE + RamUsageEstimator
            .alignObjectSize(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + RamUsageEstimator.NUM_BYTES_OBJECT_REF);

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
     * Factory for INITIAL mode (data nodes).
     *
     * @param fieldLookup          on-demand lookup for metric field metadata
     * @param dataStreamsByIndex   backing index names mapped to their parent data streams, including cluster qualifiers
     * @param metadataSourceChannel channel index for {@code _timeseries_metadata} block
     * @param indexChannel          channel index for {@code _index} block
     */
    public record Factory(
        MetricsInfoOperator.MetricFieldLookup fieldLookup,
        Map<String, String> dataStreamsByIndex,
        int metadataSourceChannel,
        int indexChannel
    ) implements OperatorFactory {
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
                NO_LIMIT
            );
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
    public record FinalFactory(int[] channels, int limit) implements OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            return new TsInfoOperator(Mode.FINAL, driverContext.blockFactory(), null, Map.of(), -1, -1, channels, limit);
        }

        @Override
        public String describe() {
            return "TsInfoOperator[mode=FINAL" + (limit == NO_LIMIT ? "" : ", limit=" + limit) + "]";
        }
    }

    public enum Mode {
        INITIAL,
        FINAL
    }

    private Map<SeriesKey, Series> seriesByKey = new LinkedHashMap<>();
    /** Metric names, dimension JSON, index abstraction names and value lists, shared by all series. */
    private MetadataValues values;
    private int entriesAccumulated;

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
    /** Maximum number of distinct series. Only FINAL mode sets a limit. */
    private final int limit;

    private boolean finished = false;
    private boolean outputProduced = false;

    private int pagesReceived;
    private long rowsReceived;
    private long rowsEmitted;

    private TsInfoOperator(
        Mode mode,
        BlockFactory blockFactory,
        MetricsInfoOperator.MetricFieldLookup fieldLookup,
        Map<String, String> dataStreamsByIndex,
        int metadataSourceChannel,
        int indexChannel,
        int[] channels,
        int limit
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
        this.values = new MetadataValues(breaker, "TsInfoOperator");
    }

    @Override
    public boolean needsInput() {
        return finished == false;
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
     *   <li>Second pass (at the root level): records each metric in its {@link Series},
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
            String dimensionsJson = buildDimensionsJson(dimensionKeyValues);
            List<String> dimensionFieldKeys = values.sharedList(dimensionKeyValues.navigableKeySet());
            collectMetrics(metadata, null, indexName, indexAbstractionName, dimensionsJson, dimensionFieldKeys);
        }
    }

    /**
     * Second pass: records each metric field in its {@link Series}.
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
        List<String> dimensionFieldKeys
    ) {
        for (Map.Entry<String, Object> entry : metadata.entrySet()) {
            String key = prefix == null ? entry.getKey() : prefix + "." + entry.getKey();
            Object value = entry.getValue();

            MetricFieldInfo fieldInfo = fieldLookup.lookup(indexName, key);
            if (fieldInfo != null) {
                Series series = findOrCreateSeriesWithinLimit(fieldInfo.name(), dimensionsJson);
                assert series != null : "INITIAL mode has no series limit";
                addDimensionFieldKeys(series, dimensionFieldKeys);
                IndexAbstractionMetadata indexAbstraction = findOrCreateIndexAbstraction(series, indexAbstractionName);
                indexAbstraction.units = values.sharedWithValue(indexAbstraction.units, fieldInfo.unit());
                indexAbstraction.fieldTypes = values.sharedWithValue(indexAbstraction.fieldTypes, fieldInfo.fieldType());
                indexAbstraction.metricTypes = values.sharedWithValue(indexAbstraction.metricTypes, fieldInfo.metricType());
            } else if (value instanceof Map<?, ?> nested) {
                collectMetrics((Map<String, Object>) nested, key, indexName, indexAbstractionName, dimensionsJson, dimensionFieldKeys);
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

    /** FINAL mode: read the 7-column output from data nodes and accumulate into seriesByKey. */
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
                Series series = findOrCreateSeriesWithinLimit(metricName, dimensionsJson);
                if (series == null) {
                    continue;
                }

                addDimensionFieldKeys(series, values.sharedList(readMultiValue(dfBlock, pos, scratch)));
                List<String> units = values.sharedList(readMultiValue(unitBlock, pos, scratch));
                List<String> fieldTypes = values.sharedList(readMultiValue(ftBlock, pos, scratch));
                List<String> metricTypes = values.sharedList(readMultiValue(mtBlock, pos, scratch));
                for (String indexAbstractionName : readMultiValue(dsBlock, pos, scratch)) {
                    IndexAbstractionMetadata indexAbstraction = findOrCreateIndexAbstraction(series, indexAbstractionName);
                    indexAbstraction.units = values.sharedUnion(indexAbstraction.units, units);
                    indexAbstraction.fieldTypes = values.sharedUnion(indexAbstraction.fieldTypes, fieldTypes);
                    indexAbstraction.metricTypes = values.sharedUnion(indexAbstraction.metricTypes, metricTypes);
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

    /** Returns the series, or {@code null} if it is new and the operator already holds {@link #limit} series. */
    private Series findOrCreateSeriesWithinLimit(String metricName, String dimensionsJson) {
        Series series = seriesByKey.get(new SeriesKey(metricName, dimensionsJson));
        if (series == null && seriesByKey.size() < limit) {
            trackBytes(SERIES_BYTES);
            SeriesKey key = new SeriesKey(values.string(metricName), values.string(dimensionsJson));
            series = new Series(key);
            seriesByKey.put(key, series);
        }
        return series;
    }

    /**
     * Adopts the shared key list of the first row of a series. The keys come from the series' dimension JSON, so later
     * rows carry the same list. Only inconsistent input grows it, into a list the series owns.
     */
    private void addDimensionFieldKeys(Series series, List<String> sharedKeys) {
        series.dimensionFieldKeys = series.dimensionFieldKeys.isEmpty()
            ? sharedKeys
            : values.unsharedUnion(series.dimensionFieldKeys, sharedKeys);
    }

    private IndexAbstractionMetadata findOrCreateIndexAbstraction(Series series, String name) {
        for (IndexAbstractionMetadata indexAbstraction : series.indexAbstractions) {
            if (indexAbstraction.name.equals(name)) {
                return indexAbstraction;
            }
        }
        trackBytes(INDEX_ABSTRACTION_BYTES);
        IndexAbstractionMetadata indexAbstraction = new IndexAbstractionMetadata(values.string(name));
        series.indexAbstractions.add(indexAbstraction);
        entriesAccumulated++;
        return indexAbstraction;
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

        Page output = seriesByKey.isEmpty() ? createEmptyPage() : createOutputPage();
        rowsEmitted = output.getPositionCount();
        // The driver can keep this operator open until the whole pipeline finishes, while the page already holds the result.
        releaseState();
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

    private Page createOutputPage() {
        int estimatedRows = seriesByKey.size();

        try (
            BytesRefBlock.Builder nameBuilder = blockFactory.newBytesRefBlockBuilder(estimatedRows);
            BytesRefBlock.Builder dsBuilder = blockFactory.newBytesRefBlockBuilder(estimatedRows);
            BytesRefBlock.Builder unitBuilder = blockFactory.newBytesRefBlockBuilder(estimatedRows);
            BytesRefBlock.Builder mtBuilder = blockFactory.newBytesRefBlockBuilder(estimatedRows);
            BytesRefBlock.Builder ftBuilder = blockFactory.newBytesRefBlockBuilder(estimatedRows);
            BytesRefBlock.Builder dfBuilder = blockFactory.newBytesRefBlockBuilder(estimatedRows);
            BytesRefBlock.Builder dimBuilder = blockFactory.newBytesRefBlockBuilder(estimatedRows)
        ) {
            int rowCount = 0;
            for (Series series : seriesByKey.values()) {
                for (Map.Entry<FieldMetadata, List<String>> row : indexAbstractionsByMetadata(series).entrySet()) {
                    FieldMetadata metadata = row.getKey();
                    nameBuilder.appendBytesRef(new BytesRef(series.key.metricName()));
                    appendMultiValued(dsBuilder, row.getValue());
                    appendMultiValued(unitBuilder, metadata.units());
                    appendMultiValued(mtBuilder, metadata.metricTypes());
                    appendMultiValued(ftBuilder, metadata.fieldTypes());
                    appendMultiValued(dfBuilder, series.dimensionFieldKeys);
                    dimBuilder.appendBytesRef(new BytesRef(series.key.dimensionsJson()));
                    rowCount++;
                }
            }

            return new Page(
                rowCount,
                Block.Builder.buildAll(nameBuilder, dsBuilder, unitBuilder, mtBuilder, ftBuilder, dfBuilder, dimBuilder)
            );
        }
    }

    /**
     * Groups the index abstraction names of one series by metadata, one output row per group. The map lives only while
     * the series is written and is as large as the series' index abstraction count, so it is not charged to the breaker.
     */
    private static Map<FieldMetadata, List<String>> indexAbstractionsByMetadata(Series series) {
        Map<FieldMetadata, List<String>> groups = new LinkedHashMap<>();
        for (IndexAbstractionMetadata indexAbstraction : series.indexAbstractions) {
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
        // Replace rather than clear the maps, because clear() keeps their tables allocated.
        seriesByKey = new LinkedHashMap<>();
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
        return "TsInfoOperator[mode=" + mode + (limit == NO_LIMIT ? "" : ", limit=" + limit) + "]";
    }

    public record Status(Mode mode, int pagesReceived, long rowsReceived, long rowsEmitted, int entriesAccumulated)
        implements
            Operator.Status {

        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "ts_info",
            Status::new
        );

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

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("mode", mode);
            builder.field("pages_received", pagesReceived);
            builder.field("rows_received", rowsReceived);
            builder.field("rows_emitted", rowsEmitted);
            builder.field("entries_accumulated", entriesAccumulated);
            return builder.endObject();
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
