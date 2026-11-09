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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Operator for the ESQL {@code METRICS_INFO} command.
 * <p>
 * Operates in two modes, mirroring the two-phase INITIAL/FINAL pattern used by aggregations:
 * <ul>
 *   <li><b>INITIAL</b> (data nodes) – created via {@link Factory}. Expects one row per set of fields that
 *       occur together in a document, with blocks for the index name and the multivalued field names, as
 *       emitted by {@code FieldPresenceOperator}. Fields that the {@link MetricFieldLookup} doesn't know as
 *       metrics are dimensions. Groups metrics by (metricName, indexAbstractionName) so that different backing
 *       indices of the same data stream share one entry. Conflicting unit/metric_type/field_type values across
 *       backing indices become multi-valued. Dimension keys are assigned only to the metrics of the same row.
 *       Produces one row per distinct metric signature within the local shards.</li>
 *   <li><b>FINAL</b> (coordinator) – created via {@link FinalFactory}. Receives the 6-column
 *       output pages produced by INITIAL instances on each data node and merges rows that share
 *       the same metric name and metadata, unioning multi-valued fields ({@code data_stream},
 *       {@code dimension_fields}, etc.).</li>
 * </ul>
 *
 * <h2>Output columns (both modes)</h2>
 * <ul>
 *   <li>{@code metric_name} – keyword (single-valued)</li>
 *   <li>{@code data_stream} – keyword (multi-valued); data stream or standalone index names that have this metric
 *       with the same signature (backing index names are resolved to their parent data stream)</li>
 *   <li>{@code unit} – keyword (multi-valued when backing indices differ; may be null)</li>
 *   <li>{@code metric_type} – keyword (multi-valued when definitions differ across data)</li>
 *   <li>{@code field_type} – keyword (multi-valued when definitions differ across data)</li>
 *   <li>{@code dimension_fields} – keyword (multi-valued); union of dimension keys for this row</li>
 * </ul>
 *
 * <h2>State and memory</h2>
 * State is keyed by metric name. Each metric maps its data streams and standalone indices to the metric's metadata
 * and dimension keys there. Output combines the index abstractions of a metric that share metadata, one metric at a
 * time, so no second full-size structure is built. Names repeat across entries, so they come from
 * {@link MetadataValues}, which keeps and charges each distinct name once. The state is bounded by the metric fields
 * of the mappings, so each entry owns its metadata lists instead of sharing them. Dimension keys grow as documents and
 * backing indices add keys, so each entry owns them, as a sorted array of ids into the operator's dimension names.
 * A row maps its dimension names to ids once, so merging them into each of its metrics compares integers, not strings.
 */
public class MetricsInfoOperator implements Operator {

    public static final int NUM_BLOCKS = 6;

    /** One metric and its index abstractions. The name is canonical. */
    private static final class Metric {
        final String name;
        final Map<String, IndexAbstractionMetadata> indexAbstractions = new LinkedHashMap<>();

        Metric(String name) {
            this.name = name;
        }
    }

    /**
     * The metric's state in one data stream or standalone index. The name is shared through {@link MetadataValues}.
     * The metadata lists and dimension ids are owned by this entry.
     */
    private static final class IndexAbstractionMetadata {
        final String name;
        List<String> units = List.of();
        List<String> fieldTypes = List.of();
        List<String> metricTypes = List.of();
        /** Sorted, distinct ids into the operator's dimension names. */
        int[] dimensionIds = NO_DIMENSIONS;

        IndexAbstractionMetadata(String name) {
            this.name = name;
        }
    }

    /** Index abstractions of one metric with equal metadata produce one output row. */
    private record FieldMetadata(List<String> units, List<String> fieldTypes, List<String> metricTypes) {}

    /** The index abstractions and the union of dimension keys of one output row. */
    private record OutputRow(List<String> indexAbstractionNames, SortedSet<String> dimensionFieldKeys) {}

    /** A new metric with its map entry and its empty index abstraction map. */
    private static final long METRIC_BYTES = MetadataValues.MAP_ENTRY_BYTES + RamUsageEstimator.shallowSizeOfInstance(Metric.class)
        + RamUsageEstimates.LINKED_HASH_MAP_SHALLOW_SIZE;

    /** A new index abstraction of a metric with its map entry. */
    private static final long INDEX_ABSTRACTION_BYTES = MetadataValues.MAP_ENTRY_BYTES + RamUsageEstimator.shallowSizeOfInstance(
        IndexAbstractionMetadata.class
    );

    /** A new dimension name with its map entry, its boxed id and its slot in the list of names, excluding the name. */
    private static final long DIMENSION_NAME_BYTES = MetadataValues.MAP_ENTRY_BYTES + RamUsageEstimator.shallowSizeOfInstance(Integer.class)
        + 2L * RamUsageEstimator.NUM_BYTES_OBJECT_REF;

    private static final int[] NO_DIMENSIONS = new int[0];

    /**
     * Looks up metric field metadata on demand.
     * Allows the operator to query mapping information without depending on index mapper classes.
     */
    @FunctionalInterface
    public interface MetricFieldLookup {
        /**
         * Looks up metric field info for a given index and field name.
         *
         * @param indexName the index name
         * @param fieldName the field name (metric name)
         * @return {@link MetricFieldInfo} if the field is a metric, {@code null} otherwise
         */
        MetricFieldInfo lookup(String indexName, String fieldName);
    }

    /** Column indices in the 6-column output. Used by FINAL mode to read incoming pages. */
    private static final int COL_METRIC_NAME = 0;
    private static final int COL_DATA_STREAM = 1;
    private static final int COL_UNIT = 2;
    private static final int COL_METRIC_TYPE = 3;
    private static final int COL_FIELD_TYPE = 4;
    private static final int COL_DIMENSION_FIELDS = 5;

    /**
     * Factory for INITIAL mode (data nodes): builds metric metadata from the sets of fields found in the shards.
     *
     * @param fieldLookup        on-demand lookup for metric field metadata
     * @param dataStreamsByIndex backing index names mapped to their parent data streams, including cluster qualifiers
     * @param indexChannel       channel of the index name block, including the cluster qualifier
     * @param fieldNamesChannel  channel of the multivalued block with the names of fields that occur together
     */
    public record Factory(MetricFieldLookup fieldLookup, Map<String, String> dataStreamsByIndex, int indexChannel, int fieldNamesChannel)
        implements
            OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            return new MetricsInfoOperator(
                Mode.INITIAL,
                driverContext.blockFactory(),
                fieldLookup,
                dataStreamsByIndex,
                indexChannel,
                fieldNamesChannel,
                null
            );
        }

        @Override
        public String describe() {
            return "MetricsInfoOperator[mode=INITIAL, indexChannel=" + indexChannel + ", fieldNamesChannel=" + fieldNamesChannel + "]";
        }
    }

    /**
     * Factory for FINAL mode (coordinator): merges 6-column pages from multiple data nodes.
     *
     * @param channels the 6 input channel indices for
     *                 [metric_name, data_stream, unit, metric_type, field_type, dimension_fields]
     */
    public record FinalFactory(int[] channels) implements OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            return new MetricsInfoOperator(Mode.FINAL, driverContext.blockFactory(), null, Map.of(), -1, -1, channels);
        }

        @Override
        public String describe() {
            return "MetricsInfoOperator[mode=FINAL]";
        }
    }

    public enum Mode {
        INITIAL,
        FINAL
    }

    private Map<String, Metric> metricsByName = new LinkedHashMap<>();
    /** Metric and index abstraction names, shared by all entries, and the metadata lists the entries own. */
    private MetadataValues values;
    /** Ids of the dimension names seen so far. */
    private Map<String, Integer> dimensionIdsByName = new HashMap<>();
    /** Dimension names by id. */
    private List<String> dimensionNames = new ArrayList<>();
    private int metricsAccumulated;

    private final Mode mode;
    private final BlockFactory blockFactory;
    private final CircuitBreaker breaker;
    private long trackedBytes;

    /** Fields used only in INITIAL mode. */
    private final MetricFieldLookup fieldLookup;
    private final Map<String, String> dataStreamsByIndex;
    private final int indexChannel;
    private final int fieldNamesChannel;
    /** FINAL-mode field: input channel indices for the 6 output columns. Null in INITIAL mode. */
    private final int[] finalChannels;

    private boolean finished = false;
    private boolean outputProduced = false;

    private int pagesReceived;
    private long rowsReceived;
    private long rowsEmitted;

    private MetricsInfoOperator(
        Mode mode,
        BlockFactory blockFactory,
        MetricFieldLookup fieldLookup,
        Map<String, String> dataStreamsByIndex,
        int indexChannel,
        int fieldNamesChannel,
        int[] channels
    ) {
        this.mode = mode;
        this.blockFactory = blockFactory;
        this.breaker = blockFactory.breaker();
        this.fieldLookup = fieldLookup;
        this.dataStreamsByIndex = dataStreamsByIndex;
        this.indexChannel = indexChannel;
        this.fieldNamesChannel = fieldNamesChannel;
        this.finalChannels = channels;
        this.values = new MetadataValues(breaker, "MetricsInfoOperator");
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

    /** INITIAL mode: classify each set of fields that occur together into metrics and dimensions. */
    private void addInputInitial(Page page) {
        try {
            BytesRefBlock indexBlock = page.getBlock(indexChannel);
            BytesRefBlock fieldNamesBlock = page.getBlock(fieldNamesChannel);

            BytesRef scratch = new BytesRef();
            for (int p = 0; p < page.getPositionCount(); p++) {
                if (indexBlock.isNull(p) || fieldNamesBlock.isNull(p)) {
                    continue;
                }
                String indexName = indexBlock.getBytesRef(indexBlock.getFirstValueIndex(p), scratch).utf8ToString();
                String indexAbstractionName = dataStreamsByIndex.getOrDefault(indexName, indexName);
                recordFieldSet(indexName, indexAbstractionName, fieldNamesBlock, p, scratch);
            }
        } finally {
            page.releaseBlocks();
        }
    }

    private void addInputFinal(Page page) {
        try {
            BytesRefBlock nameBlock = page.getBlock(finalChannels[COL_METRIC_NAME]);
            BytesRefBlock dsBlock = page.getBlock(finalChannels[COL_DATA_STREAM]);
            BytesRefBlock unitBlock = page.getBlock(finalChannels[COL_UNIT]);
            BytesRefBlock mtBlock = page.getBlock(finalChannels[COL_METRIC_TYPE]);
            BytesRefBlock ftBlock = page.getBlock(finalChannels[COL_FIELD_TYPE]);
            BytesRefBlock dfBlock = page.getBlock(finalChannels[COL_DIMENSION_FIELDS]);

            BytesRef scratch = new BytesRef();
            for (int pos = 0; pos < page.getPositionCount(); pos++) {
                String metricName = readSingleValue(nameBlock, pos, scratch);
                if (metricName == null) {
                    continue;
                }

                SortedSet<String> units = readMultiValue(unitBlock, pos, scratch);
                SortedSet<String> fieldTypes = readMultiValue(ftBlock, pos, scratch);
                SortedSet<String> metricTypes = readMultiValue(mtBlock, pos, scratch);
                int[] dimensionIds = dimensionIds(readMultiValue(dfBlock, pos, scratch));

                Metric metric = findOrCreateMetric(metricName);
                for (String indexAbstractionName : readMultiValue(dsBlock, pos, scratch)) {
                    IndexAbstractionMetadata indexAbstraction = findOrCreateIndexAbstraction(metric, indexAbstractionName);
                    indexAbstraction.units = values.unsharedUnion(indexAbstraction.units, units);
                    indexAbstraction.fieldTypes = values.unsharedUnion(indexAbstraction.fieldTypes, fieldTypes);
                    indexAbstraction.metricTypes = values.unsharedUnion(indexAbstraction.metricTypes, metricTypes);
                    addDimensionIds(indexAbstraction, dimensionIds);
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

    /**
     * Classifies the fields of one document as metrics (via {@link #fieldLookup}) or dimensions, and records the
     * dimensions only on the metrics of the same document, not on every metric ever seen.
     *
     * @param indexName            concrete index name, used for the field lookup because mappings are per index
     * @param indexAbstractionName parent data-stream name or concrete index name, used as the grouping key so that
     *                             backing indices of the same data stream share a single entry
     * @param fieldNamesBlock      names of the fields that occur together in a document, at {@code position}. The
     *                             presence check reports each field once, so the names are distinct.
     */
    private void recordFieldSet(
        String indexName,
        String indexAbstractionName,
        BytesRefBlock fieldNamesBlock,
        int position,
        BytesRef scratch
    ) {
        List<String> dimensionKeys = new ArrayList<>();
        List<MetricFieldInfo> metrics = new ArrayList<>();
        int start = fieldNamesBlock.getFirstValueIndex(position);
        int end = start + fieldNamesBlock.getValueCount(position);
        for (int i = start; i < end; i++) {
            String fieldName = fieldNamesBlock.getBytesRef(i, scratch).utf8ToString();
            MetricFieldInfo fieldInfo = fieldLookup.lookup(indexName, fieldName);
            if (fieldInfo != null) {
                metrics.add(fieldInfo);
            } else {
                dimensionKeys.add(fieldName);
            }
        }
        int[] dimensionIds = dimensionIds(dimensionKeys);
        for (MetricFieldInfo fieldInfo : metrics) {
            recordMetric(fieldInfo, indexAbstractionName, dimensionIds);
        }
    }

    /**
     * Records a metric field under (metricName, indexAbstractionName), so that backing indices within the same data
     * stream share one entry. Conflicting unit/metric_type/field_type across backing indices of the same data stream
     * become multi-valued.
     */
    private void recordMetric(MetricFieldInfo fieldInfo, String indexAbstractionName, int[] dimensionIds) {
        IndexAbstractionMetadata indexAbstraction = findOrCreateIndexAbstraction(
            findOrCreateMetric(fieldInfo.name()),
            indexAbstractionName
        );
        indexAbstraction.units = values.unsharedWithValue(indexAbstraction.units, fieldInfo.unit());
        indexAbstraction.fieldTypes = values.unsharedWithValue(indexAbstraction.fieldTypes, fieldInfo.fieldType());
        indexAbstraction.metricTypes = values.unsharedWithValue(indexAbstraction.metricTypes, fieldInfo.metricType());
        addDimensionIds(indexAbstraction, dimensionIds);
    }

    /** Returns the sorted, distinct ids of {@code names}, assigning ids to names seen for the first time. */
    private int[] dimensionIds(Collection<String> names) {
        if (names.isEmpty()) {
            return NO_DIMENSIONS;
        }
        int[] ids = new int[names.size()];
        int count = 0;
        for (String name : names) {
            ids[count++] = dimensionId(name);
        }
        Arrays.sort(ids);
        int distinct = 1;
        for (int i = 1; i < count; i++) {
            if (ids[i] != ids[distinct - 1]) {
                ids[distinct++] = ids[i];
            }
        }
        return distinct == count ? ids : Arrays.copyOf(ids, distinct);
    }

    private int dimensionId(String name) {
        Integer id = dimensionIdsByName.get(name);
        if (id == null) {
            trackBytes(DIMENSION_NAME_BYTES + RamUsageEstimator.sizeOf(name));
            id = dimensionNames.size();
            dimensionNames.add(name);
            dimensionIdsByName.put(name, id);
        }
        return id;
    }

    /** Merges {@code added} into the entry's ids. Both are sorted and distinct. The replaced array is released. */
    private void addDimensionIds(IndexAbstractionMetadata indexAbstraction, int[] added) {
        int[] current = indexAbstraction.dimensionIds;
        if (current == added || added.length == 0) {
            return;
        }
        int missing = 0;
        for (int i = 0, a = 0; a < added.length; a++) {
            while (i < current.length && current[i] < added[a]) {
                i++;
            }
            if (i == current.length || current[i] != added[a]) {
                missing++;
            }
        }
        if (missing == 0) {
            return;
        }
        int[] merged = new int[current.length + missing];
        trackBytes(RamUsageEstimator.sizeOf(merged));
        int i = 0;
        int a = 0;
        int m = 0;
        while (i < current.length || a < added.length) {
            if (a == added.length || (i < current.length && current[i] < added[a])) {
                merged[m++] = current[i++];
            } else if (i == current.length || added[a] < current[i]) {
                merged[m++] = added[a++];
            } else {
                merged[m++] = current[i++];
                a++;
            }
        }
        if (current.length > 0) {
            releaseBytes(RamUsageEstimator.sizeOf(current));
        }
        indexAbstraction.dimensionIds = merged;
    }

    private Metric findOrCreateMetric(String name) {
        Metric metric = metricsByName.get(name);
        if (metric == null) {
            trackBytes(METRIC_BYTES);
            metric = new Metric(values.string(name));
            metricsByName.put(metric.name, metric);
        }
        return metric;
    }

    private IndexAbstractionMetadata findOrCreateIndexAbstraction(Metric metric, String name) {
        IndexAbstractionMetadata indexAbstraction = metric.indexAbstractions.get(name);
        if (indexAbstraction == null) {
            trackBytes(INDEX_ABSTRACTION_BYTES);
            indexAbstraction = new IndexAbstractionMetadata(values.string(name));
            metric.indexAbstractions.put(indexAbstraction.name, indexAbstraction);
            metricsAccumulated++;
        }
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

        Page output = metricsByName.isEmpty() ? createEmptyPage() : createOutputPage();
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
        int estimatedRows = metricsByName.size();

        try (
            BytesRefBlock.Builder nameBuilder = blockFactory.newBytesRefBlockBuilder(estimatedRows);
            BytesRefBlock.Builder dsBuilder = blockFactory.newBytesRefBlockBuilder(estimatedRows);
            BytesRefBlock.Builder unitBuilder = blockFactory.newBytesRefBlockBuilder(estimatedRows);
            BytesRefBlock.Builder mtBuilder = blockFactory.newBytesRefBlockBuilder(estimatedRows);
            BytesRefBlock.Builder ftBuilder = blockFactory.newBytesRefBlockBuilder(estimatedRows);
            BytesRefBlock.Builder dfBuilder = blockFactory.newBytesRefBlockBuilder(estimatedRows)
        ) {
            int rowCount = 0;
            for (Metric metric : metricsByName.values()) {
                for (Map.Entry<FieldMetadata, OutputRow> row : rowsByMetadata(metric).entrySet()) {
                    FieldMetadata metadata = row.getKey();
                    nameBuilder.appendBytesRef(new BytesRef(metric.name));
                    appendMultiValued(dsBuilder, row.getValue().indexAbstractionNames());
                    appendMultiValued(unitBuilder, metadata.units());
                    appendMultiValued(mtBuilder, metadata.metricTypes());
                    appendMultiValued(ftBuilder, metadata.fieldTypes());
                    appendMultiValued(dfBuilder, row.getValue().dimensionFieldKeys());
                    rowCount++;
                }
            }

            return new Page(rowCount, Block.Builder.buildAll(nameBuilder, dsBuilder, unitBuilder, mtBuilder, ftBuilder, dfBuilder));
        }
    }

    /**
     * Groups the index abstractions of one metric by metadata, one output row per group. The map lives only while the
     * metric is written. It holds references to the metric's names and at most one copy of each distinct dimension key
     * per row, so it is not charged to the breaker.
     */
    private Map<FieldMetadata, OutputRow> rowsByMetadata(Metric metric) {
        Map<FieldMetadata, OutputRow> rows = new LinkedHashMap<>();
        for (IndexAbstractionMetadata indexAbstraction : metric.indexAbstractions.values()) {
            FieldMetadata metadata = new FieldMetadata(indexAbstraction.units, indexAbstraction.fieldTypes, indexAbstraction.metricTypes);
            OutputRow row = rows.computeIfAbsent(metadata, k -> new OutputRow(new ArrayList<>(), new TreeSet<>()));
            row.indexAbstractionNames().add(indexAbstraction.name);
            for (int id : indexAbstraction.dimensionIds) {
                row.dimensionFieldKeys().add(dimensionNames.get(id));
            }
        }
        for (OutputRow row : rows.values()) {
            row.indexAbstractionNames().sort(null);
        }
        return rows;
    }

    private static void appendMultiValued(BytesRefBlock.Builder builder, Iterable<String> values) {
        var iterator = values.iterator();
        if (iterator.hasNext() == false) {
            builder.appendNull();
            return;
        }
        String first = iterator.next();
        if (iterator.hasNext() == false) {
            builder.appendBytesRef(new BytesRef(first));
            return;
        }
        builder.beginPositionEntry();
        builder.appendBytesRef(new BytesRef(first));
        while (iterator.hasNext()) {
            builder.appendBytesRef(new BytesRef(iterator.next()));
        }
        builder.endPositionEntry();
    }

    private void trackBytes(long delta) {
        breaker.addEstimateBytesAndMaybeBreak(delta, "MetricsInfoOperator");
        trackedBytes += delta;
    }

    private void releaseBytes(long bytes) {
        breaker.addWithoutBreaking(-bytes);
        trackedBytes -= bytes;
    }

    @Override
    public void close() {
        releaseState();
    }

    private void releaseState() {
        // Replace rather than clear the maps, because clear() keeps their tables allocated.
        metricsByName = new LinkedHashMap<>();
        dimensionIdsByName = new HashMap<>();
        dimensionNames = new ArrayList<>();
        values.close();
        values = new MetadataValues(breaker, "MetricsInfoOperator");
        breaker.addWithoutBreaking(-trackedBytes);
        trackedBytes = 0;
    }

    @Override
    public Operator.Status status() {
        return new Status(mode, pagesReceived, rowsReceived, rowsEmitted, metricsAccumulated);
    }

    @Override
    public String toString() {
        return "MetricsInfoOperator[mode=" + mode + "]";
    }

    public record Status(Mode mode, int pagesReceived, long rowsReceived, long rowsEmitted, int metricsAccumulated)
        implements
            Operator.Status {

        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "metrics_info",
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
            .required("metrics_accumulated", ProfileSchema.integer())
            .build();

        private static final TransportVersion ESQL_METRICS_INFO_OPERATOR_STATUS = TransportVersion.fromName(
            "esql_metrics_info_operator_status"
        );

        Status(StreamInput in) throws IOException {
            this(Mode.valueOf(in.readString()), in.readVInt(), in.readVLong(), in.readVLong(), in.readVInt());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(mode.name());
            out.writeVInt(pagesReceived);
            out.writeVLong(rowsReceived);
            out.writeVLong(rowsEmitted);
            out.writeVInt(metricsAccumulated);
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
            builder.field("metrics_accumulated", metricsAccumulated);
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
                && metricsAccumulated == other.metricsAccumulated;
        }

        @Override
        public int hashCode() {
            return Objects.hash(mode, pagesReceived, rowsReceived, rowsEmitted, metricsAccumulated);
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
            return version.supports(ESQL_METRICS_INFO_OPERATOR_STATUS);
        }
    }
}
