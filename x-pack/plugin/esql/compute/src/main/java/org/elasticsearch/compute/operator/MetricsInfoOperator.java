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
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

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
 *       the same {@link MetricSignature}, unioning multi-valued fields ({@code data_stream},
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
 */
public class MetricsInfoOperator implements Operator {

    public static final int NUM_BLOCKS = 6;

    private record MetricInfoKey(String metricName, String indexAbstractionName) {}

    /**
     * Represents an intermediate state grouped by metric name and parent data-stream or concrete index name.
     */
    private static class MetricInfo {
        final String name;
        final String indexAbstractionName;
        final Set<String> units = new HashSet<>();
        final Set<String> metricTypes = new HashSet<>();
        final Set<String> fieldTypes = new HashSet<>();
        final Set<String> dimensionFieldKeys = new HashSet<>();

        MetricInfo(String name, String indexAbstractionName) {
            this.name = name;
            this.indexAbstractionName = indexAbstractionName;
        }
    }

    /**
     * Represents a merged output row where indices and data streams with the same
     * signature are combined into one row.
     */
    private static class MetricInfoRow {
        final String metricName;
        final Set<String> indexAbstractionNames = new HashSet<>();
        final Set<String> units;
        final Set<String> fieldTypes;
        final Set<String> metricTypes;
        final Set<String> dimensionFieldKeys = new HashSet<>();

        MetricInfoRow(String metricName, Set<String> units, Set<String> fieldTypes, Set<String> metricTypes) {
            this.metricName = metricName;
            this.units = units;
            this.fieldTypes = fieldTypes;
            this.metricTypes = metricTypes;
        }
    }

    /**
     * Signature for merging rows. Indices and data streams with the same signature are merged
     * into one row with multi-valued data_stream.
     */
    private record MetricSignature(String metricName, Set<String> units, Set<String> fieldTypes, Set<String> metricTypes) {}

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

    /** Shallow size of a MetricInfoKey + MetricInfo pair (object headers and field slots only). */
    static final long ENTRY_SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(MetricInfoKey.class) + RamUsageEstimator
        .shallowSizeOfInstance(MetricInfo.class);

    public enum Mode {
        INITIAL,
        FINAL
    }

    private final Map<MetricInfoKey, MetricInfo> metricsByKey = new LinkedHashMap<>();

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
                recordFieldSet(indexName, indexAbstractionName, readMultiValue(fieldNamesBlock, p, scratch));
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

                Set<String> indexAbstractionNames = readMultiValue(dsBlock, pos, scratch);
                Set<String> units = readMultiValue(unitBlock, pos, scratch);
                Set<String> fieldTypes = readMultiValue(ftBlock, pos, scratch);
                Set<String> metricTypes = readMultiValue(mtBlock, pos, scratch);
                Set<String> dimensionFields = readMultiValue(dfBlock, pos, scratch);

                for (String indexAbstractionName : indexAbstractionNames) {
                    MetricInfoKey key = new MetricInfoKey(metricName, indexAbstractionName);
                    MetricInfo info = metricsByKey.get(key);
                    if (info == null) {
                        trackNewEntry(metricName, indexAbstractionName);
                        info = new MetricInfo(key.metricName(), key.indexAbstractionName());
                        metricsByKey.put(key, info);
                    }
                    trackSetAddAll(info.units, units);
                    trackSetAddAll(info.fieldTypes, fieldTypes);
                    trackSetAddAll(info.metricTypes, metricTypes);
                    trackSetAddAll(info.dimensionFieldKeys, dimensionFields);
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

    /**
     * Classifies the fields of one document as metrics (via {@link #fieldLookup}) or dimensions, and records the
     * dimensions only on the metrics of the same document, not on every metric ever seen.
     *
     * @param indexName            concrete index name, used for the field lookup because mappings are per index
     * @param indexAbstractionName parent data-stream name or concrete index name, used as the grouping key so that
     *                             backing indices of the same data stream share a single {@link MetricInfo}
     * @param fieldNames           names of the fields that occur together in a document
     */
    private void recordFieldSet(String indexName, String indexAbstractionName, Set<String> fieldNames) {
        Set<String> dimensionKeys = new HashSet<>();
        Set<MetricInfo> touchedMetrics = new HashSet<>();
        for (String fieldName : fieldNames) {
            MetricFieldInfo fieldInfo = fieldLookup.lookup(indexName, fieldName);
            if (fieldInfo != null) {
                recordMetric(fieldInfo, indexAbstractionName, touchedMetrics);
            } else {
                dimensionKeys.add(fieldName);
            }
        }
        for (MetricInfo info : touchedMetrics) {
            trackSetAddAll(info.dimensionFieldKeys, dimensionKeys);
        }
    }

    /**
     * Records a metric field into the per-key metric map, grouping by (metricName, indexAbstractionName)
     * so that backing indices within the same data stream share one {@link MetricInfo} entry.
     * Conflicting unit/metric_type/field_type across backing indices of the same data stream
     * become multi-valued.
     */
    private void recordMetric(MetricFieldInfo fieldInfo, String indexAbstractionName, Set<MetricInfo> touchedMetrics) {
        MetricInfoKey infoKey = new MetricInfoKey(fieldInfo.name(), indexAbstractionName);
        MetricInfo info = metricsByKey.get(infoKey);
        if (info == null) {
            trackNewEntry(fieldInfo.name(), indexAbstractionName);
            info = new MetricInfo(infoKey.metricName(), infoKey.indexAbstractionName());
            metricsByKey.put(infoKey, info);
        }
        touchedMetrics.add(info);

        trackSetAdd(info.units, fieldInfo.unit());
        trackSetAdd(info.fieldTypes, fieldInfo.fieldType());
        trackSetAdd(info.metricTypes, fieldInfo.metricType());
    }

    private List<MetricInfoRow> mergeRowsBySignature(Map<MetricInfoKey, MetricInfo> metricsByKey) {
        Map<MetricSignature, MetricInfoRow> bySignature = new LinkedHashMap<>();

        for (MetricInfo info : metricsByKey.values()) {
            MetricSignature sig = new MetricSignature(info.name, info.units, info.fieldTypes, info.metricTypes);
            MetricInfoRow row = bySignature.computeIfAbsent(
                sig,
                s -> new MetricInfoRow(s.metricName(), s.units(), s.fieldTypes(), s.metricTypes())
            );

            row.indexAbstractionNames.add(info.indexAbstractionName);
            row.dimensionFieldKeys.addAll(info.dimensionFieldKeys);
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

        if (metricsByKey.isEmpty()) {
            return createEmptyPage();
        }

        Page output = createOutputPageFromRows(mergeRowsBySignature(metricsByKey));
        rowsEmitted = output.getPositionCount();
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

    private Page createOutputPageFromRows(List<MetricInfoRow> rows) {
        int rowCount = rows.size();

        try (
            BytesRefBlock.Builder nameBuilder = blockFactory.newBytesRefBlockBuilder(rowCount);
            BytesRefBlock.Builder dsBuilder = blockFactory.newBytesRefBlockBuilder(rowCount);
            BytesRefBlock.Builder unitBuilder = blockFactory.newBytesRefBlockBuilder(rowCount);
            BytesRefBlock.Builder mtBuilder = blockFactory.newBytesRefBlockBuilder(rowCount);
            BytesRefBlock.Builder ftBuilder = blockFactory.newBytesRefBlockBuilder(rowCount);
            BytesRefBlock.Builder dfBuilder = blockFactory.newBytesRefBlockBuilder(rowCount)
        ) {

            for (MetricInfoRow row : rows) {
                nameBuilder.appendBytesRef(new BytesRef(row.metricName));
                appendMultiValued(dsBuilder, row.indexAbstractionNames);
                appendMultiValued(unitBuilder, row.units);
                appendMultiValued(mtBuilder, row.metricTypes);
                appendMultiValued(ftBuilder, row.fieldTypes);
                appendMultiValued(dfBuilder, row.dimensionFieldKeys);
            }

            return new Page(rowCount, Block.Builder.buildAll(nameBuilder, dsBuilder, unitBuilder, mtBuilder, ftBuilder, dfBuilder));
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

    private void trackBytes(long delta) {
        breaker.addEstimateBytesAndMaybeBreak(delta, "MetricsInfoOperator");
        trackedBytes += delta;
    }

    private void trackNewEntry(String name, String indexAbstractionName) {
        trackBytes(ENTRY_SHALLOW_SIZE + RamUsageEstimator.sizeOf(name) + RamUsageEstimator.sizeOf(indexAbstractionName));
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

    @Override
    public void close() {
        breaker.addWithoutBreaking(-trackedBytes);
    }

    @Override
    public Operator.Status status() {
        return new Status(mode, pagesReceived, rowsReceived, rowsEmitted, metricsByKey.size());
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

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("mode", mode);
            builder.field("pages_received", pagesReceived);
            builder.field("rows_received", rowsReceived);
            builder.field("rows_emitted", rowsEmitted);
            builder.field("metrics_accumulated", metricsAccumulated);
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
