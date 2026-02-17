/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Operator for the ESQL {@code METRICS_INFO} command.
 * <p>
 * Operates in two modes, mirroring the two-phase INITIAL/FINAL pattern used by aggregations:
 * <ul>
 *   <li><b>INITIAL</b> (data nodes) – created via {@link Factory}. Expects deduplicated input
 *       (one row per {@code _tsid}) from an upstream {@link DistinctByOperator}, with blocks for
 *       {@code _timeseries_metadata} and {@code _index}. Groups metrics by (metricName, dataStreamName)
 *       so that different backing indices of the same data stream share one entry. Conflicting
 *       unit/metric_type/field_type values across backing indices become multi-valued. Dimension
 *       keys are assigned only to the metrics that actually appeared in each tsid's metadata.
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
 *   <li>{@code data_stream} – keyword (multi-valued); data stream names that have this metric
 *       with the same signature (backing index names are resolved to their parent data stream)</li>
 *   <li>{@code unit} – keyword (multi-valued when backing indices differ; may be null)</li>
 *   <li>{@code metric_type} – keyword (multi-valued when definitions differ across data)</li>
 *   <li>{@code field_type} – keyword (multi-valued when definitions differ across data)</li>
 *   <li>{@code dimension_fields} – keyword (multi-valued); union of dimension keys for this row</li>
 * </ul>
 */
public class MetricsInfoOperator implements Operator {

    public static final int NUM_BLOCKS = 6;

    private record MetricInfoKey(String metricName, String dataStreamName) {}

    /**
     * Represents an intermediate state grouped by name and dataStream.
     */
    private static class MetricInfo {
        final String name;
        final String dataStream;
        final Set<String> units = new HashSet<>();
        final Set<String> metricTypes = new HashSet<>();
        final Set<String> fieldTypes = new HashSet<>();
        final Set<String> dimensionFieldKeys = new HashSet<>();

        MetricInfo(String name, String dataStream) {
            this.name = name;
            this.dataStream = dataStream;
        }
    }

    /**
     * Represents a merged output row where multiple data streams with the same
     * signature are combined into one row.
     */
    private static class MetricInfoRow {
        final String metricName;
        final Set<String> dataStreams = new HashSet<>();
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
     * Signature for merging rows. Data streams with the same signature are merged
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
     * Factory for INITIAL mode (data nodes): extracts metric metadata from shards.
     *
     * @param fieldLookup          on-demand lookup for metric field metadata
     * @param metadataSourceChannel channel index for {@code _timeseries_metadata} block
     * @param indexChannel         channel index for {@code _index} block
     */
    public record Factory(MetricFieldLookup fieldLookup, int metadataSourceChannel, int indexChannel) implements OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            return new MetricsInfoOperator(driverContext.blockFactory(), fieldLookup, metadataSourceChannel, indexChannel);
        }

        @Override
        public String describe() {
            return "MetricsInfoOperator[mode=INITIAL, metadataSourceChannel="
                + metadataSourceChannel
                + ", indexChannel="
                + indexChannel
                + "]";
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
            return new MetricsInfoOperator(driverContext.blockFactory(), channels);
        }

        @Override
        public String describe() {
            return "MetricsInfoOperator[mode=FINAL]";
        }
    }

    private final Map<MetricInfoKey, MetricInfo> metricsByKey = new LinkedHashMap<>();
    /** Accumulates merged rows in FINAL mode. Null in INITIAL mode. */
    private final Map<MetricSignature, MetricInfoRow> mergedRows;

    private final BlockFactory blockFactory;
    /** INITIAL-mode fields (null in FINAL mode). */
    private final MetricFieldLookup fieldLookup;
    private final int metadataSourceChannel;
    private final int indexChannel;
    /** FINAL-mode field: input channel indices for the 6 output columns. Null in INITIAL mode. */
    private final int[] finalChannels;

    private boolean finished = false;
    private boolean outputProduced = false;

    /**
     * Creates an INITIAL-mode operator (data nodes).
     */
    public MetricsInfoOperator(BlockFactory blockFactory, MetricFieldLookup fieldLookup, int metadataSourceChannel, int indexChannel) {
        this.blockFactory = blockFactory;
        this.fieldLookup = fieldLookup;
        this.metadataSourceChannel = metadataSourceChannel;
        this.indexChannel = indexChannel;
        this.finalChannels = null;
        this.mergedRows = null;
    }

    /**
     * Creates a FINAL-mode operator (coordinator).
     *
     * @param channels the 6 input channel indices mapping to
     *                 [metric_name, data_stream, unit, metric_type, field_type, dimension_fields]
     */
    public MetricsInfoOperator(BlockFactory blockFactory, int[] channels) {
        this.blockFactory = blockFactory;
        this.fieldLookup = null;
        this.metadataSourceChannel = -1;
        this.indexChannel = -1;
        this.finalChannels = channels;
        this.mergedRows = new LinkedHashMap<>();
    }

    private boolean isFinalMode() {
        return finalChannels != null;
    }

    @Override
    public boolean needsInput() {
        return finished == false;
    }

    @Override
    public void addInput(Page page) {
        if (isFinalMode()) {
            addInputFinal(page);
        } else {
            addInputInitial(page);
        }
    }

    /** INITIAL mode: extract metric metadata from _timeseries_metadata and _index blocks. */
    private void addInputInitial(Page page) {
        BytesRefBlock metadataSource = metadataSourceChannel >= 0 ? (BytesRefBlock) page.getBlock(metadataSourceChannel) : null;
        BytesRefBlock indexBlock = indexChannel >= 0 ? (BytesRefBlock) page.getBlock(indexChannel) : null;

        BytesRef indexScratch = new BytesRef();

        for (int p = 0; p < page.getPositionCount(); p++) {
            if (metadataSource == null || metadataSource.isNull(p)) {
                continue;
            }
            if (indexBlock == null || indexBlock.isNull(p)) {
                continue;
            }

            String indexName = indexBlock.getBytesRef(p, indexScratch).utf8ToString();
            String dataStreamName = resolveDataStreamName(indexName);
            Map<String, Object> metadata = parseMetadataSource(metadataSource, p);
            if (metadata == null) {
                continue;
            }

            collectAndAggregateFields(metadata, null, indexName, dataStreamName, new HashSet<>(), new HashSet<>());
        }

        page.releaseBlocks();
    }

    /** FINAL mode: read the 6-column output from data nodes and merge by metric signature. */
    private void addInputFinal(Page page) {
        BytesRefBlock nameBlock = page.getBlock(finalChannels[COL_METRIC_NAME]);
        BytesRefBlock dsBlock = page.getBlock(finalChannels[COL_DATA_STREAM]);
        BytesRefBlock unitBlock = page.getBlock(finalChannels[COL_UNIT]);
        BytesRefBlock mtBlock = page.getBlock(finalChannels[COL_METRIC_TYPE]);
        BytesRefBlock ftBlock = page.getBlock(finalChannels[COL_FIELD_TYPE]);
        BytesRefBlock dfBlock = page.getBlock(finalChannels[COL_DIMENSION_FIELDS]);

        for (int pos = 0; pos < page.getPositionCount(); pos++) {
            String metricName = readSingleValue(nameBlock, pos);
            if (metricName == null) {
                continue;
            }

            Set<String> units = readMultiValue(unitBlock, pos);
            Set<String> fieldTypes = readMultiValue(ftBlock, pos);
            Set<String> metricTypes = readMultiValue(mtBlock, pos);

            MetricSignature sig = new MetricSignature(metricName, units, fieldTypes, metricTypes);
            MetricInfoRow row = mergedRows.computeIfAbsent(
                sig,
                s -> new MetricInfoRow(s.metricName(), s.units(), s.fieldTypes(), s.metricTypes())
            );

            row.dataStreams.addAll(readMultiValue(dsBlock, pos));
            row.dimensionFieldKeys.addAll(readMultiValue(dfBlock, pos));
        }

        page.releaseBlocks();
    }

    private static String readSingleValue(BytesRefBlock block, int position) {
        if (block.isNull(position)) {
            return null;
        }
        return block.getBytesRef(position, new BytesRef()).utf8ToString();
    }

    private static Set<String> readMultiValue(BytesRefBlock block, int position) {
        Set<String> values = new HashSet<>();
        if (block.isNull(position)) {
            return values;
        }
        int start = block.getFirstValueIndex(position);
        int count = block.getValueCount(position);
        BytesRef scratch = new BytesRef();
        for (int i = 0; i < count; i++) {
            values.add(block.getBytesRef(start + i, scratch).utf8ToString());
        }
        return values;
    }

    /**
     * Recursively walks the parsed {@code _timeseries_metadata} JSON, classifying each leaf as
     * either a metric (via {@link #fieldLookup}) or a dimension key.
     *
     * @param metadata       the (possibly nested) metadata map for one tsid
     * @param prefix         dotted path prefix for the current nesting level ({@code null} at root)
     * @param indexName      concrete backing-index name – used for the field lookup (mapping is per backing index)
     * @param dataStreamName resolved data-stream name – used as the grouping key so that all
     *                       backing indices of the same data stream share a single {@link MetricInfo}
     * @param dimensionKeys  accumulates non-metric leaf keys found in this document
     * @param touchedMetrics accumulates the {@link MetricInfo} entries that were created or updated
     *                       by this document, so that dimension keys are only added to the metrics
     *                       that actually appeared in the same tsid (not all metrics ever seen)
     */
    @SuppressWarnings("unchecked")
    private void collectAndAggregateFields(
        Map<String, Object> metadata,
        String prefix,
        String indexName,
        String dataStreamName,
        Set<String> dimensionKeys,
        Set<MetricInfo> touchedMetrics
    ) {
        for (Map.Entry<String, Object> entry : metadata.entrySet()) {
            String key = prefix == null ? entry.getKey() : prefix + "." + entry.getKey();
            Object value = entry.getValue();

            // Check the metric lookup first, before inspecting the value type.
            // Some metric types (histogram, exponential_histogram, tdigest) produce
            // nested JSON in synthetic source. Without this early check, the method
            // would recurse into the nested structure and misclassify internal fields
            // (e.g. "values", "counts", "centroids") as dimension keys.
            MetricFieldInfo fieldInfo = fieldLookup.lookup(indexName, key);
            if (fieldInfo != null) {
                recordMetric(fieldInfo, dataStreamName, touchedMetrics);
            } else if (value instanceof Map<?, ?> nested) {
                collectAndAggregateFields((Map<String, Object>) nested, key, indexName, dataStreamName, dimensionKeys, touchedMetrics);
            } else {
                dimensionKeys.add(key);
            }
        }

        if (prefix == null && dimensionKeys.isEmpty() == false) {
            for (MetricInfo info : touchedMetrics) {
                info.dimensionFieldKeys.addAll(dimensionKeys);
            }
        }
    }

    /**
     * Records a metric field into the per-key metric map, grouping by (metricName, dataStreamName)
     * so that backing indices within the same data stream share one {@link MetricInfo} entry.
     * Conflicting unit/metric_type/field_type across backing indices of the same data stream
     * become multi-valued.
     */
    private void recordMetric(MetricFieldInfo fieldInfo, String dataStreamName, Set<MetricInfo> touchedMetrics) {
        MetricInfoKey infoKey = new MetricInfoKey(fieldInfo.name(), dataStreamName);
        MetricInfo info = metricsByKey.computeIfAbsent(infoKey, k -> new MetricInfo(k.metricName(), k.dataStreamName()));
        touchedMetrics.add(info);

        if (fieldInfo.unit() != null) {
            info.units.add(fieldInfo.unit());
        }
        if (fieldInfo.fieldType() != null) {
            info.fieldTypes.add(fieldInfo.fieldType());
        }
        if (fieldInfo.metricType() != null) {
            info.metricTypes.add(fieldInfo.metricType());
        }
    }

    /**
     * Matches the default backing-index / failure-store naming convention produced by
     * {@code DataStream#getDefaultIndexName}: {@code .ds-{name}-{yyyy.MM.dd}-{000001}}
     * (or the {@code .fs-} variant).
     * <p>
     * Group 1 captures the data-stream name.
     */
    private static final Pattern BACKING_INDEX_PATTERN = Pattern.compile("^\\.(?:ds|fs)-(.+)-\\d{4}\\.\\d{2}\\.\\d{2}-\\d{6}$");

    /**
     * Resolves the data-stream name from a concrete backing-index name.
     * <p>
     * If the name matches the standard format produced by
     * {@code DataStream#getDefaultIndexName} ({@code .ds-{name}-{yyyy.MM.dd}-{000001}}),
     * the data-stream name is extracted. Otherwise the raw index name is returned unchanged.
     */
    static String resolveDataStreamName(String indexName) {
        Matcher m = BACKING_INDEX_PATTERN.matcher(indexName);
        return m.matches() ? m.group(1) : indexName;
    }

    private List<MetricInfoRow> mergeRowsBySignature(Map<MetricInfoKey, MetricInfo> metricsByKey) {
        Map<MetricSignature, MetricInfoRow> bySignature = new LinkedHashMap<>();

        for (MetricInfo info : metricsByKey.values()) {
            MetricSignature sig = new MetricSignature(info.name, info.units, info.fieldTypes, info.metricTypes);
            MetricInfoRow row = bySignature.computeIfAbsent(
                sig,
                s -> new MetricInfoRow(s.metricName(), s.units(), s.fieldTypes(), s.metricTypes())
            );

            row.dataStreams.add(info.dataStream);
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

        if (isFinalMode()) {
            return mergedRows.isEmpty() ? createEmptyPage() : createOutputPageFromRows(new ArrayList<>(mergedRows.values()));
        }

        if (metricsByKey.isEmpty()) {
            return createEmptyPage();
        }

        return createOutputPageFromRows(mergeRowsBySignature(metricsByKey));
    }

    private Page createEmptyPage() {
        Block[] blocks = new Block[NUM_BLOCKS];
        try {
            for (int i = 0; i < blocks.length; i++) {
                blocks[i] = blockFactory.newConstantBytesRefBlockWith(new BytesRef(""), 0);
            }
            return new Page(0, blocks);
        } catch (Exception e) {
            Releasables.closeExpectNoException(blocks);
            throw e;
        }
    }

    private Page createOutputPageFromRows(List<MetricInfoRow> rows) {
        int rowCount = rows.size();

        Block[] blocks = new Block[NUM_BLOCKS];

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
                appendMultiValued(dsBuilder, row.dataStreams);
                appendMultiValued(unitBuilder, row.units);
                appendMultiValued(mtBuilder, row.metricTypes);
                appendMultiValued(ftBuilder, row.fieldTypes);
                appendMultiValued(dfBuilder, row.dimensionFieldKeys);
            }

            blocks[0] = nameBuilder.build();
            blocks[1] = dsBuilder.build();
            blocks[2] = unitBuilder.build();
            blocks[3] = mtBuilder.build();
            blocks[4] = ftBuilder.build();
            blocks[5] = dfBuilder.build();

            return new Page(rowCount, blocks);
        } catch (Exception e) {
            Releasables.closeExpectNoException(blocks);
            throw e;
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

    private Map<String, Object> parseMetadataSource(BytesRefBlock metadataSource, int position) {
        if (metadataSource == null || metadataSource.isNull(position)) {
            return null;
        }
        BytesRef bytes = metadataSource.getBytesRef(position, new BytesRef());
        try (
            var parser = XContentType.JSON.xContent()
                .createParser(XContentParserConfiguration.EMPTY, bytes.bytes, bytes.offset, bytes.length)
        ) {
            parser.nextToken();
            return parser.mapOrdered();
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public void close() {}

    @Override
    public String toString() {
        return "MetricsInfoOperator[mode=" + (isFinalMode() ? "FINAL" : "INITIAL") + "]";
    }
}
