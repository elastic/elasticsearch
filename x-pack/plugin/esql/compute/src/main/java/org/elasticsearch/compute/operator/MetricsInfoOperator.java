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

/**
 * Operator for the ESQL {@code METRICS_INFO} command.
 * <p>
 * Expects deduplicated input (one row per {@code _tsid}) from upstream {@link DistinctByOperator},
 * with blocks for {@code _timeseries_metadata} and {@code _index}. Produces one row per distinct
 * metric signature, with metadata aggregated from all matching documents.
 *
 * <h2>Processing</h2>
 * <ol>
 *   <li><b>During {@link #addInput(Page)}:</b> For each row, parses the {@code _timeseries_metadata}
 *       JSON and walks the metadata map. For each leaf key, {@link MetricFieldLookup} is used to
 *       determine if it is a metric field; if so, it is aggregated into an internal map keyed by
 *       {@code (metricName, dataStreamName)}, where {@code dataStreamName} is the index name from
 *       {@code _index}. Non-metric leaf keys are collected as dimension keys and associated with
 *       all metrics seen in that document.</li>
 *   <li><b>When building output:</b> In {@link #getOutput()}, {@link #createOutputPage()} runs once.
 *       It merges aggregated entries by {@link MetricSignature} (metric name plus units, field
 *       types, and metric types). Data streams that share the same signature are combined into one
 *       output row with multi-valued {@code data_stream} and a union of {@code dimension_fields}.</li>
 * </ol>
 *
 * <h2>Output columns</h2>
 * <ul>
 *   <li>{@code metric_name} – keyword (single-valued)</li>
 *   <li>{@code data_stream} – keyword (multi-valued); indices/data streams that have this metric
 *       with the same signature</li>
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

    /**
     * Factory for creating {@link MetricsInfoOperator} instances.
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
            return "MetricsInfoOperator[metadataSourceChannel=" + metadataSourceChannel + ", indexChannel=" + indexChannel + "]";
        }
    }

    private final Map<MetricInfoKey, MetricInfo> metricsByKey = new LinkedHashMap<>();

    private final BlockFactory blockFactory;
    private final MetricFieldLookup fieldLookup;
    private final int metadataSourceChannel;
    private final int indexChannel;

    private boolean finished = false;
    private boolean outputProduced = false;

    /**
     * Creates a METRICS_INFO operator.
     *
     * @param blockFactory         factory for building output blocks
     * @param fieldLookup          on-demand lookup for metric field metadata
     * @param metadataSourceChannel channel index for {@code _timeseries_metadata} block (-1 if absent)
     * @param indexChannel         channel index for {@code _index} block (-1 if absent)
     */
    public MetricsInfoOperator(BlockFactory blockFactory, MetricFieldLookup fieldLookup, int metadataSourceChannel, int indexChannel) {
        this.blockFactory = blockFactory;
        this.fieldLookup = fieldLookup;
        this.metadataSourceChannel = metadataSourceChannel;
        this.indexChannel = indexChannel;
    }

    @Override
    public boolean needsInput() {
        return finished == false;
    }

    @Override
    public void addInput(Page page) {
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
            Map<String, Object> metadata = parseMetadataSource(metadataSource, p);
            if (metadata == null) {
                continue;
            }

            collectAndAggregateFields(metadata, null, indexName, new HashSet<>());
        }

        page.releaseBlocks();
    }

    @SuppressWarnings("unchecked")
    private void collectAndAggregateFields(Map<String, Object> metadata, String prefix, String indexName, Set<String> dimensionKeys) {
        for (Map.Entry<String, Object> entry : metadata.entrySet()) {
            String key = prefix == null ? entry.getKey() : prefix + "." + entry.getKey();
            Object value = entry.getValue();
            if (value instanceof Map<?, ?> nested) {
                collectAndAggregateFields((Map<String, Object>) nested, key, indexName, dimensionKeys);
            } else {
                MetricFieldInfo fieldInfo = fieldLookup.lookup(indexName, key);
                if (fieldInfo != null) {
                    // Step 1. Group by (metricName, dataStreamName)
                    MetricInfoKey infoKey = new MetricInfoKey(fieldInfo.name(), indexName);
                    MetricInfo info = metricsByKey.computeIfAbsent(infoKey, k -> new MetricInfo(k.metricName(), k.dataStreamName()));

                    if (fieldInfo.unit() != null) {
                        info.units.add(fieldInfo.unit());
                    }
                    if (fieldInfo.fieldType() != null) {
                        info.fieldTypes.add(fieldInfo.fieldType());
                    }
                    if (fieldInfo.metricType() != null) {
                        info.metricTypes.add(fieldInfo.metricType());
                    }
                } else {
                    dimensionKeys.add(key);
                }
            }
        }

        if (prefix == null && dimensionKeys.isEmpty() == false) {
            for (MetricInfo info : metricsByKey.values()) {
                info.dimensionFieldKeys.addAll(dimensionKeys);
            }
        }
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
    public Page getOutput() {
        if (finished == false || outputProduced) {
            return null;
        }

        outputProduced = true;

        if (metricsByKey.isEmpty()) {
            return createEmptyPage();
        }

        return createOutputPage();
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

    private Page createOutputPage() {
        // Step 2: Merge data streams with the same signature into one row
        List<MetricInfoRow> rows = mergeRowsBySignature(metricsByKey);
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
        return "MetricsInfoOperator[]";
    }
}
