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
import java.util.Map;
import java.util.Set;

/**
 * Operator for the ESQL {@code METRICS_INFO} command.
 * <p>
 * Returns one row per metric that matches all conditions and attaches metadata to it.
 * Expects deduplicated input (one row per {@code _tsid}) from upstream {@link DistinctByOperator}.
 * Aggregates metrics during {@link #addInput(Page)} without tracking {@code _tsid}.
 *
 * <h2>Output columns</h2>
 * <ul>
 *   <li>{@code metric_name} – keyword (single-valued)</li>
 *   <li>{@code data_stream} – keyword (multi-valued); indices/data streams that have this metric</li>
 *   <li>{@code unit} – keyword (multi-valued if different backing indices have different units; may be null)</li>
 *   <li>{@code metric_type} – keyword (multi-valued if conflicts within data streams)</li>
 *   <li>{@code field_type} – keyword (multi-valued if conflicts within data streams)</li>
 *   <li>{@code dimension_fields} – keyword (multi-valued); union of all dimension keys</li>
 * </ul>
 */
public class MetricsInfoOperator implements Operator {

    public static final int NUM_BLOCKS = 6;

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

    private final BlockFactory blockFactory;
    private final MetricFieldLookup fieldLookup;
    private final int metadataSourceChannel;
    private final int indexChannel;

    private final Map<String, MetricInfo> metricsByKey = new LinkedHashMap<>();
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
            Map<String, Object> metadataMap = parseMetadataSource(metadataSource, p);
            if (metadataMap == null) {
                continue;
            }

            collectAndAggregateFields(metadataMap, null, indexName, new HashSet<>());
        }

        page.releaseBlocks();
    }

    @SuppressWarnings("unchecked")
    private void collectAndAggregateFields(Map<String, Object> map, String prefix, String indexName, Set<String> dimensionKeys) {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String key = prefix == null ? entry.getKey() : prefix + "." + entry.getKey();
            Object value = entry.getValue();
            if (value instanceof Map<?, ?> nested) {
                collectAndAggregateFields((Map<String, Object>) nested, key, indexName, dimensionKeys);
            } else {
                MetricFieldInfo fieldInfo = fieldLookup.lookup(indexName, key);
                if (fieldInfo != null) {
                    MetricInfo info = metricsByKey.computeIfAbsent(fieldInfo.name(), MetricInfo::new);

                    info.dataStreams.add(indexName);
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
                if (info.dataStreams.contains(indexName)) {
                    info.dimensionFieldKeys.addAll(dimensionKeys);
                }
            }
        }
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
        ArrayList<MetricInfo> metrics = new ArrayList<>(metricsByKey.values());
        int rowCount = metrics.size();

        Block[] blocks = new Block[NUM_BLOCKS];
        try {
            // Build metric_name column (single-valued keyword)
            try (BytesRefBlock.Builder nameBuilder = blockFactory.newBytesRefBlockBuilder(rowCount)) {
                for (MetricInfo metric : metrics) {
                    nameBuilder.appendBytesRef(new BytesRef(metric.name));
                }
                blocks[0] = nameBuilder.build();
            }

            // Build data_stream column (multi-valued keyword)
            try (BytesRefBlock.Builder dsBuilder = blockFactory.newBytesRefBlockBuilder(rowCount)) {
                for (MetricInfo metric : metrics) {
                    appendMultiValued(dsBuilder, metric.dataStreams);
                }
                blocks[1] = dsBuilder.build();
            }

            // Build unit column (multi-valued if different backing indices have different units)
            try (BytesRefBlock.Builder unitBuilder = blockFactory.newBytesRefBlockBuilder(rowCount)) {
                for (MetricInfo metric : metrics) {
                    appendMultiValued(unitBuilder, metric.units);
                }
                blocks[2] = unitBuilder.build();
            }

            // Build metric_type column (multi-valued keyword if conflicts)
            try (BytesRefBlock.Builder mtBuilder = blockFactory.newBytesRefBlockBuilder(rowCount)) {
                for (MetricInfo metric : metrics) {
                    appendMultiValued(mtBuilder, metric.metricTypes);
                }
                blocks[3] = mtBuilder.build();
            }

            // Build field_type column (multi-valued keyword if conflicts)
            try (BytesRefBlock.Builder ftBuilder = blockFactory.newBytesRefBlockBuilder(rowCount)) {
                for (MetricInfo metric : metrics) {
                    appendMultiValued(ftBuilder, metric.fieldTypes);
                }
                blocks[4] = ftBuilder.build();
            }

            // Build dimension_fields column (multi-valued keyword)
            try (BytesRefBlock.Builder dfBuilder = blockFactory.newBytesRefBlockBuilder(rowCount)) {
                for (MetricInfo metric : metrics) {
                    appendMultiValued(dfBuilder, metric.dimensionFieldKeys);
                }
                blocks[5] = dfBuilder.build();
            }

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

    private static class MetricInfo {
        final String name;
        final Set<String> units = new HashSet<>();
        final Set<String> dataStreams = new HashSet<>();
        final Set<String> metricTypes = new HashSet<>();
        final Set<String> fieldTypes = new HashSet<>();
        final Set<String> dimensionFieldKeys = new HashSet<>();

        MetricInfo(String name) {
            this.name = name;
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
