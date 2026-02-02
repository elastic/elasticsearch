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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Operator for METRICS_INFO command.
 * The METRICS_INFO command returns one row per metric that matches all conditions and attaches metadata to it.
 * In cases where the same metric is defined with different properties in different indices, the same metric may appear twice in the output.
 * Output columns:
 * - metric_name: keyword (single-valued)
 * - data_stream: keyword (multi-valued) - data streams that have this metric with this unit
 * - unit: keyword (single-valued for this row, can be null)
 * - metric_type: keyword (multi-valued if conflicts within data streams)
 * - field_type: keyword (multi-valued if conflicts within data streams)
 * - dimension_fields: keyword (multi-valued) - union of all dimension keys
 */
public class MetricsInfoOperator implements Operator {

    public record Factory(List<MetricFieldInfo> metricFields, Set<String> dimensionFields) implements OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            return new MetricsInfoOperator(driverContext.blockFactory(), metricFields, dimensionFields);
        }

        @Override
        public String describe() {
            return "MetricsInfoOperator[metricFields=" + metricFields.size() + "]";
        }
    }

    private final BlockFactory blockFactory;
    private final List<MetricFieldInfo> metricFields;
    private final Set<String> dimensionFields;

    private boolean finished = false;
    private boolean outputProduced = false;

    public MetricsInfoOperator(BlockFactory blockFactory, List<MetricFieldInfo> metricFields, Set<String> dimensionFields) {
        this.blockFactory = blockFactory;
        this.metricFields = metricFields;
        this.dimensionFields = dimensionFields;
    }

    @Override
    public boolean needsInput() {
        return finished == false;
    }

    @Override
    public void addInput(Page page) {
        // No per-document processing needed. Data comes from shard contexts at planning time
        page.releaseBlocks();
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

        if (metricFields.isEmpty()) {
            return createEmptyPage();
        }

        return createOutputPage();
    }

    private Page createEmptyPage() {
        Block[] blocks = new Block[6];
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
        record MetricKey(String name, String unit) {}

        Map<MetricKey, MetricInfo> metricsByKey = new LinkedHashMap<>();
        for (MetricFieldInfo fieldInfo : metricFields) {
            MetricKey key = new MetricKey(fieldInfo.name(), fieldInfo.unit());
            MetricInfo info = metricsByKey.computeIfAbsent(key, k -> new MetricInfo(k.name(), k.unit()));

            if (fieldInfo.indexName() != null) {
                info.dataStreams.add(fieldInfo.indexName());
            }

            if (fieldInfo.fieldType() != null) {
                info.fieldTypes.add(fieldInfo.fieldType());
            }
            if (fieldInfo.metricType() != null) {
                info.metricTypes.add(fieldInfo.metricType());
            }

            info.dimensionFieldKeys.addAll(dimensionFields);
        }

        List<MetricInfo> metrics = new ArrayList<>(metricsByKey.values());
        int rowCount = metrics.size();

        Block[] blocks = new Block[6];
        try {
            // Build metric_name column (single-valued keyword)
            try (BytesRefBlock.Builder nameBuilder = blockFactory.newBytesRefBlockBuilder(rowCount)) {
                for (MetricInfo metric : metrics) {
                    nameBuilder.appendBytesRef(new BytesRef(metric.name));
                }
                blocks[0] = nameBuilder.build();
            }

            // Build data_stream column (multi-valued keyword) - data streams that have this metric with this unit
            try (BytesRefBlock.Builder dsBuilder = blockFactory.newBytesRefBlockBuilder(rowCount)) {
                for (MetricInfo metric : metrics) {
                    appendMultiValued(dsBuilder, metric.dataStreams);
                }
                blocks[1] = dsBuilder.build();
            }

            // Build unit column (single-valued keyword, can be null)
            try (BytesRefBlock.Builder unitBuilder = blockFactory.newBytesRefBlockBuilder(rowCount)) {
                for (MetricInfo metric : metrics) {
                    if (metric.unit == null) {
                        unitBuilder.appendNull();
                    } else {
                        unitBuilder.appendBytesRef(new BytesRef(metric.unit));
                    }
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

            // Build dimension_fields column (multi-valued keyword) - union of all dimension keys
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
        final String unit;
        final Set<String> dataStreams = new HashSet<>();
        final Set<String> metricTypes = new HashSet<>();
        final Set<String> fieldTypes = new HashSet<>();
        final Set<String> dimensionFieldKeys = new HashSet<>();

        MetricInfo(String name, String unit) {
            this.name = name;
            this.unit = unit;
        }
    }

    @Override
    public void close() {}

    @Override
    public String toString() {
        return "MetricsInfoOperator[metrics=" + metricFields.size() + "]";
    }

}
