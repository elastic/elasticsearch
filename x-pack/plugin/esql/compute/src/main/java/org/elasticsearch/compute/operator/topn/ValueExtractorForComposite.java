/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.compute.data.AggregateMetricDoubleBlockBuilder;
import org.elasticsearch.compute.data.CompositeBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;

import java.util.List;

public class ValueExtractorForComposite implements ValueExtractor {
    private final CompositeBlock block;

    ValueExtractorForComposite(TopNEncoder encoder, CompositeBlock block) {
        assert encoder == TopNEncoder.DEFAULT_UNSORTABLE;
        this.block = block;
    }

    @Override
    public void writeValue(BreakingBytesRefBuilder values, int position) {
        if (block.getBlockCount() != AggregateMetricDoubleBlockBuilder.Metric.values().length) {
            throw new UnsupportedOperationException("Composite Blocks for non-aggregate-metric-doubles do not have value extractors");
        }
        for (AggregateMetricDoubleBlockBuilder.Metric metric : List.of(
            AggregateMetricDoubleBlockBuilder.Metric.MIN,
            AggregateMetricDoubleBlockBuilder.Metric.MAX,
            AggregateMetricDoubleBlockBuilder.Metric.SUM
        )) {
            DoubleBlock doubleBlock = block.getBlock(metric.getIndex());
            if (doubleBlock.isNull(position)) {
                TopNEncoder.DEFAULT_UNSORTABLE.encodeBoolean(false, values);
            } else {
                TopNEncoder.DEFAULT_UNSORTABLE.encodeBoolean(true, values);
                TopNEncoder.DEFAULT_UNSORTABLE.encodeDouble(doubleBlock.getDouble(position), values);
            }
        }
        IntBlock intBlock = block.getBlock(AggregateMetricDoubleBlockBuilder.Metric.COUNT.getIndex());
        if (intBlock.isNull(position)) {
            TopNEncoder.DEFAULT_UNSORTABLE.encodeBoolean(false, values);
        } else {
            TopNEncoder.DEFAULT_UNSORTABLE.encodeBoolean(true, values);
            TopNEncoder.DEFAULT_UNSORTABLE.encodeInt(intBlock.getInt(position), values);
        }
    }

    @Override
    public String toString() {
        return "ValueExtractorForComposite";
    }
}
