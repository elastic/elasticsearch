/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.compute.data.AggregateMetricDoubleBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;

import java.util.List;

public class ValueExtractorForAggregateMetricDouble implements ValueExtractor {
    private final AggregateMetricDoubleBlock block;

    ValueExtractorForAggregateMetricDouble(TopNEncoder encoder, AggregateMetricDoubleBlock block) {
        assert encoder == TopNEncoder.DEFAULT_UNSORTABLE;
        this.block = block;
    }

    @Override
    public void writeValue(BreakingBytesRefBuilder values, int position) {
        for (DoubleBlock doubleBlock : List.of(block.minBlock(), block.maxBlock(), block.sumBlock())) {
            if (doubleBlock.isNull(position)) {
                TopNEncoder.DEFAULT_UNSORTABLE.encodeBoolean(false, values);
            } else {
                TopNEncoder.DEFAULT_UNSORTABLE.encodeBoolean(true, values);
                TopNEncoder.DEFAULT_UNSORTABLE.encodeDouble(doubleBlock.getDouble(position), values);
            }
        }
        IntBlock intBlock = block.countBlock();
        if (intBlock.isNull(position)) {
            TopNEncoder.DEFAULT_UNSORTABLE.encodeBoolean(false, values);
        } else {
            TopNEncoder.DEFAULT_UNSORTABLE.encodeBoolean(true, values);
            TopNEncoder.DEFAULT_UNSORTABLE.encodeInt(intBlock.getInt(position), values);
        }
    }

    @Override
    public String toString() {
        return "ValueExtractorForAggregateMetricDouble";
    }
}
