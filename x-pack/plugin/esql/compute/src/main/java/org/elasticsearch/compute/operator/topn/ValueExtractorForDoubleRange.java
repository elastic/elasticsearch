/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleRangeBlock;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;

public class ValueExtractorForDoubleRange implements ValueExtractor {
    private final DoubleRangeBlock block;

    ValueExtractorForDoubleRange(TopNEncoder encoder, DoubleRangeBlock block) {
        assert encoder == TopNEncoder.DEFAULT_UNSORTABLE;
        this.block = block;
    }

    @Override
    public void writeValue(BreakingBytesRefBuilder values, int position) {
        DoubleBlock fromBlock = block.getDoubleFromBlock();
        DoubleBlock toBlock = block.getDoubleToBlock();
        int count = fromBlock.getValueCount(position);
        TopNEncoder.DEFAULT_UNSORTABLE.encodeVInt(count, values);
        if (count == 0) {
            return;
        }
        int start = fromBlock.getFirstValueIndex(position);
        int end = start + count;
        for (int i = start; i < end; i++) {
            TopNEncoder.DEFAULT_UNSORTABLE.encodeDouble(fromBlock.getDouble(i), values);
            TopNEncoder.DEFAULT_UNSORTABLE.encodeDouble(toBlock.getDouble(i), values);
        }
    }

    @Override
    public String toString() {
        return "ValueExtractorForDoubleRange";
    }
}
