/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongRangeBlock;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;

public class ValueExtractorForLongRange implements ValueExtractor {
    private final LongRangeBlock block;

    ValueExtractorForLongRange(TopNEncoder encoder, LongRangeBlock block) {
        assert encoder == TopNEncoder.DEFAULT_UNSORTABLE;
        this.block = block;
    }

    @Override
    public void writeValue(BreakingBytesRefBuilder values, int position) {
        LongBlock fromBlock = block.getFromBlock();
        LongBlock toBlock = block.getToBlock();
        int count = fromBlock.getValueCount(position);
        TopNEncoder.DEFAULT_UNSORTABLE.encodeVInt(count, values);
        if (count == 0) {
            return;
        }
        int start = fromBlock.getFirstValueIndex(position);
        int end = start + count;
        for (int i = start; i < end; i++) {
            TopNEncoder.DEFAULT_UNSORTABLE.encodeLong(fromBlock.getLong(i), values);
            TopNEncoder.DEFAULT_UNSORTABLE.encodeLong(toBlock.getLong(i), values);
        }
    }

    @Override
    public String toString() {
        return "ValueExtractorForLongRange";
    }
}
