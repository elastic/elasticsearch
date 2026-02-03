/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

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
        TopNEncoder.DEFAULT_UNSORTABLE.encodeVInt(1, values);
        if (block.getFromBlock().isNull(position)) {
            TopNEncoder.DEFAULT_UNSORTABLE.encodeBoolean(false, values);
        } else {
            TopNEncoder.DEFAULT_UNSORTABLE.encodeBoolean(true, values);
            TopNEncoder.DEFAULT_UNSORTABLE.encodeLong(block.getFromBlock().getLong(position), values);
        }
        if (block.getToBlock().isNull(position)) {
            TopNEncoder.DEFAULT_UNSORTABLE.encodeBoolean(false, values);
        } else {
            TopNEncoder.DEFAULT_UNSORTABLE.encodeBoolean(true, values);
            TopNEncoder.DEFAULT_UNSORTABLE.encodeLong(block.getToBlock().getLong(position), values);
        }
    }

    @Override
    public String toString() {
        return "ValueExtractorForLongRange";
    }
}
