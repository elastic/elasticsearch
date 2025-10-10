/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.compute.data.DateRangeBlock;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;

public class ValueExtractorForDateRange implements ValueExtractor {
    private final DateRangeBlock block;

    ValueExtractorForDateRange(TopNEncoder encoder, DateRangeBlock block) {
        assert encoder == TopNEncoder.DEFAULT_UNSORTABLE;
        this.block = block;
    }

    @Override
    public void writeValue(BreakingBytesRefBuilder values, int position) {
        TopNEncoder.DEFAULT_UNSORTABLE.encodeVInt(1, values);
        if (block.isNull(position)) {
            TopNEncoder.DEFAULT_UNSORTABLE.encodeBoolean(false, values);
        } else {
            TopNEncoder.DEFAULT_UNSORTABLE.encodeBoolean(true, values);
            TopNEncoder.DEFAULT_UNSORTABLE.encodeDouble(block.getFromBlock().getLong(position), values);
            TopNEncoder.DEFAULT_UNSORTABLE.encodeDouble(block.getToBlock().getLong(position), values);
        }
    }

    @Override
    public String toString() {
        return "ValueExtractorForDateRange";
    }
}
