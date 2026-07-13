/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.lookup;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;

/**
 * Emit page of boolean values where corresponding position in specified channel contains a single value.
 * Used in AbstractLookupService to filter out false-positive matches when using BulkKeywordLookup optimization.
 */
public record BulkLookupSingleValued(DriverContext context, int channelOffset, Warnings warnings) implements ExpressionEvaluator {
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(BulkLookupSingleValued.class);

    @Override
    public Block eval(Page page) {
        final Block block = page.getBlock(channelOffset);
        final int positionCount = block.getPositionCount();
        final BooleanVector.FixedBuilder singles = context.blockFactory().newBooleanVectorFixedBuilder(positionCount);

        boolean encounteredMultiValue = false;
        for (int p = 0; p < positionCount; p++) {
            final int valueCount = block.getValueCount(p);
            if (valueCount > 1) {
                encounteredMultiValue = true;
            }
            singles.appendBoolean(valueCount == 1);
        }
        if (encounteredMultiValue) {
            warnings.registerException(new IllegalArgumentException("LOOKUP JOIN encountered multi-value"));
        }

        final Block result = singles.build().asBlock();
        return result;
    }

    @Override
    public long baseRamBytesUsed() {
        return BASE_RAM_BYTES_USED;
    }

    @Override
    public String toString() {
        return "BulkLookupSingleValued[channelOffset=" + channelOffset + ']';
    }

    @Override
    public void close() {}
}
