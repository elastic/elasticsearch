/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.expression;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

public record LiteralsEvaluator(BlockFactory blockFactory, Object value) implements ExpressionEvaluator {
    public record Factory(Object value) implements ExpressionEvaluator.Factory {
        @Override
        public ExpressionEvaluator get(DriverContext context) {
            return new LiteralsEvaluator(context.blockFactory(), value);
        }

        @Override
        public String toString() {
            return "LiteralsEvaluator[lit=" + value + "]";
        }

        @Override
        public boolean eagerEvalSafeInLazy() {
            return true;
        }
    }

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(LiteralsEvaluator.class);

    @Override
    public Block eval(Page page) {
        return BlockUtils.constantBlock(blockFactory, value, page.getPositionCount());
    }

    @Override
    public String toString() {
        return "LiteralsEvaluator[lit=" + value + ']';
    }

    @Override
    public long baseRamBytesUsed() {
        long ramBytesUsed = BASE_RAM_BYTES_USED;
        if (value instanceof BytesRef b) {
            ramBytesUsed += b.length;
        } else {
            ramBytesUsed += RamUsageEstimator.sizeOfObject(value);
        }
        return ramBytesUsed;
    }

    @Override
    public void close() {}
}
