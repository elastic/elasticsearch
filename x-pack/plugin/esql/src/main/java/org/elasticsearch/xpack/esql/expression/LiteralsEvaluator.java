/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;

public record LiteralsEvaluator(BlockFactory blockFactory, Literal lit) implements ExpressionEvaluator {
    public record Factory(Literal lit) implements ExpressionEvaluator.Factory {
        @Override
        public ExpressionEvaluator get(DriverContext context) {
            return new LiteralsEvaluator(context.blockFactory(), lit);
        }

        @Override
        public String toString() {
            return "LiteralsEvaluator[lit=" + lit + "]";
        }

        @Override
        public boolean eagerEvalSafeInLazy() {
            return true;
        }
    }

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(LiteralsEvaluator.class);

    @Override
    public Block eval(Page page) {
        return BlockUtils.constantBlock(blockFactory, lit.value(), page.getPositionCount());
    }

    @Override
    public String toString() {
        return "LiteralsEvaluator[lit=" + lit + "]";
    }

    @Override
    public long baseRamBytesUsed() {
        return BASE_RAM_BYTES_USED + lit.ramBytesUsed();
    }

    @Override
    public void close() {}
}
