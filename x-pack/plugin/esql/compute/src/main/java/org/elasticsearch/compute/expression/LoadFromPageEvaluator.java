/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.expression;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link ExpressionEvaluator} that loads from the {@link Page} directly.
 */
public record LoadFromPageEvaluator(int channel) implements ExpressionEvaluator {
    public record Factory(int channel) implements ExpressionEvaluator.Factory {
        @Override
        public ExpressionEvaluator get(DriverContext driverContext) {
            return new LoadFromPageEvaluator(channel);
        }

        @Override
        public String toString() {
            /*
             * ESQL uses this name for the output of a command, and we use it here
             * because it's the name we've always used. It'd be more descriptive
             * to make it LoadChannel, but that'd change a ton of tests. And, like,
             * traditions are fun.
             */
            return "Attribute[channel=" + channel + "]";
        }

        @Override
        public boolean eagerEvalSafeInLazy() {
            return true;
        }
    }

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(LoadFromPageEvaluator.class);

    @Override
    public Block eval(Page page) {
        Block block = page.getBlock(channel);
        block.incRef();
        return block;
    }

    @Override
    public long baseRamBytesUsed() {
        return BASE_RAM_BYTES_USED;
    }

    @Override
    public void close() {}

    @Override
    public String toString() {
        /*
         * ESQL uses this name for the output of a command, and we use it here
         * because it's the name we've always used. It'd be more descriptive
         * to make it LoadChannel, but that'd change a ton of tests. And, like,
         * traditions are fun.
         */
        return "Attribute[channel=" + channel + "]";
    }
}
