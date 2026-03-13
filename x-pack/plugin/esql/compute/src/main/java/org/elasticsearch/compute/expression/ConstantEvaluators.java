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
 * {@link ExpressionEvaluator}s returning constants and their {@link ExpressionEvaluator.Factory factories}.
 */
public class ConstantEvaluators {
    public static final ExpressionEvaluator.Factory CONSTANT_NULL_FACTORY = new ConstantNullEvaluator.Factory();
    public static final ExpressionEvaluator.Factory CONSTANT_TRUE_FACTORY = new ConstantTrueEvaluator.Factory();
    public static final ExpressionEvaluator.Factory CONSTANT_FALSE_FACTORY = new ConstantFalseEvaluator.Factory();

    private ConstantEvaluators() {}

    private record ConstantNullEvaluator(DriverContext context) implements ExpressionEvaluator {
        private static final String NAME = "ConstantNull";
        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ConstantNullEvaluator.class);

        @Override
        public Block eval(Page page) {
            return context.blockFactory().newConstantNullBlock(page.getPositionCount());
        }

        @Override
        public void close() {}

        @Override
        public String toString() {
            return NAME;
        }

        @Override
        public long baseRamBytesUsed() {
            return BASE_RAM_BYTES_USED;
        }

        record Factory() implements ExpressionEvaluator.Factory {
            @Override
            public ConstantNullEvaluator get(DriverContext context) {
                return new ConstantNullEvaluator(context);
            };

            @Override
            public String toString() {
                return NAME;
            }
        };
    }

    private record ConstantTrueEvaluator(DriverContext context) implements ExpressionEvaluator {
        private static final String NAME = "ConstantTrue";
        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ConstantTrueEvaluator.class);

        @Override
        public Block eval(Page page) {
            return context.blockFactory().newConstantBooleanBlockWith(true, page.getPositionCount());
        }

        @Override
        public void close() {}

        @Override
        public String toString() {
            return NAME;
        }

        @Override
        public long baseRamBytesUsed() {
            return BASE_RAM_BYTES_USED;
        }

        record Factory() implements ExpressionEvaluator.Factory {
            @Override
            public ConstantTrueEvaluator get(DriverContext context) {
                return new ConstantTrueEvaluator(context);
            };

            @Override
            public String toString() {
                return NAME;
            }
        };
    }

    private record ConstantFalseEvaluator(DriverContext context) implements ExpressionEvaluator {
        private static final String NAME = "ConstantFalse";
        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ConstantFalseEvaluator.class);

        @Override
        public Block eval(Page page) {
            return context.blockFactory().newConstantBooleanBlockWith(false, page.getPositionCount());
        }

        @Override
        public void close() {}

        @Override
        public String toString() {
            return NAME;
        }

        @Override
        public long baseRamBytesUsed() {
            return BASE_RAM_BYTES_USED;
        }

        record Factory() implements ExpressionEvaluator.Factory {
            @Override
            public ConstantFalseEvaluator get(DriverContext context) {
                return new ConstantFalseEvaluator(context);
            };

            @Override
            public String toString() {
                return NAME;
            }
        };
    }
}
