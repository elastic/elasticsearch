/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.HexFormat;

/**
 * String evaluator for to_dense_vector function. Converts a hexadecimal string to a dense_vector of bytes.
 * Cannot be automatically generated as it generates multivalues for a single hex string, representing the dense_vector byte array.
 */
class ToDenseVectorFromStringEvaluator extends AbstractConvertFunction.AbstractEvaluator {
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ToDenseVectorFromStringEvaluator.class);

    private final EvalOperator.ExpressionEvaluator field;

    ToDenseVectorFromStringEvaluator(Source source, EvalOperator.ExpressionEvaluator field, DriverContext driverContext) {
        super(driverContext, source);
        this.field = field;
    }

    @Override
    protected EvalOperator.ExpressionEvaluator next() {
        return field;
    }

    @Override
    protected Block evalVector(Vector v) {
        return evalBlock(v.asBlock());
    }

    @Override
    public Block evalBlock(Block b) {
        BytesRefBlock block = (BytesRefBlock) b;
        int positionCount = block.getPositionCount();
        int dimensions = 0;
        BytesRef scratch = new BytesRef();
        try (FloatBlock.Builder builder = driverContext.blockFactory().newFloatBlockBuilder(positionCount * dimensions)) {
            for (int p = 0; p < positionCount; p++) {
                if (block.isNull(p)) {
                    builder.appendNull();
                } else {
                    scratch = block.getBytesRef(p, scratch);
                    try {
                        byte[] bytes = HexFormat.of().parseHex(scratch.utf8ToString());
                        if (bytes.length == 0) {
                            builder.appendNull();
                            continue;
                        }
                        if (dimensions == 0) {
                            dimensions = bytes.length;
                        } else {
                            if (bytes.length != dimensions) {
                                throw new IllegalArgumentException(
                                    "All dense_vector must have the same number of dimensions. Expected: "
                                        + dimensions
                                        + ", found: "
                                        + bytes.length
                                );
                            }
                        }
                        builder.beginPositionEntry();
                        for (byte value : bytes) {
                            builder.appendFloat(value);
                        }
                        builder.endPositionEntry();
                    } catch (IllegalArgumentException e) {
                        registerException(e);
                        builder.appendNull();
                    }
                }
            }
            return builder.build();
        }
    }

    @Override
    public String toString() {
        return "ToDenseVectorFromStringEvaluator[s=" + field + ']';
    }

    @Override
    public long baseRamBytesUsed() {
        return BASE_RAM_BYTES_USED + field.baseRamBytesUsed();
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(field);
    }

    static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
        private final Source source;
        private final EvalOperator.ExpressionEvaluator.Factory field;

        Factory(Source source, EvalOperator.ExpressionEvaluator.Factory field) {
            this.source = source;
            this.field = field;
        }

        @Override
        public EvalOperator.ExpressionEvaluator get(DriverContext context) {
            return new ToDenseVectorFromStringEvaluator(source, field.get(context), context);
        }

        @Override
        public String toString() {
            return "ToDenseVectorFromStringEvaluator[s=" + field + ']';
        }
    }
}
