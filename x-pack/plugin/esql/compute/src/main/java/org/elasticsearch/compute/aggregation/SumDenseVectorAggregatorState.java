/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.operator.DriverContext;

class SumDenseVectorAggregatorState implements AggregatorState {

    private float[] sum;
    private boolean seen = false;

    SumDenseVectorAggregatorState() {}

    void add(FloatBlock vector, int startPosition, int dimensions) {
        if (vector == null) {
            return;
        }
        if (sum == null) {
            sum = new float[dimensions];
        }
        if (sum.length != dimensions) {
            throw new IllegalArgumentException(
                "Cannot sum dense vectors with different dimensions: expected [" + sum.length + "] but got [" + dimensions + "]"
            );
        }
        for (int i = 0; i < dimensions; i++) {
            sum[i] += vector.getFloat(startPosition + i);
        }
        seen = true;
    }

    float[] getSum() {
        return sum;
    }

    boolean getSeen() {
        return seen;
    }

    public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
        if (seen) {
            assert sum != null : "Sum vector must not be null if seen";
            try (var builder = driverContext.blockFactory().newFloatBlockBuilder(sum.length)) {
                if (sum != null) {
                    builder.beginPositionEntry();
                    for (float f : sum) {
                        builder.appendFloat(f);
                    }
                    builder.endPositionEntry();
                } else {
                    builder.appendNull();
                }
                blocks[offset] = builder.build();
            }
        } else {
            blocks[offset] = driverContext.blockFactory().newConstantNullBlock(1);
        }
    }

    @Override
    public void close() {}
}
