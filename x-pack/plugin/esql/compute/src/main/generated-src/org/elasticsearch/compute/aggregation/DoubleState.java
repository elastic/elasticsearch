/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.ann.Experimental;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ConstantBooleanVector;
import org.elasticsearch.compute.data.ConstantDoubleVector;

/**
 * Aggregator state for a single double.
 * This class is generated. Do not edit it.
 */
@Experimental
final class DoubleState implements AggregatorState {
    private double value;
    private boolean seen;

    DoubleState() {
        this(0);
    }

    DoubleState(double init) {
        this.value = init;
    }

    double doubleValue() {
        return value;
    }

    void doubleValue(double value) {
        this.value = value;
    }

    boolean seen() {
        return seen;
    }

    void seen(boolean seen) {
        this.seen = seen;
    }

    /** Extracts an intermediate view of the contents of this state.  */
    @Override
    public void toIntermediate(Block[] blocks, int offset) {
        assert blocks.length >= offset + 2;
        blocks[offset + 0] = new ConstantDoubleVector(value, 1).asBlock();
        blocks[offset + 1] = new ConstantBooleanVector(seen, 1).asBlock();
    }

    @Override
    public void close() {}
}
