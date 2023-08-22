/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ConstantBooleanVector;
import org.elasticsearch.compute.data.ConstantIntVector;

/**
 * Aggregator state for a single int.
 * This class is generated. Do not edit it.
 */
final class IntState implements AggregatorState {
    private int value;
    private boolean seen;

    IntState() {
        this(0);
    }

    IntState(int init) {
        this.value = init;
    }

    int intValue() {
        return value;
    }

    void intValue(int value) {
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
        blocks[offset + 0] = new ConstantIntVector(value, 1).asBlock();
        blocks[offset + 1] = new ConstantBooleanVector(seen, 1).asBlock();
    }

    @Override
    public void close() {}
}
