/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * Aggregator state for a single int.
 * It stores a third boolean to store if the aggregation failed.
 * This class is generated. Do not edit it.
 */
final class IntFallibleState implements AggregatorState {
    private int value;
    private boolean seen;
    private boolean failed;

    IntFallibleState(int init) {
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

    boolean failed() {
        return failed;
    }

    void failed(boolean failed) {
        this.failed = failed;
    }

    /** Extracts an intermediate view of the contents of this state.  */
    @Override
    public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
        assert blocks.length >= offset + 3;
        blocks[offset + 0] = driverContext.blockFactory().newConstantIntBlockWith(value, 1);
        blocks[offset + 1] = driverContext.blockFactory().newConstantBooleanBlockWith(seen, 1);
        blocks[offset + 2] = driverContext.blockFactory().newConstantBooleanBlockWith(failed, 1);
    }

    @Override
    public void close() {}
}
