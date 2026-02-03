/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

// begin generated imports
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.Releasables;
// end generated imports

/**
 * Aggregator state for a single {@code long} and a single {@code int}.
 * This class is generated. Edit {@code X-2State.java.st} instead.
 */
final class LongIntState implements AggregatorState {
    private long v1;
    private int v2;
    private boolean seen;

    LongIntState(long v1, int v2) {
        this.v1 = v1;
        this.v2 = v2;
    }

    long v1() {
        return v1;
    }

    void v1(long v1) {
        this.v1 = v1;
    }

    int v2() {
        return v2;
    }

    void v2(int v2) {
        this.v2 = v2;
    }

    boolean seen() {
        return seen;
    }

    void seen(boolean seen) {
        this.seen = seen;
    }

    /** Extracts an intermediate view of the contents of this state.  */
    @Override
    public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
        assert blocks.length >= offset + 3;
        blocks[offset + 0] = driverContext.blockFactory().newConstantLongBlockWith(v1, 1);
        blocks[offset + 1] = driverContext.blockFactory().newConstantIntBlockWith(v2, 1);
        blocks[offset + 2] = driverContext.blockFactory().newConstantBooleanBlockWith(seen, 1);
    }

    @Override
    public void close() {}
}
