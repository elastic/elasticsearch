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
 * Aggregator state for a single {@code long} and a single {@code long}, with support for null v2 values.
 * This class is generated. Edit {@code X-All2State.java.st} instead.
 */
final class AllLongLongState implements AggregatorState {
    // the timestamp
    private long v1;

    // the value
    private long v2;

    // whether we've seen a first/last timestamp
    private boolean seen;

    // because we might observe a first/last timestamp without observing a value (e.g.: value was null)
    private boolean v2Seen;

    AllLongLongState(long v1, long v2) {
        this.v1 = v1;
        this.v2 = v2;
    }

    long v1() {
        return v1;
    }

    void v1(long v1) {
        this.v1 = v1;
    }

    long v2() {
        return v2;
    }

    void v2(long v2) {
        this.v2 = v2;
    }

    boolean seen() {
        return seen;
    }

    void seen(boolean seen) {
        this.seen = seen;
    }

    boolean v2Seen() {
        return v2Seen;
    }

    void v2Seen(boolean v2Seen) {
        this.v2Seen = v2Seen;
    }

    /** Extracts an intermediate view of the contents of this state.  */
    @Override
    public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
        assert blocks.length >= offset + 4;
        blocks[offset + 0] = driverContext.blockFactory().newConstantLongBlockWith(v1, 1);
        blocks[offset + 1] = driverContext.blockFactory().newConstantLongBlockWith(v2, 1);
        blocks[offset + 2] = driverContext.blockFactory().newConstantBooleanBlockWith(seen, 1);
        blocks[offset + 3] = driverContext.blockFactory().newConstantBooleanBlockWith(v2Seen, 1);
    }

    @Override
    public void close() {}
}
