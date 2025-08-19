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
 * Aggregator state for a single {@code $v1_type$} and a single {@code $v2_type$}.
 * This class is generated. Edit {@code X-2State.java.st} instead.
 */
final class $v1_Type$$v2_Type$State implements AggregatorState {
    private $v1_type$ v1;
    private $v2_type$ v2;
    private boolean seen;

    $v1_Type$$v2_Type$State($v1_type$ v1, $v2_type$ v2) {
        this.v1 = v1;
        this.v2 = v2;
    }

    $v1_type$ v1() {
        return v1;
    }

    void v1($v1_type$ v1) {
        this.v1 = v1;
    }

    $v2_type$ v2() {
        return v2;
    }

    void v2($v2_type$ v2) {
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
        blocks[offset + 0] = driverContext.blockFactory().newConstant$v1_Type$BlockWith(v1, 1);
        blocks[offset + 1] = driverContext.blockFactory().newConstant$v2_Type$BlockWith(v2, 1);
        blocks[offset + 2] = driverContext.blockFactory().newConstantBooleanBlockWith(seen, 1);
    }

    @Override
    public void close() {}
}
