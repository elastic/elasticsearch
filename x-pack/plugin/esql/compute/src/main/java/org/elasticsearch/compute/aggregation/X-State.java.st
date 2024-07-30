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
 * Aggregator state for a single $type$.
 * This class is generated. Do not edit it.
 */
final class $Type$State implements AggregatorState {
    private $type$ value;
    private boolean seen;

    $Type$State() {
$if(boolean)$
        this(false);
$else$
        this(0);
$endif$
    }

    $Type$State($type$ init) {
        this.value = init;
    }

    $type$ $type$Value() {
        return value;
    }

    void $type$Value($type$ value) {
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
    public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
        assert blocks.length >= offset + 2;
        blocks[offset + 0] = driverContext.blockFactory().newConstant$Type$BlockWith(value, 1);
        blocks[offset + 1] = driverContext.blockFactory().newConstantBooleanBlockWith(seen, 1);
    }

    @Override
    public void close() {}
}
