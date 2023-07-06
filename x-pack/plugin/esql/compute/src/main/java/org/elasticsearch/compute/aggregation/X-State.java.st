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
import org.elasticsearch.compute.data.Constant$Type$Vector;

/**
 * Aggregator state for a single $type$.
 * This class is generated. Do not edit it.
 */
@Experimental
final class $Type$State implements AggregatorState<$Type$State> {
    private $type$ value;
    private boolean seen;

    $Type$State() {
        this(0);
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
    void toIntermediate(Block[] blocks, int offset) {
        assert blocks.length >= offset + 2;
        blocks[offset + 0] = new Constant$Type$Vector(value, 1).asBlock();
        blocks[offset + 1] = new ConstantBooleanVector(seen, 1).asBlock();
    }

    @Override
    public long getEstimatedSize() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {}

    @Override
    public AggregatorStateSerializer<$Type$State> serializer() {
        throw new UnsupportedOperationException();
    }
}
