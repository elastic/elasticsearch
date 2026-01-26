/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

// begin generated imports
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
// end generated imports

/**
 * Aggregator state for a single {@code long} and a single {@code boolean}, with support for null v2 values.
 * This class is generated. Edit {@code X-All2State.java.st} instead.
 */
final class AllLongBooleanState implements AggregatorState {

    private BigArrays bigArrays;

    /**
     * Whether an observation was recorded in this state
     */
    private boolean observed;

    /**
     * The timestamp
     */
    private long v1;

    /**
     * Whether the observed timestamp was null
     */
    private boolean v1Seen;

    /**
     * The value can be null, single valued of multivalued.
     */
    private ByteArray v2;

    public AllLongBooleanState(BigArrays bigArrays) {
        this.bigArrays = bigArrays;
    }

    BigArrays bigArrays() {
        return bigArrays;
    }

    boolean observed() {
        return observed;
    }

    void observed(boolean observed) {
        this.observed = observed;
    }

    long v1() {
        return v1;
    }

    void v1(long v1) {
        this.v1 = v1;
    }

    boolean v1Seen() {
        return v1Seen;
    }

    void v1Seen(boolean v1Seen) {
        this.v1Seen = v1Seen;
    }

    ByteArray v2() {
        return v2;
    }

    void v2(ByteArray v2) {
        this.v2 = v2;
    }

    /** Extracts an intermediate view of the contents of this state.  */
    @Override
    public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
        assert blocks.length >= offset + 4;
        blocks[offset + 0] = driverContext.blockFactory().newConstantBooleanBlockWith(observed, 1);
        blocks[offset + 1] = driverContext.blockFactory().newConstantBooleanBlockWith(v1Seen, 1);
        blocks[offset + 2] = driverContext.blockFactory().newConstantLongBlockWith(v1, 1);
        blocks[offset + 3] = intermediateValuesBlockBuilder(driverContext);
    }

    public Block intermediateValuesBlockBuilder(DriverContext driverContext) {
        if (v2 == null) {
            return driverContext.blockFactory().newConstantNullBlock(1);
        }

        int size = (int) v2.size();
        boolean[] values = new boolean[size];
        for (int i = 0; i < size; ++i) {
            values[i] = v2.get(i) == 1;
        }
        return driverContext.blockFactory().newBooleanArrayBlock(values, 1, new int[] { 0, size }, null, Block.MvOrdering.UNORDERED);
    }

    @Override
    public void close() {
        Releasables.close(v2);
    }
}
