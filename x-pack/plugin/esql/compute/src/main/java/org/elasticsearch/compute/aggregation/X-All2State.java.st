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
$if(v2_boolean)$
import org.elasticsearch.common.util.ByteArray;
$else$
import org.elasticsearch.common.util.$v2_Type$Array;
$endif$
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.$v2_Type$Block;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
// end generated imports

/**
 * Aggregator state for a single {@code $v1_type$} and a single {@code $v2_type$}, with support for null v2 values.
 * This class is generated. Edit {@code X-All2State.java.st} instead.
 */
final class All$v1_Type$$v2_Type$State implements AggregatorState {

    private BigArrays bigArrays;

    /**
     * Whether an observation was recorded in this state
     */
    private boolean observed;

    /**
     * The timestamp
     */
    private $v1_type$ v1;

    /**
     * Whether the observed timestamp was null
     */
    private boolean v1Seen;

    /**
     * The value can be null, single valued of multivalued.
     */
    private $if(v2_boolean)$ByteArray$else$$v2_Type$Array$endif$ v2;

    public All$v1_Type$$v2_Type$State(BigArrays bigArrays) {
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

    $v1_type$ v1() {
        return v1;
    }

    void v1($v1_type$ v1) {
        this.v1 = v1;
    }

    boolean v1Seen() {
        return v1Seen;
    }

    void v1Seen(boolean v1Seen) {
        this.v1Seen = v1Seen;
    }

    $if(v2_boolean)$ByteArray$else$$v2_Type$Array$endif$ v2() {
        return v2;
    }

    $if(v2_boolean)$
    void v2(ByteArray v2) {
    $else$
    void v2($v2_Type$Array v2) {
    $endif$
        this.v2 = v2;
    }

    /** Extracts an intermediate view of the contents of this state.  */
    @Override
    public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
        assert blocks.length >= offset + 4;
        blocks[offset + 0] = driverContext.blockFactory().newConstantBooleanBlockWith(observed, 1);
        blocks[offset + 1] = driverContext.blockFactory().newConstantBooleanBlockWith(v1Seen, 1);
        blocks[offset + 2] = driverContext.blockFactory().newConstant$v1_Type$BlockWith(v1, 1);
        blocks[offset + 3] = intermediateValuesBlockBuilder(driverContext);
    }

    public Block intermediateValuesBlockBuilder(DriverContext driverContext) {
        if (v2 == null) {
            return driverContext.blockFactory().newConstantNullBlock(1);
        }

        int size = (int) v2.size();
        $if(v2_BytesRef)$
        var result = driverContext.blockFactory().newBytesRefArrayBlock(v2, 1, new int[] { 0, size }, null, Block.MvOrdering.UNORDERED);
        v2 = null; // transfer the ownership of v2 to the block
        return result;
        $else$
        $v2_type$[] values = new $v2_type$[size];
        for (int i = 0; i < size; ++i) {
            values[i] = $if(v2_boolean)$v2.get(i) == 1$else$v2.get(i)$endif$;
        }
        return driverContext.blockFactory().new$v2_Type$ArrayBlock(values, 1, new int[] { 0, size }, null, Block.MvOrdering.UNORDERED);
        $endif$
    }

    @Override
    public void close() {
        Releasables.close(v2);
    }
}
