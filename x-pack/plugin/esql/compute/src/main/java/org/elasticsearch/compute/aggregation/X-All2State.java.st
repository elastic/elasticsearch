/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;
// begin generated imports
$if(BytesRef)$ import org.apache.lucene.util.BytesRef; $endif$
import org.elasticsearch.common.util.$v2_Array$;
import org.elasticsearch.compute.data.Block;
$if(BytesRef)$ import org.elasticsearch.compute.data.$v2_Type$Block; $endif$
import org.elasticsearch.compute.operator.DriverContext;
$if(BytesRef)$ import org.elasticsearch.compute.operator.BreakingBytesRefBuilder; $endif$
$if(BytesRef)$ import org.elasticsearch.common.breaker.CircuitBreaker; $endif$
import org.elasticsearch.core.Releasables;
// end generated imports

/**
 * Aggregator state for a single {@code $v1_type$} and a single {@code $v2_type$}, with support for null v2 values.
 * This class is generated. Edit {@code X-All2State.java.st} instead.
 */
final class All$v1_Type$$v2_Type$State implements AggregatorState {
    // Whether an observation was recorded in this state
    private boolean observed;

    // The timestamp
    private $v1_type$ v1;

    // Tells whether the observed timestamp was null
    private boolean v1Seen;

    // The value can be null, single valued of multivalued.
    private $v2_Array$ v2;

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

    $v2_Array$ v2() {
        return v2;
    }

    void v2($v2_Array$ v2) {
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
        return driverContext.blockFactory().newBytesRefArrayBlock(v2, 1, new int[] { 0, size }, null, Block.MvOrdering.UNORDERED);
        $else$
        $v2_type$[] values = new $v2_type$[size];
        for (int i = 0; i < size; ++i) {
            values[i] = v2.get(i);
        }
        return driverContext.blockFactory().new$v2_Type$ArrayBlock(values, 1, new int[] { 0, size }, null, Block.MvOrdering.UNORDERED);
        $endif$
    }

    @Override
    public void close() {
        Releasables.close(v2);
    }
}
