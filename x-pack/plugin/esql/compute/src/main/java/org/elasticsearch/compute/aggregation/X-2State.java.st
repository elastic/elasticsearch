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
 * Aggregator state for a single {@code $v1_type$} and a single {@code $v2_type$}.
 * This class is generated. Edit {@code X-2State.java.st} instead.
 */
final class $v1_Type$$v2_Type$State implements AggregatorState {
    private $v1_type$ v1;
    $if(v2_BytesRef)$
    private final BreakingBytesRefBuilder v2;
    $else$
    private $v2_type$ v2;
    $endif$
    private boolean seen;

    $v1_Type$$v2_Type$State($v1_type$ v1, $v2_type$ v2$if(v2_BytesRef)$, CircuitBreaker breaker, String label$endif$) {
        this.v1 = v1;
        $if(v2_BytesRef)$
        this.v2 = new BreakingBytesRefBuilder(breaker, label, v2.length);
        this.v2.copyBytes(v2);
        $else$
        this.v2 = v2;
        $endif$
    }

    $v1_type$ v1() {
        return v1;
    }

    void v1($v1_type$ v1) {
        this.v1 = v1;
    }

    $v2_type$ v2() {
        return v2$if(v2_BytesRef)$.bytesRefView()$endif$;
    }

    void v2($v2_type$ v2) {
        $if(v2_BytesRef)$
        this.v2.copyBytes(v2);
        $else$
        this.v2 = v2;
        $endif$
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
        blocks[offset + 1] = driverContext.blockFactory().newConstant$v2_Type$BlockWith(v2$if(v2_BytesRef)$.bytesRefView()$endif$, 1);
        blocks[offset + 2] = driverContext.blockFactory().newConstantBooleanBlockWith(seen, 1);
    }

    @Override
    $if(v2_BytesRef)$
    public void close() {
        Releasables.close(this.v2);
    }
    $else$
    public void close() {}
    $endif$
}
