/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.tree.Source;

import static org.hamcrest.Matchers.equalTo;

public class FoldContextTests extends ESTestCase {
    public void testTrackAllocation() {
        FoldContext ctx = new FoldContext(10);
        ctx.trackAllocation(Source.synthetic("shouldn't break"), 10);
        Exception e = expectThrows(
            FoldContext.FoldTooMuchMemoryException.class,
            () -> ctx.trackAllocation(Source.synthetic("should break"), 1)
        );
        assertThat(
            e.getMessage(),
            equalTo(
                "line -1:-1: Folding query used more than 10b. "
                    + "The expression that pushed past the limit is [should break] which needed 1b."
            )
        );
    }

    public void testCircuitBreakerViewBreaking() {
        FoldContext ctx = new FoldContext(10);
        ctx.circuitBreakerView(Source.synthetic("shouldn't break")).addEstimateBytesAndMaybeBreak(10, "test");
        Exception e = expectThrows(
            FoldContext.FoldTooMuchMemoryException.class,
            () -> ctx.circuitBreakerView(Source.synthetic("should break")).addEstimateBytesAndMaybeBreak(1, "test")
        );
        assertThat(
            e.getMessage(),
            equalTo(
                "line -1:-1: Folding query used more than 10b. "
                    + "The expression that pushed past the limit is [should break] which needed 1b."
            )
        );
    }

    public void testCircuitBreakerViewWithoutBreaking() {
        FoldContext ctx = new FoldContext(10);
        CircuitBreaker view = ctx.circuitBreakerView(Source.synthetic("shouldn't break"));
        view.addEstimateBytesAndMaybeBreak(10, "test");
        view.addWithoutBreaking(-1);
        assertThat(view.getUsed(), equalTo(9L));
    }
}
