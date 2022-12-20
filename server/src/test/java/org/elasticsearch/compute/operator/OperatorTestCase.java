/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;

public abstract class OperatorTestCase extends ESTestCase {

    protected SourceOperator simpleInput(int end) {
        return new SequenceLongBlockSourceOperator(LongStream.range(0, end));
    }

    protected abstract Operator simple(BigArrays bigArrays);

    protected abstract void assertSimpleOutput(int end, List<Page> results);

    public void testSimple() {
        assertSimple(new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, new NoneCircuitBreakerService()));
    }

    public void testCircuitBreaking() {
        Exception e = expectThrows(
            CircuitBreakingException.class,
            () -> assertSimple(new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofBytes(between(1, 32))))
        );
        assertThat(e.getMessage(), equalTo(MockBigArrays.ERROR_MESSAGE));
    }

    public void testWithCranky() {
        CrankyCircuitBreakerService breaker = new CrankyCircuitBreakerService();
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, breaker).withCircuitBreaking();
        try {
            assertSimple(bigArrays);
            // Either we get lucky and cranky doesn't throw and the test completes or we don't and it throws
        } catch (CircuitBreakingException e) {
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    private void assertSimple(BigArrays bigArrays) {
        int end = between(1_000, 100_000);
        List<Page> results = new ArrayList<>();

        try (
            Driver d = new Driver(
                simpleInput(end),
                List.of(simple(bigArrays.withCircuitBreaking())),
                new PageConsumerOperator(page -> results.add(page)),
                () -> {}
            )
        ) {
            d.run();
        }
        assertSimpleOutput(end, results);
    }
}
