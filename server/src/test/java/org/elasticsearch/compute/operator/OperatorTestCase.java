/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;

public abstract class OperatorTestCase extends ESTestCase {

    protected SourceOperator simpleInput(int end) {
        return new SequenceLongBlockSourceOperator(LongStream.range(0, end));
    }

    protected abstract Operator.OperatorFactory simple(BigArrays bigArrays);

    protected abstract String expectedDescriptionOfSimple();

    protected abstract void assertSimpleOutput(int end, List<Page> results);

    /**
     * A {@link ByteSizeValue} that is so small any input to the operator
     * will cause it to circuit break.
     */
    protected abstract ByteSizeValue smallEnoughToCircuitBreak();

    public final void testSimple() {
        assertSimple(nonBreakingBigArrays());
    }

    public final void testCircuitBreaking() {
        Exception e = expectThrows(
            CircuitBreakingException.class,
            () -> assertSimple(new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, smallEnoughToCircuitBreak()))
        );
        assertThat(e.getMessage(), equalTo(MockBigArrays.ERROR_MESSAGE));
    }

    public final void testWithCranky() {
        CrankyCircuitBreakerService breaker = new CrankyCircuitBreakerService();
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, breaker).withCircuitBreaking();
        try {
            assertSimple(bigArrays);
            // Either we get lucky and cranky doesn't throw and the test completes or we don't and it throws
        } catch (CircuitBreakingException e) {
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    public final void testSimpleDescription() {
        assertThat(simple(nonBreakingBigArrays()).describe(), equalTo(expectedDescriptionOfSimple()));
    }

    protected final BigArrays nonBreakingBigArrays() {
        return new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, new NoneCircuitBreakerService()).withCircuitBreaking();
    }

    protected final List<Page> oneDriverPerPage(SourceOperator source, Supplier<List<Operator>> operators) {
        List<Page> result = new ArrayList<>();
        try {
            while (source.isFinished() == false) {
                Page in = source.getOutput();
                if (in == null) {
                    continue;
                }
                try (
                    Driver d = new Driver(
                        new CannedSourceOperator(Iterators.single(in)),
                        operators.get(),
                        new PageConsumerOperator(result::add),
                        () -> {}
                    )
                ) {
                    d.run();
                }
            }
        } finally {
            source.close();
        }
        return result;
    }

    protected final List<Page> oneDriverPerPageList(Iterator<List<Page>> source, Supplier<List<Operator>> operators) {
        List<Page> result = new ArrayList<>();
        while (source.hasNext()) {
            List<Page> in = source.next();
            try (
                Driver d = new Driver(
                    new CannedSourceOperator(in.iterator()),
                    operators.get(),
                    new PageConsumerOperator(result::add),
                    () -> {}
                )
            ) {
                d.run();
            }
        }
        return result;
    }

    private void assertSimple(BigArrays bigArrays) {
        int end = between(1_000, 100_000);
        List<Page> results = new ArrayList<>();

        try (
            Driver d = new Driver(
                simpleInput(end),
                List.of(simple(bigArrays.withCircuitBreaking()).get()),
                new PageConsumerOperator(page -> results.add(page)),
                () -> {}
            )
        ) {
            d.run();
        }
        assertSimpleOutput(end, results);
    }
}
