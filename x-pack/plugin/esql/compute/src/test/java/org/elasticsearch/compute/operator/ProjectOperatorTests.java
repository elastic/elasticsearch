/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.junit.After;
import org.junit.Before;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ProjectOperatorTests extends OperatorTestCase {

    final CircuitBreaker breaker = new MockBigArrays.LimitedBreaker("esql-test-breaker", ByteSizeValue.ofGb(1));
    final BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, mockBreakerService(breaker));
    final BlockFactory blockFactory = BlockFactory.getInstance(breaker, bigArrays);

    @Before
    @After
    public void assertBreakerIsZero() {
        assertThat(breaker.getUsed(), is(0L));
    }

    @Override
    protected DriverContext driverContext() {
        return new DriverContext(blockFactory.bigArrays(), blockFactory);
    }

    public void testProjectionOnEmptyPage() {
        var page = new Page(0);
        var projection = new ProjectOperator(randomProjection(10));
        projection.addInput(page);
        assertEquals(page, projection.getOutput());
    }

    public void testProjection() {
        var size = randomIntBetween(2, 5);
        var blocks = new Block[size];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = blockFactory.newConstantIntBlockWith(i, size);
        }

        var page = new Page(size, blocks);
        var randomProjection = randomProjection(size);

        var projection = new ProjectOperator(randomProjection);
        projection.addInput(page);
        var out = projection.getOutput();
        assertThat(randomProjection.size(), lessThanOrEqualTo(out.getBlockCount()));

        Set<Block> blks = new HashSet<>();
        for (int i = 0; i < out.getBlockCount(); i++) {
            var block = out.<IntBlock>getBlock(i);
            assertEquals(block, page.getBlock(randomProjection.get(i)));
            blks.add(block);
        }

        // close all blocks separately since the same block can be used by multiple columns (aliased)
        Releasables.closeWhileHandlingException(blks.toArray(new Block[0]));
    }

    private List<Integer> randomProjection(int size) {
        return randomList(size, () -> randomIntBetween(0, size - 1));
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int end) {
        return new TupleBlockSourceOperator(blockFactory, LongStream.range(0, end).mapToObj(l -> Tuple.tuple(l, end - l)));
    }

    @Override
    protected Operator.OperatorFactory simple(BigArrays bigArrays) {
        return new ProjectOperator.ProjectOperatorFactory(Arrays.asList(1));
    }

    @Override
    protected String expectedDescriptionOfSimple() {
        return "ProjectOperator[projection = [1]]";
    }

    @Override
    protected String expectedToStringOfSimple() {
        return expectedDescriptionOfSimple();
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        long expected = input.stream().mapToInt(Page::getPositionCount).sum();
        int total = 0;
        for (Page page : results) {
            assertThat(page.getBlockCount(), equalTo(1));
            LongBlock remaining = page.getBlock(0);
            total += page.getPositionCount();
            for (int i = 0; i < page.getPositionCount(); i++) {
                assertThat(remaining.getLong(i), equalTo(expected));
                expected--;
            }
        }
        assertThat(total, equalTo(input.stream().mapToInt(Page::getPositionCount).sum()));
    }

    @Override
    protected ByteSizeValue smallEnoughToCircuitBreak() {
        assumeTrue("doesn't use big arrays so can't break", false);
        return null;
    }

    // A breaker service that always returns the given breaker for getBreaker(CircuitBreaker.REQUEST)
    static CircuitBreakerService mockBreakerService(CircuitBreaker breaker) {
        CircuitBreakerService breakerService = mock(CircuitBreakerService.class);
        when(breakerService.getBreaker(CircuitBreaker.REQUEST)).thenReturn(breaker);
        return breakerService;
    }
}
