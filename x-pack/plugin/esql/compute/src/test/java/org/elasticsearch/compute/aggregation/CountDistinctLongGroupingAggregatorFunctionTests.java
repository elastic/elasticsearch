/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.operator.blocksource.TupleLongLongBlockSourceOperator;
import org.elasticsearch.core.Tuple;

import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;

public class CountDistinctLongGroupingAggregatorFunctionTests extends PartitionedGroupingAggregatorFunctionTestCase {
    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new CountDistinctLongAggregatorFunctionSupplier(CountDistinctTestUtils.PRECISION);
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "count_distinct of longs";
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new TupleLongLongBlockSourceOperator(
            blockFactory,
            LongStream.range(0, size).mapToObj(l -> Tuple.tuple(randomGroupId(size), randomLongBetween(0, 100_000)))
        );
    }

    @Override
    protected void assertSimpleGroup(List<Page> input, Block result, int position, Long group) {
        CountDistinctTestUtils.assertCount(
            ((LongBlock) result).getLong(position),
            input.stream().flatMapToLong(p -> allLongs(p, group)).distinct().map(CountDistinctTestUtils::hash)
        );
    }

    /**
     * {@code 21685} and {@code 76695} share the top 25 bits of their hash, so linear counting stores them as one entry
     * and counts this group as 1. {@link #assertSimpleGroup} must accept that as a hash collision.
     */
    public void testHashCollisionInSmallGroup() {
        var runner = new TestDriverRunner().builder(driverContext()).collectDeepCopy();
        runner.input(
            new TupleLongLongBlockSourceOperator(runner.blockFactory(), List.of(Tuple.tuple(0L, 21685L), Tuple.tuple(0L, 76695L)))
        );
        List<Page> results = runner.run(simple());
        assertSimpleOutput(runner.deepCopy(), results);
        assertThat(((LongBlock) results.getFirst().getBlock(1)).getLong(0), equalTo(1L));
    }

    /**
     * {@link CountDistinctTestUtils#assertCount} only accepts an undercount in a small group when the values' hashes
     * really collide, so it still catches values being lost.
     */
    public void testUndercountWithoutHashCollisionFails() {
        expectThrows(
            AssertionError.class,
            () -> CountDistinctTestUtils.assertCount(1, LongStream.of(1, 2).map(CountDistinctTestUtils::hash))
        );
    }

    @Override
    protected void assertOutputFromNullOnly(Block b, int position) {
        assertThat(b.isNull(position), equalTo(false));
        assertThat(b.getValueCount(position), equalTo(1));
        assertThat(((LongBlock) b).getLong(b.getFirstValueIndex(position)), equalTo(0L));
    }

    @Override
    protected void assertOutputFromAllFiltered(Block b) {
        assertThat(b.elementType(), equalTo(ElementType.LONG));
        LongVector v = (LongVector) b.asVector();
        for (int p = 0; p < v.getPositionCount(); p++) {
            assertThat(v.getLong(p), equalTo(0L));
        }
    }
}
