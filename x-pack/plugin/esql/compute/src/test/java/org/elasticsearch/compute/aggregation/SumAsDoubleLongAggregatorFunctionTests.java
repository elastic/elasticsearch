/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.TestResultPageSinkOperator;
import org.elasticsearch.compute.test.operator.blocksource.SequenceLongBlockSourceOperator;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class SumAsDoubleLongAggregatorFunctionTests extends AggregatorFunctionTestCase {
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        // Any long will do: the sum of them doesn't fit in a long, and that is the point.
        return new SequenceLongBlockSourceOperator(blockFactory, LongStream.range(0, size).map(l -> randomLong()));
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new SumAsDoubleLongAggregatorFunctionSupplier();
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "sum_as_double of longs";
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, Block result) {
        double sum = input.stream()
            .flatMapToLong(p -> allLongs(p.getBlock(0)))
            .mapToObj(BigInteger::valueOf)
            .reduce(BigInteger.ZERO, BigInteger::add)
            .doubleValue();
        /*
         * A single pass is exact. The partial sums of many passes are merged like the ones of a sum of doubles, and each merge
         * may round. The partial sums are much larger than the longs, which cancel each other out, so the error is relative to
         * the longs and not to the sum.
         */
        assertThat(((DoubleBlock) result).getDouble(0), closeTo(sum, Math.max(0x1p63, Math.abs(sum)) * 1e-9));
    }

    public void testOverflowSucceeds() {
        assertThat(sum(Long.MAX_VALUE, Long.MAX_VALUE, 2), equalTo(0x1p64));
    }

    public void testNegativeOverflowSucceeds() {
        assertThat(sum(Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE), equalTo(-0x1.8p64));
    }

    /**
     * The longs are added exactly, so the ones that a double can't hold don't lose their low bits before they cancel
     * out, as they do when each of them is converted to a double first: that would make this sum {@code 0}.
     */
    public void testExactAboveDoublePrecision() {
        long notADouble = (1L << 53) + 1;
        assertThat((double) notADouble, equalTo(0x1p53));
        assertThat(sum(notADouble, notADouble, -(1L << 54)), equalTo(2.0));
    }

    public void testCarriesBetweenWords() {
        // -1 + 1 carries out of the low word and into the sign extension of the -1
        assertThat(sum(-1, 1), equalTo(0.0));
        assertThat(sum(-1, -1, 1), equalTo(-1.0));
        assertThat(sum(Long.MIN_VALUE, Long.MAX_VALUE, 1), equalTo(0.0));
        assertThat(sum(Long.MAX_VALUE, 1, Long.MIN_VALUE, -5), equalTo(-5.0));
    }

    /**
     * A single pass is exact whatever the values and however many pages they come in: the sum is rounded once, at the end.
     */
    public void testSinglePassIsExact() {
        long[] values = LongStream.generate(() -> randomBoolean() ? randomLong() : randomLongBetween(-1000, 1000))
            .limit(between(1, 20_000))
            .toArray();
        BigInteger expected = LongStream.of(values).mapToObj(BigInteger::valueOf).reduce(BigInteger.ZERO, BigInteger::add);
        assertThat(sum(values), equalTo(expected.doubleValue()));
    }

    private double sum(long... values) {
        DriverContext driverContext = driverContext();
        List<Page> results = new ArrayList<>();
        try (
            Driver d = TestDriverFactory.create(
                driverContext,
                new SequenceLongBlockSourceOperator(driverContext.blockFactory(), LongStream.of(values)),
                List.of(simple().get(driverContext)),
                new TestResultPageSinkOperator(results::add)
            )
        ) {
            new TestDriverRunner().run(d);
        }
        assertThat(results.size(), equalTo(1));
        double sum = results.get(0).<DoubleBlock>getBlock(0).getDouble(0);
        assertDriverContext(driverContext);
        return sum;
    }
}
