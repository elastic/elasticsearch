/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.tests.util.RamUsageTester;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.SequenceLongBlockSourceOperator;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matcher;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static java.util.Comparator.naturalOrder;
import static java.util.Comparator.reverseOrder;
import static org.elasticsearch.compute.data.ElementType.LONG;
import static org.elasticsearch.compute.operator.topn.TopNEncoder.DEFAULT_UNSORTABLE;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.number.OrderingComparison.lessThanOrEqualTo;

public class UngroupedTopNOperatorTests extends TopNOperatorTests {
    @Override
    protected List<Integer> groupKeys() {
        return List.of();
    }

    @Override
    protected void testRandomTopN(boolean asc, DriverContext context) {
        int limit = randomIntBetween(1, 20);
        List<Long> inputValues = randomList(0, 5000, ESTestCase::randomLong);
        Comparator<Long> comparator = asc ? naturalOrder() : reverseOrder();
        List<Long> expectedValues = inputValues.stream().sorted(comparator).limit(limit).toList();
        List<Long> outputValues = topNLong(context, inputValues, limit, asc, false);
        assertThat(outputValues, equalTo(expectedValues));
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        for (int i = 0; i < results.size() - 1; i++) {
            assertThat(results.get(i).getPositionCount(), equalTo(pageSize));
        }
        assertThat(results.get(results.size() - 1).getPositionCount(), lessThanOrEqualTo(pageSize));
        long[] topN = input.stream()
            .flatMapToLong(
                page -> IntStream.range(0, page.getPositionCount())
                    .filter(p -> false == page.getBlock(0).isNull(p))
                    .mapToLong(p -> ((LongBlock) page.getBlock(0)).getLong(p))
            )
            .sorted()
            .limit(4)
            .toArray();
        assertThat(
            results.stream()
                .flatMapToLong(page -> IntStream.range(0, page.getPositionCount()).mapToLong(i -> page.<LongBlock>getBlock(0).getLong(i)))
                .toArray(),
            equalTo(topN)
        );
    }

    @Override
    protected TopNOperator.TopNOperatorFactory simple(SimpleOptions options) {
        return new TopNOperator.TopNOperatorFactory(
            4,
            List.of(LONG),
            List.of(DEFAULT_UNSORTABLE),
            List.of(new TopNOperator.SortOrder(0, true, false)),
            groupKeys(),
            pageSize
        );
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo(
            "TopNOperator[count=0/4, elementTypes=[LONG], encoders=[DefaultUnsortable], "
                + "sortOrders=[SortOrder[channel=0, asc=true, nullsFirst=false]]]"
        );
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo(
            "TopNOperator[count=4, elementTypes=[LONG], encoders=[DefaultUnsortable], "
                + "sortOrders=[SortOrder[channel=0, asc=true, nullsFirst=false]]]"
        );
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new SequenceLongBlockSourceOperator(
            blockFactory,
            LongStream.range(0, size).map(l -> ESTestCase.randomLong()),
            between(1, size * 2)
        );
    }

    public void testRamBytesUsed() {
        RamUsageTester.Accumulator acc = new RamUsageTester.Accumulator() {
            @Override
            public long accumulateObject(Object o, long shallowSize, Map<Field, Object> fieldValues, Collection<Object> queue) {
                if (o instanceof ElementType) {
                    return 0; // shared
                }
                if (o instanceof TopNEncoder) {
                    return 0; // shared
                }
                if (o instanceof CircuitBreaker) {
                    return 0; // shared
                }
                if (o instanceof BlockFactory) {
                    return 0; // shard
                }
                return super.accumulateObject(o, shallowSize, fieldValues, queue);
            }
        };
        int topCount = 10_000;
        // We under-count by a few bytes because of the lists. In that end that's fine, but we need to account for it here.
        long underCount = 200;
        DriverContext context = driverContext();
        try (
            TopNOperator op = new TopNOperator.TopNOperatorFactory(
                topCount,
                List.of(LONG),
                List.of(DEFAULT_UNSORTABLE),
                List.of(new TopNOperator.SortOrder(0, true, false)),
                pageSize
            ).get(context)
        ) {
            long actualEmpty = RamUsageTester.ramUsed(op, acc);
            assertThat(op.ramBytesUsed(), both(greaterThan(actualEmpty - underCount)).and(lessThan(actualEmpty)));
            // But when we fill it then we're quite close
            for (Page p : CannedSourceOperator.collectPages(simpleInput(context.blockFactory(), topCount))) {
                op.addInput(p);
            }
            long actualFull = RamUsageTester.ramUsed(op, acc);
            assertThat(op.ramBytesUsed(), both(greaterThan(actualFull - underCount)).and(lessThan(actualFull)));

            // TODO empty it again and check.
        }
    }
}
