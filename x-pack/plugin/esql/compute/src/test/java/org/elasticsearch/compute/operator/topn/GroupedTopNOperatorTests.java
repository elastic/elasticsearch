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
import org.elasticsearch.compute.test.TupleLongLongBlockSourceOperator;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matcher;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.elasticsearch.compute.data.ElementType.LONG;
import static org.elasticsearch.compute.operator.topn.TopNEncoder.DEFAULT_UNSORTABLE;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class GroupedTopNOperatorTests extends TopNOperatorTests {
    private static final int TOP_COUNT = 4;

    @Override
    protected List<Integer> groupKeys() {
        return List.of(0);
    }

    public void testBasicTopN() {
        List<Long> values = Arrays.asList(2L, 1L, 4L, null, 4L, null);
        assertThat(topNLong(values, 1, true, false), equalTo(Arrays.asList(1L, 2L, 4L, null)));
        assertThat(topNLong(values, 1, false, false), equalTo(Arrays.asList(4L, 2L, 1L, null)));
        assertThat(topNLong(values, 1, true, true), equalTo(Arrays.asList(null, 1L, 2L, 4L)));
        assertThat(topNLong(values, 1, false, true), equalTo(Arrays.asList(null, 4L, 2L, 1L)));
        assertThat(topNLong(values, 2, true, false), equalTo(Arrays.asList(1L, 2L, 4L, 4L, null, null)));
        assertThat(topNLong(values, 2, false, false), equalTo(Arrays.asList(4L, 4L, 2L, 1L, null, null)));
        assertThat(topNLong(values, 2, true, true), equalTo(Arrays.asList(null, null, 1L, 2L, 4L, 4L)));
        assertThat(topNLong(values, 2, false, true), equalTo(Arrays.asList(null, null, 4L, 4L, 2L, 1L)));
    }

    private List<Long> topNLong(List<Long> inputValues, int limit, boolean ascendingOrder, boolean nullsFirst) {
        return topNLong(driverContext(), inputValues, limit, ascendingOrder, nullsFirst);
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new TupleLongLongBlockSourceOperator(
            blockFactory,
            LongStream.range(0, size).mapToObj(l -> Tuple.tuple(ESTestCase.randomLong(), ESTestCase.randomLong())),
            between(1, size * 2)
        );
    }

    @Override
    protected TopNOperator.TopNOperatorFactory simple(SimpleOptions options) {
        return new TopNOperator.TopNOperatorFactory(
            TOP_COUNT,
            List.of(LONG, LONG),
            List.of(DEFAULT_UNSORTABLE, DEFAULT_UNSORTABLE),
            List.of(new TopNOperator.SortOrder(0, true, false)),
            List.of(1),
            pageSize
        );
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo(
            "TopNOperator[count=4, elementTypes=[LONG, LONG], encoders=[DefaultUnsortable, DefaultUnsortable], "
                + "sortOrders=[SortOrder[channel=0, asc=true, nullsFirst=false]], groupKeys=[1]]"
        );
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo(
            "TopNOperator[count=0/4, elementTypes=[LONG, LONG], encoders=[DefaultUnsortable, DefaultUnsortable], "
                + "sortOrders=[SortOrder[channel=0, asc=true, nullsFirst=false]], groupKeys=[1]]"
        );
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        for (int i = 0; i < results.size() - 1; i++) {
            assertThat(results.get(i).getPositionCount(), equalTo(pageSize));
        }
        assertThat(results.get(results.size() - 1).getPositionCount(), lessThanOrEqualTo(pageSize));
        Map<Long, List<Long>> topN = input.stream()
            .flatMap(
                page -> IntStream.range(0, page.getPositionCount())
                    .filter(p -> false == page.getBlock(0).isNull(p))
                    .mapToObj(p -> Tuple.tuple(((LongBlock) page.getBlock(0)).getLong(p), ((LongBlock) page.getBlock(1)).getLong(p)))

            )
            .collect(
                Collectors.groupingBy(
                    Tuple::v2,
                    Collectors.mapping(
                        Tuple::v1,
                        Collectors.collectingAndThen(Collectors.toList(), list -> list.stream().sorted().limit(TOP_COUNT).toList())
                    )
                )
            );
        var expected = topN.entrySet()
            .stream()
            .flatMap(entry -> entry.getValue().stream().map(v -> Tuple.tuple(v, entry.getKey())))
            .sorted(Comparator.comparing(Tuple::v1))
            .toList();
        assertThat(
            results.stream()
                .flatMap(
                    page -> IntStream.range(0, page.getPositionCount())
                        .mapToObj(i -> Tuple.tuple(page.<LongBlock>getBlock(0).getLong(i), page.<LongBlock>getBlock(1).getLong(i)))
                )
                .toList(),
            equalTo(expected)
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
                List.of(LONG, LONG),
                List.of(DEFAULT_UNSORTABLE, DEFAULT_UNSORTABLE),
                List.of(new TopNOperator.SortOrder(0, true, false)),
                List.of(1),
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
