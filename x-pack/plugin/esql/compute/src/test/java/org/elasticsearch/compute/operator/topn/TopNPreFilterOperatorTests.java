/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.BlockTestUtils;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.operator.blocksource.TupleLongLongBlockSourceOperator;
import org.elasticsearch.core.Tuple;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class TopNPreFilterOperatorTests extends OperatorTestCase {
    private static final int LIMIT = 10;

    @Override
    protected TopNPreFilterOperator.Factory simple(SimpleOptions options) {
        return new TopNPreFilterOperator.Factory(ElementType.LONG, 0, true, false, LIMIT);
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new TupleLongLongBlockSourceOperator(
            blockFactory,
            LongStream.range(0, size).mapToObj(i -> Tuple.tuple(randomLongBetween(0, size), i))
        );
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("TopNPreFilterOperator[channel=0, asc=true, nullsFirst=false, limit=" + LIMIT + "]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return expectedDescriptionOfSimple();
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        TreeSet<Long> seen = new TreeSet<>();
        List<Long> expectedIds = new ArrayList<>();
        for (Page page : input) {
            LongBlock keys = page.getBlock(0);
            LongBlock ids = page.getBlock(1);
            for (int p = 0; p < page.getPositionCount(); p++) {
                seen.add(keys.getLong(p));
            }
            Long bottom = seen.size() < LIMIT ? null : nthSmallest(seen, LIMIT);
            for (int p = 0; p < page.getPositionCount(); p++) {
                if (bottom == null || keys.getLong(p) <= bottom) {
                    expectedIds.add(ids.getLong(p));
                }
            }
        }
        List<Long> actualIds = new ArrayList<>();
        for (Page page : results) {
            LongBlock ids = page.getBlock(1);
            for (int p = 0; p < page.getPositionCount(); p++) {
                actualIds.add(ids.getLong(p));
            }
        }
        assertThat(actualIds, equalTo(expectedIds));

        // The guarantee the optimizer relies on: no row whose key is among the final top LIMIT distinct keys is dropped.
        Set<Long> topKeys = new HashSet<>();
        Iterator<Long> it = seen.iterator();
        for (int i = 0; i < LIMIT && it.hasNext(); i++) {
            topKeys.add(it.next());
        }
        Set<Long> survivingIds = new HashSet<>(actualIds);
        for (Page page : input) {
            LongBlock keys = page.getBlock(0);
            LongBlock ids = page.getBlock(1);
            for (int p = 0; p < page.getPositionCount(); p++) {
                if (topKeys.contains(keys.getLong(p))) {
                    assertTrue(
                        "row " + ids.getLong(p) + " with top key " + keys.getLong(p) + " was dropped",
                        survivingIds.contains(ids.getLong(p))
                    );
                }
            }
        }
    }

    private static long nthSmallest(TreeSet<Long> values, int n) {
        Iterator<Long> it = values.iterator();
        long v = it.next();
        for (int i = 1; i < n; i++) {
            v = it.next();
        }
        return v;
    }

    public void testPassesEverythingThroughUntilLimitDistinctKeysSeen() {
        DriverContext ctx = driverContext();
        try (TopNPreFilterOperator op = operator(ctx, true, false, 3); Page page = page(ctx, 5L, 1L, 5L, 1L)) {
            op.addInput(page.shallowCopy());
            // only two distinct keys, so the page is passed through untouched
            try (Page out = op.getOutput()) {
                assertThat(out.getBlock(0), sameInstance(page.getBlock(0)));
            }
            op.addInput(page(ctx, 9L, 2L, 7L));
            // distinct keys are now [1, 2, 5, 7, 9]; the 3rd smallest is 5
            assertKeys(op.getOutput(), Arrays.asList(List.of(2L)));
        }
    }

    public void testKeepsTiesWithTheWorstKey() {
        DriverContext ctx = driverContext();
        try (TopNPreFilterOperator op = operator(ctx, true, false, 2)) {
            op.addInput(page(ctx, 3L, 1L, 2L, 3L, 2L, 4L));
            assertKeys(op.getOutput(), Arrays.asList(List.of(1L), List.of(2L), List.of(2L)));
        }
    }

    public void testDescending() {
        DriverContext ctx = driverContext();
        try (TopNPreFilterOperator op = operator(ctx, false, false, 2)) {
            op.addInput(page(ctx, 1L, 9L, 5L, 9L, 4L));
            // distinct keys are [1, 4, 5, 9]; the 2nd largest is 5
            assertKeys(op.getOutput(), Arrays.asList(List.of(9L), List.of(5L), List.of(9L)));
        }
    }

    public void testDropsEntirePage() {
        DriverContext ctx = driverContext();
        try (TopNPreFilterOperator op = operator(ctx, true, false, 2)) {
            op.addInput(page(ctx, 1L, 2L));
            assertKeys(op.getOutput(), Arrays.asList(List.of(1L), List.of(2L)));
            op.addInput(page(ctx, 3L, 4L, 5L));
            assertThat(op.getOutput(), nullValue());
        }
    }

    public void testNullsLast() {
        DriverContext ctx = driverContext();
        try (TopNPreFilterOperator op = operator(ctx, true, false, 2)) {
            // fewer than 2 distinct keys seen: nulls pass through
            op.addInput(page(ctx, null, 1L));
            assertKeys(op.getOutput(), Arrays.asList(null, List.of(1L)));
            // 2 distinct non-null keys seen: nulls are ranked after them and dropped
            op.addInput(page(ctx, null, 2L, 3L, null, 1L));
            assertKeys(op.getOutput(), Arrays.asList(List.of(2L), List.of(1L)));
            // an all-null page is dropped entirely
            op.addInput(page(ctx, null, null));
            assertThat(op.getOutput(), nullValue());
        }
    }

    public void testNullsFirst() {
        DriverContext ctx = driverContext();
        try (TopNPreFilterOperator op = operator(ctx, true, true, 2)) {
            op.addInput(page(ctx, null, 2L, 3L, null, 1L));
            assertKeys(op.getOutput(), Arrays.asList(null, List.of(2L), null, List.of(1L)));
            try (Page allNulls = page(ctx, null, null)) {
                op.addInput(allNulls.shallowCopy());
                try (Page out = op.getOutput()) {
                    assertThat(out.getBlock(0), sameInstance(allNulls.getBlock(0)));
                }
            }
        }
    }

    public void testMultivaluedKeysAreKeptIfAnyValueIsCompetitive() {
        DriverContext ctx = driverContext();
        try (TopNPreFilterOperator op = operator(ctx, true, false, 2)) {
            // every value counts as a distinct key: [1, 2, 3, 7, 8, 10], the 2nd smallest is 2
            op.addInput(page(ctx, List.of(10L, 1L), 2L, List.of(7L, 8L), 3L));
            assertKeys(op.getOutput(), Arrays.asList(List.of(10L, 1L), List.of(2L)));
        }
    }

    public void testFactoryRejectsInvalidArguments() {
        int limit = randomIntBetween(Integer.MIN_VALUE, 0);
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> new TopNPreFilterOperator.Factory(ElementType.LONG, 0, true, false, limit)
        );
        assertThat(e.getMessage(), equalTo("limit must be positive; got [" + limit + "]"));
        e = expectThrows(
            UnsupportedOperationException.class,
            () -> new TopNPreFilterOperator.Factory(ElementType.BYTES_REF, 0, true, false, 10)
        );
        assertThat(e.getMessage(), equalTo("TopNPreFilterOperator doesn't support BYTES_REF"));
    }

    private static TopNPreFilterOperator operator(DriverContext ctx, boolean asc, boolean nullsFirst, int limit) {
        return new TopNPreFilterOperator(ctx, 0, asc, nullsFirst, limit);
    }

    private static Page page(DriverContext ctx, Object... keys) {
        return new Page(BlockTestUtils.asBlock(ctx.blockFactory(), ElementType.LONG, Arrays.asList(keys)));
    }

    private static void assertKeys(Page out, List<List<Long>> expected) {
        try {
            Block keys = out.getBlock(0);
            List<List<Long>> actual = BlockTestUtils.valuesAtPositions(keys, 0, keys.getPositionCount());
            assertThat(actual, equalTo(expected));
        } finally {
            out.releaseBlocks();
        }
    }
}
