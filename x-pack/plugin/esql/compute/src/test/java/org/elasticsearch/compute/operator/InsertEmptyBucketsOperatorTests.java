/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.InsertEmptyBucketsOperator.DefaultValue;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.operator.blocksource.AbstractBlockSourceOperator;
import org.elasticsearch.core.TimeValue;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class InsertEmptyBucketsOperatorTests extends OperatorTestCase {

    private static final long T0 = 0L;
    private static final long T1 = 1000L;
    private static final long T2 = 2000L;
    private static final long[] BOUNDARIES = { T0, T1, T2 };
    private static final long INPUT_VALUE = 42L;
    private static final int MAX_PAGE_SIZE = 1000;

    // A 1000ms rounding over [0, 3000) generates exactly the boundaries {T0, T1, T2} on the fly.
    private static InsertEmptyBucketsOperator.DateCursorFactory dateCursorFactory() {
        return new InsertEmptyBucketsOperator.DateCursorFactory(
            Rounding.builder(TimeValue.timeValueMillis(1000)).build().prepareForUnknown(),
            0L,
            3000L,
            false
        );
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        // A single (bucketless-grouping) STATS c = COUNT(*) BY b = BUCKET(...): a bucket key column and a zero-filled count.
        return new InsertEmptyBucketsOperator.Factory(new LinkedHashMap<>() {
            {
                put(0, dateCursorFactory());
            }
            // defaultValues holds only value columns (the zero-filled count), not the bucket key
        }, List.of(), Map.of(1, new DefaultValue(ElementType.LONG, 0L)), MAX_PAGE_SIZE);
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        // Every input row lands in the first bucket (T0), so T1 and T2 are always missing and must be filled with zero.
        return new AbstractBlockSourceOperator(blockFactory, 8 * 1024) {
            private int idx;

            @Override
            protected int remaining() {
                return size - idx;
            }

            @Override
            protected Page createPage(int positionOffset, int length) {
                idx += length;
                try (
                    LongBlock.Builder bucketBuilder = blockFactory.newLongBlockBuilder(length);
                    LongBlock.Builder valueBuilder = blockFactory.newLongBlockBuilder(length)
                ) {
                    for (int i = 0; i < length; i++) {
                        bucketBuilder.appendLong(T0);
                        valueBuilder.appendLong(INPUT_VALUE);
                    }
                    return new Page(bucketBuilder.build(), valueBuilder.build());
                }
            }
        };
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        int inputRows = input.stream().mapToInt(Page::getPositionCount).sum();

        // The output may be split across several pages; flatten it while preserving order.
        List<Long> buckets = new ArrayList<>();
        List<Long> values = new ArrayList<>();
        for (Page page : results) {
            LongBlock bucketBlock = page.getBlock(0);
            LongBlock valueBlock = page.getBlock(1);
            for (int p = 0; p < page.getPositionCount(); p++) {
                buckets.add(bucketBlock.getLong(p));
                values.add(valueBlock.getLong(p));
            }
        }

        if (inputRows == 0) {
            // No input at all: the sole BUCKET grouping still emits its full range, every boundary an empty (zero) bucket.
            assertThat(buckets, equalTo(List.of(T0, T1, T2)));
            values.forEach(v -> assertThat(v, equalTo(0L)));
            return;
        }

        assertThat(buckets.size(), equalTo(inputRows + 2));
        // All real rows land in bucket T0 and therefore sort first, each carrying the input value.
        for (int p = 0; p < inputRows; p++) {
            assertThat(buckets.get(p), equalTo(T0));
            assertThat(values.get(p), equalTo(INPUT_VALUE));
        }
        // The two missing boundaries are appended in ascending order with a zero count.
        assertThat(buckets.get(inputRows), equalTo(T1));
        assertThat(values.get(inputRows), equalTo(0L));
        assertThat(buckets.get(inputRows + 1), equalTo(T2));
        assertThat(values.get(inputRows + 1), equalTo(0L));
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("InsertEmptyBucketsOperator[bucketCursorFactories=[0], groupChannels=[], defaultValues=[1]]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo("InsertEmptyBucketsOperator[bucketCursors=[0], groupChannels=[], defaultValues=[1]]");
    }

    @Override
    protected void assertStatus(Map<String, Object> map, List<Page> input, List<Page> output) {
        assertThat(map, nullValue());
    }

    /**
     * Rows are ordered by (non-bucket grouping keys, bucket key) and the missing buckets are filled per observed
     * combination (cartesian product), with default fills on every non-group/non-bucket channel.
     */
    public void testCartesianFillZeroAndNull() {
        BytesRef a = new BytesRef("A");
        BytesRef b = new BytesRef("B");
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();

        Page input;
        try (
            BytesRefBlock.Builder hostBuilder = blockFactory.newBytesRefBlockBuilder(3);
            LongBlock.Builder bucketBuilder = blockFactory.newLongBlockBuilder(3);
            LongBlock.Builder countBuilder = blockFactory.newLongBlockBuilder(3);
            DoubleBlock.Builder sumBuilder = blockFactory.newDoubleBlockBuilder(3)
        ) {
            hostBuilder.appendBytesRef(a);
            bucketBuilder.appendLong(T0);
            countBuilder.appendLong(5);
            sumBuilder.appendDouble(1.5);

            hostBuilder.appendBytesRef(a);
            bucketBuilder.appendLong(T2);
            countBuilder.appendLong(7);
            sumBuilder.appendDouble(2.5);

            hostBuilder.appendBytesRef(b);
            bucketBuilder.appendLong(T1);
            countBuilder.appendLong(3);
            sumBuilder.appendDouble(9.0);

            input = new Page(hostBuilder.build(), bucketBuilder.build(), countBuilder.build(), sumBuilder.build());
        }

        InsertEmptyBucketsOperator.Factory factory = new InsertEmptyBucketsOperator.Factory(new LinkedHashMap<>() {
            {
                put(1, dateCursorFactory());
            }
        }, List.of(0), Map.of(2, new DefaultValue(ElementType.LONG, 0L), 3, new DefaultValue(ElementType.DOUBLE, null)), MAX_PAGE_SIZE);

        Page result = runToSinglePage(ctx, factory, input);
        try {
            BytesRefBlock hosts = result.getBlock(0);
            LongBlock buckets = result.getBlock(1);
            LongBlock counts = result.getBlock(2);
            DoubleBlock sums = result.getBlock(3);

            // Ordered by (host, bucket): 3 real rows interleaved with (A,T1) + (B,T0) + (B,T2) = 6.
            assertThat(result.getPositionCount(), equalTo(6));

            BytesRef scratch = new BytesRef();
            assertRow(hosts, buckets, counts, sums, 0, a, T0, 5, 1.5, scratch);
            assertFillRow(hosts, buckets, counts, sums, 1, a, T1, scratch);
            assertRow(hosts, buckets, counts, sums, 2, a, T2, 7, 2.5, scratch);
            assertFillRow(hosts, buckets, counts, sums, 3, b, T0, scratch);
            assertRow(hosts, buckets, counts, sums, 4, b, T1, 3, 9.0, scratch);
            assertFillRow(hosts, buckets, counts, sums, 5, b, T2, scratch);
        } finally {
            result.releaseBlocks();
        }
    }

    /**
     * The operator sorts its input itself, so unsorted rows spread across several pages are still gap-filled correctly by
     * (non-bucket grouping keys, bucket key). This is what decouples it from the upstream aggregation's emission order.
     */
    public void testUnsortedMultiPageInput() {
        BytesRef a = new BytesRef("A");
        BytesRef b = new BytesRef("B");
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();

        // Rows arrive scrambled and split across two pages; only their sorted (host, bucket) order must matter.
        Page page1;
        try (
            BytesRefBlock.Builder hostBuilder = blockFactory.newBytesRefBlockBuilder(2);
            LongBlock.Builder bucketBuilder = blockFactory.newLongBlockBuilder(2);
            LongBlock.Builder countBuilder = blockFactory.newLongBlockBuilder(2);
            DoubleBlock.Builder sumBuilder = blockFactory.newDoubleBlockBuilder(2)
        ) {
            hostBuilder.appendBytesRef(b);
            bucketBuilder.appendLong(T1);
            countBuilder.appendLong(3);
            sumBuilder.appendDouble(9.0);

            hostBuilder.appendBytesRef(a);
            bucketBuilder.appendLong(T2);
            countBuilder.appendLong(7);
            sumBuilder.appendDouble(2.5);

            page1 = new Page(hostBuilder.build(), bucketBuilder.build(), countBuilder.build(), sumBuilder.build());
        }
        Page page2;
        try (
            BytesRefBlock.Builder hostBuilder = blockFactory.newBytesRefBlockBuilder(1);
            LongBlock.Builder bucketBuilder = blockFactory.newLongBlockBuilder(1);
            LongBlock.Builder countBuilder = blockFactory.newLongBlockBuilder(1);
            DoubleBlock.Builder sumBuilder = blockFactory.newDoubleBlockBuilder(1)
        ) {
            hostBuilder.appendBytesRef(a);
            bucketBuilder.appendLong(T0);
            countBuilder.appendLong(5);
            sumBuilder.appendDouble(1.5);

            page2 = new Page(hostBuilder.build(), bucketBuilder.build(), countBuilder.build(), sumBuilder.build());
        }

        InsertEmptyBucketsOperator.Factory factory = new InsertEmptyBucketsOperator.Factory(new LinkedHashMap<>() {
            {
                put(1, dateCursorFactory());
            }
        }, List.of(0), Map.of(2, new DefaultValue(ElementType.LONG, 0L), 3, new DefaultValue(ElementType.DOUBLE, null)), MAX_PAGE_SIZE);

        Page result;
        try (InsertEmptyBucketsOperator op = (InsertEmptyBucketsOperator) factory.get(ctx)) {
            op.addInput(page1);
            op.addInput(page2);
            op.finish();
            // The 6 output rows fit in a single page (MAX_PAGE_SIZE).
            result = op.getOutput();
            assertNotNull(result);
            assertNull(op.getOutput());
            assertTrue(op.isFinished());
        }

        try {
            BytesRefBlock hosts = result.getBlock(0);
            LongBlock buckets = result.getBlock(1);
            LongBlock counts = result.getBlock(2);
            DoubleBlock sums = result.getBlock(3);

            // Ordered by (host, bucket): 3 real rows interleaved with (A,T1) + (B,T0) + (B,T2) = 6.
            assertThat(result.getPositionCount(), equalTo(6));

            BytesRef scratch = new BytesRef();
            assertRow(hosts, buckets, counts, sums, 0, a, T0, 5, 1.5, scratch);
            assertFillRow(hosts, buckets, counts, sums, 1, a, T1, scratch);
            assertRow(hosts, buckets, counts, sums, 2, a, T2, 7, 2.5, scratch);
            assertFillRow(hosts, buckets, counts, sums, 3, b, T0, scratch);
            assertRow(hosts, buckets, counts, sums, 4, b, T1, 3, 9.0, scratch);
            assertFillRow(hosts, buckets, counts, sums, 5, b, T2, scratch);
        } finally {
            result.releaseBlocks();
        }
    }

    /**
     * Numeric (double) bucket boundaries are filled just like the date (long) ones, in ascending bucket order.
     */
    public void testNumericBoundaries() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();

        Page input;
        try (
            DoubleBlock.Builder bucketBuilder = blockFactory.newDoubleBlockBuilder(1);
            LongBlock.Builder countBuilder = blockFactory.newLongBlockBuilder(1)
        ) {
            bucketBuilder.appendDouble(2.0);
            countBuilder.appendLong(4);
            input = new Page(bucketBuilder.build(), countBuilder.build());
        }

        // roundTo=1.0 over [1.0, 4.0) generates the boundaries {1.0, 2.0, 3.0} on the fly.
        InsertEmptyBucketsOperator.Factory factory = new InsertEmptyBucketsOperator.Factory(new LinkedHashMap<>() {
            {
                put(0, new InsertEmptyBucketsOperator.NumericCursorFactory(1.0, 1.0, 4.0));
            }
        }, List.of(), Map.of(1, new DefaultValue(ElementType.LONG, 0L)), MAX_PAGE_SIZE);

        Page result = runToSinglePage(ctx, factory, input);
        try {
            DoubleBlock buckets = result.getBlock(0);
            LongBlock counts = result.getBlock(1);
            assertThat(result.getPositionCount(), equalTo(3));
            // Ascending bucket order: fill 1.0, real 2.0, fill 3.0.
            assertThat(buckets.getDouble(0), equalTo(1.0));
            assertThat(counts.getLong(0), equalTo(0L));
            assertThat(buckets.getDouble(1), equalTo(2.0));
            assertThat(counts.getLong(1), equalTo(4L));
            assertThat(buckets.getDouble(2), equalTo(3.0));
            assertThat(counts.getLong(2), equalTo(0L));
        } finally {
            result.releaseBlocks();
        }
    }

    /**
     * An empty boundary range ({@code from == to}) must not emit synthetic rows.
     */
    public void testEmptyBoundaryRangeProducesNoFills() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();

        Page input;
        try (
            LongBlock.Builder bucketBuilder = blockFactory.newLongBlockBuilder(1);
            LongBlock.Builder countBuilder = blockFactory.newLongBlockBuilder(1)
        ) {
            bucketBuilder.appendLong(T0);
            countBuilder.appendLong(4L);
            input = new Page(bucketBuilder.build(), countBuilder.build());
        }

        InsertEmptyBucketsOperator.Factory factory = new InsertEmptyBucketsOperator.Factory(new LinkedHashMap<>() {
            {
                put(
                    0,
                    new InsertEmptyBucketsOperator.DateCursorFactory(
                        Rounding.builder(TimeValue.timeValueMillis(1000)).build().prepareForUnknown(),
                        1000L,
                        1000L,
                        false
                    )
                );
            }
        }, List.of(), Map.of(1, new DefaultValue(ElementType.LONG, 0L)), MAX_PAGE_SIZE);

        Page result = runToSinglePage(ctx, factory, input);
        try {
            LongBlock buckets = result.getBlock(0);
            LongBlock counts = result.getBlock(1);
            assertThat(result.getPositionCount(), equalTo(1));
            assertThat(buckets.getLong(0), equalTo(T0));
            assertThat(counts.getLong(0), equalTo(4L));
        } finally {
            result.releaseBlocks();
        }
    }

    /**
     * The gap-filled output is streamed in bounded pages so a very large result is never materialized all at once.
     */
    public void testStreamingAcrossMultiplePages() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        int groups = 50;
        int smallPageSize = 8;

        // Each group has data only in the first bucket, so every group contributes two fills (T1, T2).
        Page input;
        try (
            LongBlock.Builder groupBuilder = blockFactory.newLongBlockBuilder(groups);
            LongBlock.Builder bucketBuilder = blockFactory.newLongBlockBuilder(groups);
            LongBlock.Builder countBuilder = blockFactory.newLongBlockBuilder(groups)
        ) {
            for (int g = 0; g < groups; g++) {
                groupBuilder.appendLong(g);
                bucketBuilder.appendLong(T0);
                countBuilder.appendLong(1);
            }
            input = new Page(groupBuilder.build(), bucketBuilder.build(), countBuilder.build());
        }

        InsertEmptyBucketsOperator.Factory factory = new InsertEmptyBucketsOperator.Factory(new LinkedHashMap<>() {
            {
                put(1, dateCursorFactory());
            }
        }, List.of(0), Map.of(2, new DefaultValue(ElementType.LONG, 0L)), smallPageSize);

        List<Page> pages = new ArrayList<>();
        try (InsertEmptyBucketsOperator op = (InsertEmptyBucketsOperator) factory.get(ctx)) {
            op.addInput(input);
            op.finish();
            Page page;
            while ((page = op.getOutput()) != null) {
                assertThat(page.getPositionCount() <= smallPageSize, equalTo(true));
                pages.add(page);
            }
            assertTrue(op.isFinished());
            // groups * boundaries rows total, split into more than one page.
            assertThat(pages.size() > 1, equalTo(true));
            int total = pages.stream().mapToInt(Page::getPositionCount).sum();
            assertThat(total, equalTo(groups * BOUNDARIES.length));
        } finally {
            for (Page page : pages) {
                page.releaseBlocks();
            }
        }
    }

    /**
     * Two bucket channels are filled as a lexicographic cartesian product of per-channel boundaries.
     */
    public void testTwoBucketCrossProductFill() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();

        Page input;
        try (
            BytesRefBlock.Builder hostBuilder = blockFactory.newBytesRefBlockBuilder(2);
            LongBlock.Builder bucketDayBuilder = blockFactory.newLongBlockBuilder(2);
            LongBlock.Builder bucketHourBuilder = blockFactory.newLongBlockBuilder(2);
            LongBlock.Builder countBuilder = blockFactory.newLongBlockBuilder(2)
        ) {
            // Existing rows: (A, day=T0, hour=T0) and (A, day=T1, hour=T1)
            hostBuilder.appendBytesRef(new BytesRef("A"));
            bucketDayBuilder.appendLong(T0);
            bucketHourBuilder.appendLong(T0);
            countBuilder.appendLong(5);

            hostBuilder.appendBytesRef(new BytesRef("A"));
            bucketDayBuilder.appendLong(T1);
            bucketHourBuilder.appendLong(T1);
            countBuilder.appendLong(7);

            input = new Page(hostBuilder.build(), bucketDayBuilder.build(), bucketHourBuilder.build(), countBuilder.build());
        }

        // Ordered map: channel 1 (day) is the outer, channel 2 (hour) the inner dimension of the cross product.
        InsertEmptyBucketsOperator.Factory factory = new InsertEmptyBucketsOperator.Factory(new LinkedHashMap<>() {
            {
                put(1, dateCursorFactory());
                put(2, dateCursorFactory());
            }
        }, List.of(0), Map.of(3, new DefaultValue(ElementType.LONG, 0L)), MAX_PAGE_SIZE);

        Page result = runToSinglePage(ctx, factory, input);
        try {
            BytesRefBlock hosts = result.getBlock(0);
            LongBlock day = result.getBlock(1);
            LongBlock hour = result.getBlock(2);
            LongBlock counts = result.getBlock(3);
            BytesRef scratch = new BytesRef();

            // 3x3 day/hour cartesian product for host A.
            assertThat(result.getPositionCount(), equalTo(9));

            int p = 0;
            assertTuple(hosts, day, hour, counts, p++, "A", T0, T0, 5, scratch);
            assertTuple(hosts, day, hour, counts, p++, "A", T0, T1, 0, scratch);
            assertTuple(hosts, day, hour, counts, p++, "A", T0, T2, 0, scratch);
            assertTuple(hosts, day, hour, counts, p++, "A", T1, T0, 0, scratch);
            assertTuple(hosts, day, hour, counts, p++, "A", T1, T1, 7, scratch);
            assertTuple(hosts, day, hour, counts, p++, "A", T1, T2, 0, scratch);
            assertTuple(hosts, day, hour, counts, p++, "A", T2, T0, 0, scratch);
            assertTuple(hosts, day, hour, counts, p++, "A", T2, T1, 0, scratch);
            assertTuple(hosts, day, hour, counts, p++, "A", T2, T2, 0, scratch);
        } finally {
            result.releaseBlocks();
        }
    }

    /**
     * With no input at all and the BUCKET as the sole grouping, the full range is still emitted as empty (zero) buckets.
     */
    public void testEmptyInputFillsFullRangeNoGroups() {
        DriverContext ctx = driverContext();
        InsertEmptyBucketsOperator.Factory factory = new InsertEmptyBucketsOperator.Factory(new LinkedHashMap<>() {
            {
                put(0, dateCursorFactory());
            }
        }, List.of(), Map.of(1, new DefaultValue(ElementType.LONG, 0L)), MAX_PAGE_SIZE);

        Page result;
        try (InsertEmptyBucketsOperator op = (InsertEmptyBucketsOperator) factory.get(ctx)) {
            op.finish();
            result = op.getOutput();
            assertNotNull(result);
            assertNull(op.getOutput());
            assertTrue(op.isFinished());
        }

        try {
            LongBlock buckets = result.getBlock(0);
            LongBlock counts = result.getBlock(1);
            assertThat(result.getPositionCount(), equalTo(BOUNDARIES.length));
            for (int p = 0; p < BOUNDARIES.length; p++) {
                assertThat(buckets.getLong(buckets.getFirstValueIndex(p)), equalTo(BOUNDARIES[p]));
                assertThat(counts.getLong(counts.getFirstValueIndex(p)), equalTo(0L));
            }
        } finally {
            result.releaseBlocks();
        }
    }

    /**
     * With an extra (non-bucket) grouping there are no groups to enumerate on empty input, so nothing is emitted.
     */
    public void testEmptyInputWithGroupsEmitsNothing() {
        DriverContext ctx = driverContext();
        InsertEmptyBucketsOperator.Factory factory = new InsertEmptyBucketsOperator.Factory(new LinkedHashMap<>() {
            {
                put(1, dateCursorFactory());
            }
        }, List.of(0), Map.of(2, new DefaultValue(ElementType.LONG, 0L)), MAX_PAGE_SIZE);

        try (InsertEmptyBucketsOperator op = (InsertEmptyBucketsOperator) factory.get(ctx)) {
            op.finish();
            assertTrue(op.isFinished());
            assertNull(op.getOutput());
        }
    }

    /**
     * Empty input with two bucket channels (and no groups) emits the full lexicographic cartesian product of boundaries.
     */
    public void testEmptyInputTwoBucketGrid() {
        DriverContext ctx = driverContext();
        InsertEmptyBucketsOperator.Factory factory = new InsertEmptyBucketsOperator.Factory(new LinkedHashMap<>() {
            {
                put(0, dateCursorFactory());
                put(1, dateCursorFactory());
            }
        }, List.of(), Map.of(2, new DefaultValue(ElementType.LONG, 0L)), MAX_PAGE_SIZE);

        Page result;
        try (InsertEmptyBucketsOperator op = (InsertEmptyBucketsOperator) factory.get(ctx)) {
            op.finish();
            result = op.getOutput();
            assertNotNull(result);
            assertNull(op.getOutput());
            assertTrue(op.isFinished());
        }

        try {
            LongBlock day = result.getBlock(0);
            LongBlock hour = result.getBlock(1);
            LongBlock counts = result.getBlock(2);
            assertThat(result.getPositionCount(), equalTo(BOUNDARIES.length * BOUNDARIES.length));
            int p = 0;
            for (long expectedDay : BOUNDARIES) {
                for (long expectedHour : BOUNDARIES) {
                    assertThat(day.getLong(day.getFirstValueIndex(p)), equalTo(expectedDay));
                    assertThat(hour.getLong(hour.getFirstValueIndex(p)), equalTo(expectedHour));
                    assertThat(counts.getLong(counts.getFirstValueIndex(p)), equalTo(0L));
                    p++;
                }
            }
        } finally {
            result.releaseBlocks();
        }
    }

    private static Page runToSinglePage(DriverContext ctx, Operator.OperatorFactory factory, Page input) {
        try (InsertEmptyBucketsOperator op = (InsertEmptyBucketsOperator) factory.get(ctx)) {
            op.addInput(input);
            op.finish();
            Page result = op.getOutput();
            assertNotNull(result);
            // The remaining pull must be empty for these small inputs.
            assertNull(op.getOutput());
            assertTrue(op.isFinished());
            return result;
        }
    }

    private static void assertRow(
        BytesRefBlock hosts,
        LongBlock buckets,
        LongBlock counts,
        DoubleBlock sums,
        int position,
        BytesRef host,
        long bucket,
        long count,
        double sum,
        BytesRef scratch
    ) {
        assertThat(hosts.getBytesRef(hosts.getFirstValueIndex(position), scratch), equalTo(host));
        assertThat(buckets.getLong(buckets.getFirstValueIndex(position)), equalTo(bucket));
        assertThat(counts.getLong(counts.getFirstValueIndex(position)), equalTo(count));
        assertThat(sums.getDouble(sums.getFirstValueIndex(position)), equalTo(sum));
    }

    private static void assertFillRow(
        BytesRefBlock hosts,
        LongBlock buckets,
        LongBlock counts,
        DoubleBlock sums,
        int position,
        BytesRef host,
        long bucket,
        BytesRef scratch
    ) {
        assertThat(hosts.getBytesRef(hosts.getFirstValueIndex(position), scratch), equalTo(host));
        assertThat(buckets.getLong(buckets.getFirstValueIndex(position)), equalTo(bucket));
        // COUNT is a zero-value channel, so an empty bucket counts zero...
        assertThat(counts.getLong(counts.getFirstValueIndex(position)), equalTo(0L));
        // ...while any other aggregate (here SUM) is filled with null.
        assertThat(sums.isNull(position), equalTo(true));
    }

    private static void assertTuple(
        BytesRefBlock hosts,
        LongBlock day,
        LongBlock hour,
        LongBlock counts,
        int position,
        String host,
        long expectedDay,
        long expectedHour,
        long expectedCount,
        BytesRef scratch
    ) {
        assertThat(hosts.getBytesRef(hosts.getFirstValueIndex(position), scratch), equalTo(new BytesRef(host)));
        assertThat(day.getLong(day.getFirstValueIndex(position)), equalTo(expectedDay));
        assertThat(hour.getLong(hour.getFirstValueIndex(position)), equalTo(expectedHour));
        assertThat(counts.getLong(counts.getFirstValueIndex(position)), equalTo(expectedCount));
    }
}
