/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class ChangePointFillEmptyBucketsOperatorTests extends ESTestCase {

    private static final Rounding.Prepared HOUR_ROUNDING = Rounding.builder(Rounding.DateTimeUnit.HOUR_OF_DAY).build().prepareForUnknown();

    private DriverContext driverContext() {
        BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();
        return new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);
    }

    public void testFillsInteriorGap() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        long hour0 = HOUR_ROUNDING.round(0L);
        long hour2 = HOUR_ROUNDING.nextRoundingValue(HOUR_ROUNDING.nextRoundingValue(hour0));
        long maxDate = HOUR_ROUNDING.nextRoundingValue(hour2);

        try (
            LongBlock.Builder keyBuilder = blockFactory.newLongBlockBuilder(2);
            DoubleBlock.Builder valueBuilder = blockFactory.newDoubleBlockBuilder(2)
        ) {
            keyBuilder.appendLong(hour0);
            keyBuilder.appendLong(hour2);
            valueBuilder.appendDouble(50d);
            valueBuilder.appendDouble(50d);
            Page input = new Page(keyBuilder.build(), valueBuilder.build());

            try (
                ChangePointFillEmptyBucketsOperator op = new ChangePointFillEmptyBucketsOperator(
                    ctx,
                    0,
                    1,
                    new int[0],
                    HOUR_ROUNDING,
                    hour0,
                    maxDate
                )
            ) {
                op.addInput(input);
                op.finish();
                Page output = op.getOutput();
                assertNotNull(output);
                assertThat(output.getPositionCount(), equalTo(3));
                LongBlock keys = (LongBlock) output.getBlock(0);
                DoubleBlock values = (DoubleBlock) output.getBlock(1);
                assertThat(keys.getLong(1), equalTo(HOUR_ROUNDING.nextRoundingValue(hour0)));
                assertThat(values.getDouble(1), equalTo(0d));
            }
        }
    }

    public void testEdgeExtensionWhenSingleBucketAtEnd() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        long start = HOUR_ROUNDING.round(0L);
        long end = start;
        for (int i = 0; i < 40; i++) {
            end = HOUR_ROUNDING.nextRoundingValue(end);
        }
        long lastBucket = previousRoundingValue(HOUR_ROUNDING, end, start);

        try (
            LongBlock.Builder keyBuilder = blockFactory.newLongBlockBuilder(1);
            LongBlock.Builder valueBuilder = blockFactory.newLongBlockBuilder(1)
        ) {
            keyBuilder.appendLong(lastBucket);
            valueBuilder.appendLong(99L);
            Page input = new Page(keyBuilder.build(), valueBuilder.build());

            try (
                ChangePointFillEmptyBucketsOperator op = new ChangePointFillEmptyBucketsOperator(
                    ctx,
                    0,
                    1,
                    new int[0],
                    HOUR_ROUNDING,
                    start,
                    end
                )
            ) {
                op.addInput(input);
                op.finish();
                Page output = op.getOutput();
                assertNotNull(output);
                assertThat(output.getPositionCount(), equalTo(ChangePointBucketFillUtils.MINIMUM_BUCKET_COUNT));
                LongBlock keys = (LongBlock) output.getBlock(0);
                LongBlock values = (LongBlock) output.getBlock(1);
                assertThat(keys.getLong(output.getPositionCount() - 1), equalTo(lastBucket));
                assertThat(values.getLong(output.getPositionCount() - 1), equalTo(99L));
                assertThat(values.getLong(0), equalTo(0L));
            }
        }
    }

    public void testInteriorFillWhenSparseRealBucketsAtOrAboveMinimum() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        long start = HOUR_ROUNDING.round(0L);
        List<Long> sparseHours = new ArrayList<>();
        long current = start;
        for (int i = 0; i < ChangePointBucketFillUtils.MINIMUM_BUCKET_COUNT; i++) {
            sparseHours.add(current);
            current = HOUR_ROUNDING.nextRoundingValue(current);
            current = HOUR_ROUNDING.nextRoundingValue(current);
        }
        long end = current;
        for (int i = 0; i < 5; i++) {
            end = HOUR_ROUNDING.nextRoundingValue(end);
        }

        try (
            LongBlock.Builder keyBuilder = blockFactory.newLongBlockBuilder(sparseHours.size());
            DoubleBlock.Builder valueBuilder = blockFactory.newDoubleBlockBuilder(sparseHours.size())
        ) {
            for (long hour : sparseHours) {
                keyBuilder.appendLong(hour);
                valueBuilder.appendDouble(5d);
            }
            Page input = new Page(keyBuilder.build(), valueBuilder.build());

            try (
                ChangePointFillEmptyBucketsOperator op = new ChangePointFillEmptyBucketsOperator(
                    ctx,
                    0,
                    1,
                    new int[0],
                    HOUR_ROUNDING,
                    start,
                    end
                )
            ) {
                op.addInput(input);
                op.finish();
                Page output = op.getOutput();
                assertNotNull(output);
                assertThat(output.getPositionCount(), equalTo(ChangePointBucketFillUtils.MINIMUM_BUCKET_COUNT * 2 - 1));
                DoubleBlock values = (DoubleBlock) output.getBlock(1);
                for (int i = 0; i < output.getPositionCount(); i++) {
                    if (sparseHours.contains(((LongBlock) output.getBlock(0)).getLong(i))) {
                        assertThat(values.getDouble(i), equalTo(5d));
                    } else {
                        assertThat(values.getDouble(i), equalTo(0d));
                    }
                }
            }
        }
    }

    public void testFillsInteriorGapPerGroup() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        long hour0 = HOUR_ROUNDING.round(0L);
        long hour2 = HOUR_ROUNDING.nextRoundingValue(HOUR_ROUNDING.nextRoundingValue(hour0));
        long maxDate = HOUR_ROUNDING.nextRoundingValue(hour2);

        try (
            LongBlock.Builder keyBuilder = blockFactory.newLongBlockBuilder(4);
            DoubleBlock.Builder valueBuilder = blockFactory.newDoubleBlockBuilder(4);
            LongBlock.Builder groupBuilder = blockFactory.newLongBlockBuilder(4)
        ) {
            keyBuilder.appendLong(hour0);
            valueBuilder.appendDouble(10d);
            groupBuilder.appendLong(1L);

            keyBuilder.appendLong(hour2);
            valueBuilder.appendDouble(20d);
            groupBuilder.appendLong(1L);

            keyBuilder.appendLong(hour0);
            valueBuilder.appendDouble(30d);
            groupBuilder.appendLong(2L);

            keyBuilder.appendLong(hour2);
            valueBuilder.appendDouble(40d);
            groupBuilder.appendLong(2L);

            Page input = new Page(keyBuilder.build(), valueBuilder.build(), groupBuilder.build());

            try (
                ChangePointFillEmptyBucketsOperator op = new ChangePointFillEmptyBucketsOperator(
                    ctx,
                    0,
                    1,
                    new int[] { 2 },
                    HOUR_ROUNDING,
                    hour0,
                    maxDate
                )
            ) {
                op.addInput(input);
                op.finish();

                Page groupOne = op.getOutput();
                assertNotNull(groupOne);
                assertThat(groupOne.getPositionCount(), equalTo(3));
                LongBlock groupOneGroups = (LongBlock) groupOne.getBlock(2);
                DoubleBlock groupOneValues = (DoubleBlock) groupOne.getBlock(1);
                assertThat(groupOneGroups.getLong(0), equalTo(1L));
                assertThat(groupOneValues.getDouble(1), equalTo(0d));

                Page groupTwo = op.getOutput();
                assertNotNull(groupTwo);
                assertThat(groupTwo.getPositionCount(), equalTo(3));
                LongBlock groupTwoGroups = (LongBlock) groupTwo.getBlock(2);
                DoubleBlock groupTwoValues = (DoubleBlock) groupTwo.getBlock(1);
                assertThat(groupTwoGroups.getLong(0), equalTo(2L));
                assertThat(groupTwoValues.getDouble(1), equalTo(0d));
                assertNull(op.getOutput());
            }
        }
    }

    public void testPassThroughWhenNoGapsAndEnoughValues() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        long hour0 = HOUR_ROUNDING.round(0L);
        List<Long> hours = new ArrayList<>();
        long current = hour0;
        for (int i = 0; i < 25; i++) {
            hours.add(current);
            current = HOUR_ROUNDING.nextRoundingValue(current);
        }
        long maxDate = current;

        try (
            LongBlock.Builder keyBuilder = blockFactory.newLongBlockBuilder(hours.size());
            DoubleBlock.Builder valueBuilder = blockFactory.newDoubleBlockBuilder(hours.size())
        ) {
            for (long hour : hours) {
                keyBuilder.appendLong(hour);
                valueBuilder.appendDouble(1d);
            }
            Page input = new Page(keyBuilder.build(), valueBuilder.build());

            try (
                ChangePointFillEmptyBucketsOperator op = new ChangePointFillEmptyBucketsOperator(
                    ctx,
                    0,
                    1,
                    new int[0],
                    HOUR_ROUNDING,
                    hour0,
                    maxDate
                )
            ) {
                op.addInput(input);
                op.finish();
                Page output = op.getOutput();
                assertNotNull(output);
                assertThat(output.getPositionCount(), equalTo(input.getPositionCount()));
                input.releaseBlocks();
            }
        }
    }

    public void testPreservesOutOfRangeRowsAfterFill() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        long hour0 = HOUR_ROUNDING.round(0L);
        long hour2 = HOUR_ROUNDING.nextRoundingValue(HOUR_ROUNDING.nextRoundingValue(hour0));
        long hour5 = HOUR_ROUNDING.nextRoundingValue(HOUR_ROUNDING.nextRoundingValue(HOUR_ROUNDING.nextRoundingValue(hour2)));
        long maxDate = HOUR_ROUNDING.nextRoundingValue(hour2);

        try (
            LongBlock.Builder keyBuilder = blockFactory.newLongBlockBuilder(3);
            DoubleBlock.Builder valueBuilder = blockFactory.newDoubleBlockBuilder(3)
        ) {
            keyBuilder.appendLong(hour0);
            valueBuilder.appendDouble(10d);
            keyBuilder.appendLong(hour2);
            valueBuilder.appendDouble(20d);
            keyBuilder.appendLong(hour5);
            valueBuilder.appendDouble(30d);
            Page input = new Page(keyBuilder.build(), valueBuilder.build());

            try (
                ChangePointFillEmptyBucketsOperator op = new ChangePointFillEmptyBucketsOperator(
                    ctx,
                    0,
                    1,
                    new int[0],
                    HOUR_ROUNDING,
                    hour0,
                    maxDate
                )
            ) {
                op.addInput(input);
                op.finish();
                Page output = op.getOutput();
                assertNotNull(output);
                assertThat(output.getPositionCount(), equalTo(4));
                LongBlock keys = (LongBlock) output.getBlock(0);
                DoubleBlock values = (DoubleBlock) output.getBlock(1);
                assertThat(keys.getLong(0), equalTo(hour0));
                assertThat(keys.getLong(1), equalTo(HOUR_ROUNDING.nextRoundingValue(hour0)));
                assertThat(values.getDouble(1), equalTo(0d));
                assertThat(keys.getLong(2), equalTo(hour2));
                assertThat(keys.getLong(3), equalTo(hour5));
                assertThat(values.getDouble(3), equalTo(30d));
            }
        }
    }

    public void testNoOutputWhenNoInput() {
        DriverContext ctx = driverContext();
        long hour0 = HOUR_ROUNDING.round(0L);
        long maxDate = hour0;
        for (int i = 0; i < 22; i++) {
            maxDate = HOUR_ROUNDING.nextRoundingValue(maxDate);
        }

        try (
            ChangePointFillEmptyBucketsOperator op = new ChangePointFillEmptyBucketsOperator(
                ctx,
                0,
                1,
                new int[0],
                HOUR_ROUNDING,
                hour0,
                maxDate
            )
        ) {
            op.finish();
            assertNull(op.getOutput());
            assertTrue(op.isFinished());
        }
    }

    private static long previousRoundingValue(Rounding.Prepared rounding, long key, long minDate) {
        long t = rounding.round(minDate);
        long previous = t;
        while (t < key) {
            previous = t;
            t = rounding.nextRoundingValue(t);
        }
        return previous;
    }
}
