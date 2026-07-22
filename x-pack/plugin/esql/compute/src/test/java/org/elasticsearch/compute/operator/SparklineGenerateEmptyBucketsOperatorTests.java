/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.operator.blocksource.AbstractBlockSourceOperator;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

public class SparklineGenerateEmptyBucketsOperatorTests extends OperatorTestCase {

    static final Rounding.Prepared ROUNDING = Rounding.builder(TimeValue.timeValueHours(1)).build().prepareForUnknown();
    static final long MIN_DATE = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2025-01-01T00:00:00Z");
    static final long T0 = MIN_DATE;
    static final long T1 = T0 + TimeValue.timeValueHours(1).millis();
    static final long T2 = T0 + TimeValue.timeValueHours(2).millis();
    static final long T3 = T0 + TimeValue.timeValueHours(3).millis();
    static final long MAX_DATE = T3;
    static final List<Long> DATE_BUCKETS = List.of(T0, T1, T2);
    private final List<Long> EXPECTED_VALUE_LIST = List.of(42L, 0L, 99L);

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return new SparklineGenerateEmptyBucketsOperator.Factory(1, ROUNDING, MIN_DATE, MAX_DATE);
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
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
                    LongBlock.Builder valueBuilder = blockFactory.newLongBlockBuilder(length * 2);
                    LongBlock.Builder dateBuilder = blockFactory.newLongBlockBuilder(length * 2)
                ) {
                    for (int i = 0; i < length; i++) {
                        valueBuilder.beginPositionEntry();
                        valueBuilder.appendLong(EXPECTED_VALUE_LIST.get(0));
                        valueBuilder.appendLong(EXPECTED_VALUE_LIST.get(2));
                        valueBuilder.endPositionEntry();

                        dateBuilder.beginPositionEntry();
                        dateBuilder.appendLong(T0);
                        dateBuilder.appendLong(T2);
                        dateBuilder.endPositionEntry();
                    }
                    return new Page(valueBuilder.build(), dateBuilder.build());
                }
            }
        };
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        assertThat(results, hasSize(input.size()));
        for (int p = 0; p < results.size(); p++) {
            Page resultPage = results.get(p);
            Page inputPage = input.get(p);
            assertThat(resultPage.getPositionCount(), equalTo(inputPage.getPositionCount()));
            assertThat(resultPage.getBlockCount(), equalTo(1));

            long[][] expectedValues = new long[inputPage.getPositionCount()][DATE_BUCKETS.size()];
            for (int pos = 0; pos < inputPage.getPositionCount(); pos++) {
                for (int dateBucketIndex = 0; dateBucketIndex < DATE_BUCKETS.size(); dateBucketIndex++) {
                    expectedValues[pos][dateBucketIndex] = EXPECTED_VALUE_LIST.get(dateBucketIndex);
                }
            }
            assertOutputMatchesExpected(resultPage.getBlock(0), expectedValues);
        }
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("SparklineGenerateEmptyBucketsOperator[numValueColumns=1]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo("SparklineGenerateEmptyBucketsOperator[numValueColumns=1]");
    }

    @Override
    protected void assertStatus(Map<String, Object> map, List<Page> input, List<Page> output) {
        assertThat(map, nullValue());
    }

    public void testMaxDateBoundaryIsExclusive() {
        long minDate = 0L;
        long maxDate = 10L * 86_400_000L;
        Rounding.Prepared dailyRounding = Rounding.builder(TimeValue.timeValueDays(1)).build().prepareForUnknown();
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        LongBlock values;
        LongBlock dates;
        try (
            LongBlock.Builder valueBuilder = blockFactory.newLongBlockBuilder(10);
            LongBlock.Builder dateBuilder = blockFactory.newLongBlockBuilder(10)
        ) {
            valueBuilder.beginPositionEntry();
            dateBuilder.beginPositionEntry();
            for (int i = 0; i < 10; i++) {
                valueBuilder.appendLong(i);
                dateBuilder.appendLong((long) i * 86_400_000L);
            }
            valueBuilder.endPositionEntry();
            dateBuilder.endPositionEntry();
            values = valueBuilder.build();
            dates = dateBuilder.build();
        }
        try (
            SparklineGenerateEmptyBucketsOperator op = new SparklineGenerateEmptyBucketsOperator(ctx, 1, dailyRounding, minDate, maxDate)
        ) {
            op.addInput(new Page(values, dates));
            op.finish();
            Page result = op.getOutput();
            assertNotNull(result);
            try {
                LongBlock out = result.getBlock(0);
                assertThat(out.getPositionCount(), equalTo(1));
                // 10 data buckets (days 0–9)
                assertThat(out.getValueCount(0), equalTo(10));
                int start = out.getFirstValueIndex(0);
                for (int i = 0; i < 10; i++) {
                    assertThat(out.getLong(start + i), equalTo((long) i));
                }
            } finally {
                result.releaseBlocks();
            }
        }
    }

    private void assertOutputMatchesExpected(LongBlock outputValues, long[][] expected) {
        assertThat(outputValues.getPositionCount(), equalTo(expected.length));
        for (int pos = 0; pos < expected.length; pos++) {
            assertThat(outputValues.getValueCount(pos), equalTo(DATE_BUCKETS.size()));
            int start = outputValues.getFirstValueIndex(pos);
            for (int i = 0; i < DATE_BUCKETS.size(); i++) {
                assertThat(outputValues.getLong(start + i), equalTo(expected[pos][i]));
            }
        }
    }
}
