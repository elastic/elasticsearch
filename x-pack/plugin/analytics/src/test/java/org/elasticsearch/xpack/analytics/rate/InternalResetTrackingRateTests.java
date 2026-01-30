/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.rate;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.mockito.Mockito.mock;

public class InternalResetTrackingRateTests extends InternalAggregationTestCase<InternalResetTrackingRate> {

    private static final int MILLIS_IN_SECOND = 1_000;
    private static final int SECONDS_IN_MINUTE = 60;
    private static final int MILLIS_IN_MINUTE = MILLIS_IN_SECOND * SECONDS_IN_MINUTE;
    private static final int MINUTES_IN_HOUR = 60;
    private static final int MILLIS_IN_HOUR = MILLIS_IN_SECOND * SECONDS_IN_MINUTE * MINUTES_IN_HOUR;
    private static final int HOURS_IN_DAY = 24;
    private static final int MILLIS_IN_DAY = MILLIS_IN_SECOND * SECONDS_IN_MINUTE * MINUTES_IN_HOUR * HOURS_IN_DAY;
    private static final int DAYS_IN_WEEK = 7;
    private static final int MILLIS_IN_WEEK = MILLIS_IN_SECOND * SECONDS_IN_MINUTE * MINUTES_IN_HOUR * HOURS_IN_DAY * DAYS_IN_WEEK;
    private static final int MONTHS_IN_QUARTER = 3;
    private static final int MONTHS_IN_YEAR = 12;

    @Override
    protected SearchPlugin registerPlugin() {
        return new AnalyticsPlugin();
    }

    @Override
    protected InternalResetTrackingRate createTestInstance(String name, Map<String, Object> metadata) {
        return new InternalResetTrackingRate(name, null, metadata, 0, 0, 0, 0, 0, Rounding.DateTimeUnit.SECOND_OF_MINUTE);
    }

    private static InternalResetTrackingRate rate(
        double startValue,
        double endValue,
        long startTime,
        long endTime,
        double resetComp,
        Rounding.DateTimeUnit rateUnit
    ) {
        return new InternalResetTrackingRate("n", null, null, startValue, endValue, startTime, endTime, resetComp, rateUnit);
    }

    public void testReductionSecond() {
        testReduction(Rounding.DateTimeUnit.SECOND_OF_MINUTE, 0.01 * MILLIS_IN_SECOND);
    }

    public void testReductionMinute() {
        testReduction(Rounding.DateTimeUnit.MINUTE_OF_HOUR, 0.01 * MILLIS_IN_MINUTE);
    }

    public void testReductionHour() {
        testReduction(Rounding.DateTimeUnit.HOUR_OF_DAY, 0.01 * MILLIS_IN_HOUR);
    }

    public void testReductionDay() {
        testReduction(Rounding.DateTimeUnit.DAY_OF_MONTH, 0.01 * MILLIS_IN_DAY);
    }

    public void testReductionWeek() {
        testReduction(Rounding.DateTimeUnit.WEEK_OF_WEEKYEAR, 0.01 * MILLIS_IN_WEEK);
    }

    public void testReductionMonth() {
        testReduction(Rounding.DateTimeUnit.MONTH_OF_YEAR, 26297.46 * MILLIS_IN_SECOND);
    }

    public void testReductionQuarter() {
        testReduction(Rounding.DateTimeUnit.QUARTER_OF_YEAR, 26297.46 * MILLIS_IN_SECOND * MONTHS_IN_QUARTER);
    }

    public void testReductionYear() {
        testReduction(Rounding.DateTimeUnit.YEAR_OF_CENTURY, 26297.46 * MILLIS_IN_SECOND * MONTHS_IN_YEAR);
    }

    @Override
    protected void assertReduced(InternalResetTrackingRate reduced, List<InternalResetTrackingRate> inputs) {
        for (InternalResetTrackingRate input : inputs) {
            assertEquals(0.01f * MILLIS_IN_SECOND, input.getValue(), 0.01);
        }
        assertEquals(0.01f * MILLIS_IN_SECOND, reduced.getValue(), 0.01);
    }

    // Buckets must always be in-order so that we can detect resets between consecutive buckets
    @Override
    protected boolean supportsOutOfOrderReduce() {
        return false;
    }

    @Override
    protected BuilderAndToReduce<InternalResetTrackingRate> randomResultsToReduce(String name, int size) {
        // generate a monotonically increasing counter, starting at 0 finishing at 1000 and increasing
        // by 10 each time
        // randomly reset to 0
        // randomly break to a new rate
        List<InternalResetTrackingRate> internalRates = new ArrayList<>();
        double startValue = 0, currentValue = 0;
        double resetComp = 0;
        long startTime = 0;
        long endTime = 0;
        while (internalRates.size() < size - 1) {
            endTime += 1000;
            currentValue += 10;
            if (randomInt(30) == 0) {
                resetComp += currentValue;
                currentValue = 0;
            }
            if (randomInt(45) == 0) {
                internalRates.add(rate(startValue, currentValue, startTime, endTime, resetComp, Rounding.DateTimeUnit.SECOND_OF_MINUTE));
                startValue = currentValue;
                resetComp = 0;
                startTime = endTime;
            }
        }
        if (startTime == endTime) {
            endTime += 1000;
            currentValue += 10;
        }
        internalRates.add(rate(startValue, currentValue, startTime, endTime, resetComp, Rounding.DateTimeUnit.SECOND_OF_MINUTE));
        return new BuilderAndToReduce<>(mock(RateAggregationBuilder.class), internalRates);
    }

    public void testIncludes() {
        InternalResetTrackingRate big = new InternalResetTrackingRate(
            "n",
            null,
            null,
            0,
            0,
            1000,
            3000,
            0,
            Rounding.DateTimeUnit.SECOND_OF_MINUTE
        );
        InternalResetTrackingRate small = new InternalResetTrackingRate(
            "n",
            null,
            null,
            0,
            0,
            1500,
            2500,
            0,
            Rounding.DateTimeUnit.SECOND_OF_MINUTE
        );
        assertTrue(big.includes(small));
        assertFalse(small.includes(big));

        InternalResetTrackingRate unrelated = new InternalResetTrackingRate(
            "n",
            null,
            null,
            0,
            0,
            100000,
            1000010,
            0,
            Rounding.DateTimeUnit.SECOND_OF_MINUTE
        );
        assertFalse(big.includes(unrelated));
        assertFalse(unrelated.includes(big));
        assertFalse(small.includes(unrelated));
        assertFalse(unrelated.includes(small));
    }

    @Override
    protected InternalResetTrackingRate mutateInstance(InternalResetTrackingRate instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    private static void testReduction(final Rounding.DateTimeUnit dateTimeUnit, double operand) {
        List<InternalAggregation> rates = List.of(
            rate(0, 10, 1000, 2000, 0, dateTimeUnit),
            rate(10, 20, 2000, 3000, 0, dateTimeUnit),
            rate(20, 5, 3000, 4000, 25, dateTimeUnit), // internal reset
            rate(5, 15, 4000, 5000, 0, dateTimeUnit),
            rate(0, 10, 5000, 6000, 0, dateTimeUnit)  // cross-boundary reset
        );
        InternalAggregation reduced = InternalAggregationTestCase.reduce(rates, null);
        assertThat(reduced, instanceOf(Rate.class));
        assertThat(((Rate) reduced).getValue(), equalTo(operand));
    }
}
