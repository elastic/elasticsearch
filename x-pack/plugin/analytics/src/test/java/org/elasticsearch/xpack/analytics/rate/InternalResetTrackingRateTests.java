/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.rate;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.mockito.Mockito.mock;

public class InternalResetTrackingRateTests extends InternalAggregationTestCase<InternalResetTrackingRate> {

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
        testReduction(Rounding.DateTimeUnit.SECOND_OF_MINUTE, 0.01);
    }

    public void testReductionMinute() {
        testReduction(Rounding.DateTimeUnit.MINUTES_OF_HOUR, 0.01 * 60);
    }

    public void testReductionHour() {
        testReduction(Rounding.DateTimeUnit.HOUR_OF_DAY, 0.01 * 60 * 60);
    }

    public void testReductionDay() {
        testReduction(Rounding.DateTimeUnit.DAY_OF_MONTH, 0.01 * 60 * 60 * 24);
    }

    public void testReductionMonth() {
        testReduction(Rounding.DateTimeUnit.MONTH_OF_YEAR, 26297.46);
    }

    public void testReductionQuarter() {
        testReduction(Rounding.DateTimeUnit.QUARTER_OF_YEAR, 26297.46 * 3);
    }

    public void testReductionYear() {
        testReduction(Rounding.DateTimeUnit.YEAR_OF_CENTURY, 26297.46 * 12);
    }

    @Override
    protected void assertReduced(InternalResetTrackingRate reduced, List<InternalResetTrackingRate> inputs) {
        for (InternalResetTrackingRate input : inputs) {
            assertEquals(0.01f, input.getValue(), 0.001);
        }
        assertEquals(0.01f, reduced.getValue(), 0.001);
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

    @Override
    protected void assertFromXContent(InternalResetTrackingRate aggregation, ParsedAggregation parsedAggregation) throws IOException {

    }

    @Override
    protected List<NamedXContentRegistry.Entry> getNamedXContents() {
        return CollectionUtils.appendToCopy(
            super.getNamedXContents(),
            new NamedXContentRegistry.Entry(Aggregation.class, new ParseField(InternalResetTrackingRate.NAME), (p, c) -> {
                assumeTrue("There is no ParsedRate yet", false);
                return null;
            })
        );
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
        InternalAggregation reduced = rates.get(0).reduce(rates, null);
        assertThat(reduced, instanceOf(Rate.class));
        assertThat(((Rate) reduced).getValue(), equalTo(operand));
    }
}
