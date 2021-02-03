/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

public class AutoDateHistogramAggregationBuilderTests extends ESTestCase {

    public void testInvalidInterval() {
        AutoDateHistogramAggregationBuilder builder = new AutoDateHistogramAggregationBuilder("name");
        IllegalArgumentException wrongIntervalName = expectThrows(IllegalArgumentException.class,
            () -> builder.setMinimumIntervalExpression("foobar"));
        assertTrue(wrongIntervalName.getMessage().startsWith("minimum_interval must be one of"));
    }

    public void testBuildRoundingsWithNullParameter() {
        int expectedLength = AutoDateHistogramAggregationBuilder.ALLOWED_INTERVALS.size();
        AutoDateHistogramAggregationBuilder.RoundingInfo[] roundings = AutoDateHistogramAggregationBuilder.buildRoundings(null, null);
        assertThat(roundings.length, equalTo(expectedLength));
    }

    public void testBuildRoundingsWithMinIntervalOfAYear() {
        int[] expectedYearIntervals = { 1, 5, 10, 20, 50, 100 };
        AutoDateHistogramAggregationBuilder.RoundingInfo[] roundings = AutoDateHistogramAggregationBuilder.buildRoundings(null, "year");
        assertThat(roundings.length, equalTo(1));
        AutoDateHistogramAggregationBuilder.RoundingInfo year = roundings[0];
        assertEquals(year.unitAbbreviation, "y");
        assertEquals(year.dateTimeUnit, "year");
        assertEquals(year.roughEstimateDurationMillis, 31536000000L);
        assertArrayEquals(year.innerIntervals, expectedYearIntervals);
    }

    public void testRoundingsMatchAllowedIntervals() {
        AutoDateHistogramAggregationBuilder.RoundingInfo[] roundings = AutoDateHistogramAggregationBuilder.buildRoundings(
            null, "second");
        Set<String> actualDateTimeUnits = Arrays.stream(roundings)
            .map(AutoDateHistogramAggregationBuilder.RoundingInfo::getDateTimeUnit)
            .collect(Collectors.toSet());
        Set<String> expectedDateTimeUnits = new HashSet<>(AutoDateHistogramAggregationBuilder.ALLOWED_INTERVALS.values());
        assertEquals(actualDateTimeUnits, expectedDateTimeUnits);
    }

}
