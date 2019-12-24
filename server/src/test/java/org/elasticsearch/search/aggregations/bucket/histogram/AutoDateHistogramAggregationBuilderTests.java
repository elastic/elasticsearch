/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
