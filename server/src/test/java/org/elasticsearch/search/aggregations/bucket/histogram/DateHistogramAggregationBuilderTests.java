/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;

public class DateHistogramAggregationBuilderTests extends ESTestCase {
    public void testParseStringOffset_Zero() {
        assertThat(DateHistogramAggregationBuilder.parseStringOffset("0"), equalTo(0L));
        assertThat(DateHistogramAggregationBuilder.parseStringOffset("0ms"), equalTo(0L));
        assertThat(DateHistogramAggregationBuilder.parseStringOffset("0s"), equalTo(0L));
        assertThat(DateHistogramAggregationBuilder.parseStringOffset("0m"), equalTo(0L));
        assertThat(DateHistogramAggregationBuilder.parseStringOffset("0h"), equalTo(0L));
        assertThat(DateHistogramAggregationBuilder.parseStringOffset("0d"), equalTo(0L));
        expectThrows(IllegalArgumentException.class, () -> { DateHistogramAggregationBuilder.parseStringOffset("0w"); });
        expectThrows(IllegalArgumentException.class, () -> { DateHistogramAggregationBuilder.parseStringOffset("0M"); });
        expectThrows(IllegalArgumentException.class, () -> { DateHistogramAggregationBuilder.parseStringOffset("0Y"); });
    }

    public void testParseStringOffset_Positive() {
        expectThrows(IllegalArgumentException.class, () -> { DateHistogramAggregationBuilder.parseStringOffset("5"); });
        assertThat(DateHistogramAggregationBuilder.parseStringOffset("5ms"), equalTo(5L));
        assertThat(DateHistogramAggregationBuilder.parseStringOffset("5s"), equalTo(TimeUnit.SECONDS.toMillis(5)));
        assertThat(DateHistogramAggregationBuilder.parseStringOffset("5m"), equalTo(TimeUnit.MINUTES.toMillis(5)));
        assertThat(DateHistogramAggregationBuilder.parseStringOffset("5h"), equalTo(TimeUnit.HOURS.toMillis(5)));
        assertThat(DateHistogramAggregationBuilder.parseStringOffset("5d"), equalTo(TimeUnit.DAYS.toMillis(5)));
        expectThrows(IllegalArgumentException.class, () -> { DateHistogramAggregationBuilder.parseStringOffset("5w"); });
        expectThrows(IllegalArgumentException.class, () -> { DateHistogramAggregationBuilder.parseStringOffset("5M"); });
        expectThrows(IllegalArgumentException.class, () -> { DateHistogramAggregationBuilder.parseStringOffset("5Y"); });
    }

    public void testParseStringOffset_Negative() {
        expectThrows(IllegalArgumentException.class, () -> { DateHistogramAggregationBuilder.parseStringOffset("-5"); });
        assertThat(DateHistogramAggregationBuilder.parseStringOffset("-5ms"), equalTo(-5L));
        assertThat(DateHistogramAggregationBuilder.parseStringOffset("-5s"), equalTo(-TimeUnit.SECONDS.toMillis(5)));
        assertThat(DateHistogramAggregationBuilder.parseStringOffset("-5m"), equalTo(-TimeUnit.MINUTES.toMillis(5)));
        assertThat(DateHistogramAggregationBuilder.parseStringOffset("-5h"), equalTo(-TimeUnit.HOURS.toMillis(5)));
        assertThat(DateHistogramAggregationBuilder.parseStringOffset("-5d"), equalTo(-TimeUnit.DAYS.toMillis(5)));
        expectThrows(IllegalArgumentException.class, () -> { DateHistogramAggregationBuilder.parseStringOffset("-5w"); });
        expectThrows(IllegalArgumentException.class, () -> { DateHistogramAggregationBuilder.parseStringOffset("-5M"); });
        expectThrows(IllegalArgumentException.class, () -> { DateHistogramAggregationBuilder.parseStringOffset("-5Y"); });
    }

    public void testParseStringCompoundOffset() {
        for (String offset : new String[] {
            "0",
            "1ms",
            "1s",
            "+1s",
            "-1s",
            "1ms+1s",
            "+1ms +1s",
            "+1s+1s",
            "-1s+1s",
            "+1s-1s",
            "-1s-1s",
            "1s 1s",
            "1d 1d",
            "1w 1d",
            "1M 1d",
            "1Y 1d",
            "-1s 1s",
            "-1d 1d",
            "-1w 1d",
            "-1M 1d",
            "-1Y 1d" }) {
            System.out.println("Testing offset: " + offset);
            DateHistogramAggregationBuilder.Offset compoundOffset = DateHistogramAggregationBuilder.parseStringCompoundOffset(offset);
            System.out.println("Converted offset[" + offset + "] into '" + compoundOffset + "'");
            assertThat(
                "Testing re-parsing of '" + compoundOffset + "'",
                DateHistogramAggregationBuilder.parseStringCompoundOffset(compoundOffset.toString()),
                equalTo(compoundOffset)
            );
        }
    }

    public void testDateHistogramAggregationBuilder_Offset() {
    }
}
