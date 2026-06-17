/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.aggregations.bucket.timeseries;

import org.elasticsearch.aggregations.bucket.AggregationBuilderTestCase;

public class TimeSeriesAggregationBuilderTests extends AggregationBuilderTestCase<TimeSeriesAggregationBuilder> {

    @Override
    protected TimeSeriesAggregationBuilder createTestAggregatorBuilder() {
        // Size set large enough tests not intending to hit the size limit shouldn't see it.
        return new TimeSeriesAggregationBuilder(randomAlphaOfLength(10), randomBoolean(), randomIntBetween(1000, 100_000));
    }

    /**
     * {@code size} is serialized via {@link TimeSeriesAggregationBuilder#doWriteTo} and round-tripped through XContent, so two
     * builders that differ only in {@code size} represent different requests and must not be {@link Object#equals equal}. The
     * generic {@code BaseAggregationTestCase#testEqualsAndHashcode} only mutates name/boost, so it does not cover {@code size}.
     */
    public void testEqualsAndHashCodeConsiderSize() {
        String name = randomAlphaOfLength(10);
        boolean keyed = randomBoolean();
        TimeSeriesAggregationBuilder small = new TimeSeriesAggregationBuilder(name, keyed, 100);
        TimeSeriesAggregationBuilder large = new TimeSeriesAggregationBuilder(name, keyed, 200);
        assertNotEquals(small, large);
        assertNotEquals(small.hashCode(), large.hashCode());

        // Builders that agree on every field, including size, are equal and hash consistently.
        TimeSeriesAggregationBuilder smallCopy = new TimeSeriesAggregationBuilder(name, keyed, 100);
        assertEquals(small, smallCopy);
        assertEquals(small.hashCode(), smallCopy.hashCode());
    }

}
