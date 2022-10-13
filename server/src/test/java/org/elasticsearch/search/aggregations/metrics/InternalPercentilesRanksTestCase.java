/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.ParsedAggregation;

import static org.hamcrest.Matchers.equalTo;

public abstract class InternalPercentilesRanksTestCase<T extends InternalAggregation & PercentileRanks> extends AbstractPercentilesTestCase<
    T> {

    @Override
    protected final void assertFromXContent(T aggregation, ParsedAggregation parsedAggregation) {
        assertTrue(parsedAggregation instanceof PercentileRanks);
        PercentileRanks parsedPercentileRanks = (PercentileRanks) parsedAggregation;

        for (Percentile percentile : aggregation) {
            Double value = percentile.getValue();
            assertEquals(aggregation.percent(value), parsedPercentileRanks.percent(value), 0);
            assertEquals(aggregation.percentAsString(value), parsedPercentileRanks.percentAsString(value));
        }

        Class<? extends ParsedPercentiles> parsedClass = implementationClass();
        assertTrue(parsedClass != null && parsedClass.isInstance(parsedAggregation));
    }

    @Override
    protected void assertPercentile(T agg, Double value) {
        assertThat(agg.percent(value), equalTo(Double.NaN));
        assertThat(agg.percentAsString(value), equalTo("NaN"));
    }
}
