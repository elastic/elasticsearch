/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.rate;

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
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;

public class InternalResetTrackingRateTests extends InternalAggregationTestCase<InternalResetTrackingRate> {

    @Override
    protected SearchPlugin registerPlugin() {
        return new AnalyticsPlugin();
    }

    @Override
    protected InternalResetTrackingRate createTestInstance(String name, Map<String, Object> metadata) {
        return new InternalResetTrackingRate(name, null, metadata, 0, 0, 0, 0, 0);
    }

    private static InternalResetTrackingRate rate(double startValue, double endValue, long startTime, long endTime, double resetComp) {
        return new InternalResetTrackingRate("n", null, null, startValue, endValue, startTime, endTime, resetComp);
    }

    public void testReduction() {
        List<InternalAggregation> rates = List.of(
            rate(0, 10, 1000, 2000, 0),
            rate(10, 20, 2000, 3000, 0),
            rate(20, 5, 3000, 4000, 25), // internal reset
            rate(5, 15, 4000, 5000, 0),
            rate(0, 10, 5000, 6000, 0)  // cross-boundary reset
        );
        InternalAggregation reduced = rates.get(0).reduce(rates, null);
        assertThat(reduced, instanceOf(Rate.class));
        assertThat(((Rate) reduced).getValue(), equalTo(0.01));
    }

    @Override
    protected void assertReduced(InternalResetTrackingRate reduced, List<InternalResetTrackingRate> inputs) {

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
}
