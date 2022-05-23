/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.rate;

import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.closeTo;
import static org.mockito.Mockito.mock;

public class InternalRateTests extends InternalAggregationTestCase<InternalRate> {

    @Override
    protected SearchPlugin registerPlugin() {
        return new AnalyticsPlugin();
    }

    @Override
    protected InternalRate createTestInstance(String name, Map<String, Object> metadata) {
        double sum = randomDouble();
        double divider = randomDoubleBetween(0.0, 100000.0, false);
        DocValueFormat formatter = randomNumericDocValueFormat();
        return new InternalRate(name, sum, divider, formatter, metadata);
    }

    @Override
    protected BuilderAndToReduce<InternalRate> randomResultsToReduce(String name, int size) {
        double divider = randomDoubleBetween(0.0, 100000.0, false);
        List<InternalRate> inputs = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            // Make sure the sum of all the counts doesn't wrap and type and tail parameters are consistent
            DocValueFormat formatter = randomNumericDocValueFormat();
            inputs.add(new InternalRate(name, randomDouble(), divider, formatter, null));
        }
        return new BuilderAndToReduce<>(mock(AggregationBuilder.class), inputs);
    }

    @Override
    protected boolean supportsSampling() {
        return true;
    }

    @Override
    protected void assertSampled(InternalRate sampled, InternalRate reduced, SamplingContext samplingContext) {
        assertThat(sampled.getValue(), closeTo(samplingContext.scaleUp(reduced.getValue()), 1e-10));
    }

    @Override
    protected void assertReduced(InternalRate reduced, List<InternalRate> inputs) {
        double expected = inputs.stream().mapToDouble(a -> a.sum).sum() / reduced.divisor;
        assertEquals(expected, reduced.getValue(), 0.00001);
    }

    @Override
    protected void assertFromXContent(InternalRate min, ParsedAggregation parsedAggregation) {
        // There is no ParsedRate yet so we cannot test it here
    }

    @Override
    protected InternalRate mutateInstance(InternalRate instance) {
        String name = instance.getName();
        double sum = instance.sum;
        double divider = instance.divisor;
        DocValueFormat formatter = instance.format();
        Map<String, Object> metadata = instance.getMetadata();
        switch (between(0, 3)) {
            case 0 -> name += randomAlphaOfLength(5);
            case 1 -> sum = randomDouble();
            case 2 -> divider = randomDouble();
            case 3 -> {
                if (metadata == null) {
                    metadata = Maps.newMapWithExpectedSize(1);
                } else {
                    metadata = new HashMap<>(instance.getMetadata());
                }
                metadata.put(randomAlphaOfLength(15), randomInt());
            }
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new InternalRate(name, sum, divider, formatter, metadata);
    }

    @Override
    protected List<NamedXContentRegistry.Entry> getNamedXContents() {
        return CollectionUtils.appendToCopy(
            super.getNamedXContents(),
            new NamedXContentRegistry.Entry(Aggregation.class, new ParseField(RateAggregationBuilder.NAME), (p, c) -> {
                assumeTrue("There is no ParsedRate yet", false);
                return null;
            })
        );
    }
}
