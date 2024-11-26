/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.test.InternalAggregationTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class InternalMedianAbsoluteDeviationTests extends InternalAggregationTestCase<InternalMedianAbsoluteDeviation> {

    @Override
    protected InternalMedianAbsoluteDeviation createTestInstance(String name, Map<String, Object> metadata) {
        final TDigestState valuesSketch = TDigestState.createWithoutCircuitBreaking(randomFrom(50.0, 100.0, 200.0, 500.0, 1000.0));
        final int numberOfValues = frequently() ? randomIntBetween(0, 1000) : 0;
        for (int i = 0; i < numberOfValues; i++) {
            valuesSketch.add(randomDouble());
        }

        return new InternalMedianAbsoluteDeviation(name, metadata, randomNumericDocValueFormat(), valuesSketch);
    }

    @Override
    protected void assertReduced(InternalMedianAbsoluteDeviation reduced, List<InternalMedianAbsoluteDeviation> inputs) {
        final TDigestState expectedValuesSketch = TDigestState.createUsingParamsFrom(reduced.getValuesSketch());

        long totalCount = 0;
        for (InternalMedianAbsoluteDeviation input : inputs) {
            expectedValuesSketch.add(input.getValuesSketch());
            totalCount += input.getValuesSketch().size();
        }

        assertEquals(totalCount, reduced.getValuesSketch().size());
        if (totalCount > 0) {
            assertEquals(expectedValuesSketch.quantile(0), reduced.getValuesSketch().quantile(0), 0d);
            assertEquals(expectedValuesSketch.quantile(1), reduced.getValuesSketch().quantile(1), 0d);
        }
    }

    @Override
    protected boolean supportsSampling() {
        return true;
    }

    @Override
    protected void assertSampled(
        InternalMedianAbsoluteDeviation sampled,
        InternalMedianAbsoluteDeviation reduced,
        SamplingContext samplingContext
    ) {
        assertThat(sampled.getMedianAbsoluteDeviation(), equalTo(reduced.getMedianAbsoluteDeviation()));
    }

    @Override
    protected InternalMedianAbsoluteDeviation mutateInstance(InternalMedianAbsoluteDeviation instance) {
        String name = instance.getName();
        TDigestState valuesSketch = instance.getValuesSketch();
        Map<String, Object> metadata = instance.getMetadata();

        switch (between(0, 2)) {
            case 0 -> name += randomAlphaOfLengthBetween(2, 10);
            case 1 -> {
                final TDigestState newValuesSketch = TDigestState.createUsingParamsFrom(instance.getValuesSketch());
                final int numberOfValues = between(10, 100);
                for (int i = 0; i < numberOfValues; i++) {
                    newValuesSketch.add(randomDouble());
                }
                valuesSketch = newValuesSketch;
            }
            case 2 -> {
                if (metadata == null) {
                    metadata = Maps.newMapWithExpectedSize(1);
                } else {
                    metadata = new HashMap<>(metadata);
                }
                metadata.put(randomAlphaOfLengthBetween(2, 10), randomInt());
            }
        }

        return new InternalMedianAbsoluteDeviation(name, metadata, instance.format, valuesSketch);
    }
}
