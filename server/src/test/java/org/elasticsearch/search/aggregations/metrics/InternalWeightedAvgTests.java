/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.test.InternalAggregationTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class InternalWeightedAvgTests extends InternalAggregationTestCase<InternalWeightedAvg> {

    @Override
    protected InternalWeightedAvg createTestInstance(String name, Map<String, Object> metadata) {
        DocValueFormat formatter = randomNumericDocValueFormat();
        return new InternalWeightedAvg(
            name,
            randomDoubleBetween(0, 100000, true),
            randomDoubleBetween(0, 100000, true),
            formatter,
            metadata
        );
    }

    @Override
    protected void assertReduced(InternalWeightedAvg reduced, List<InternalWeightedAvg> inputs) {
        double sum = 0;
        double weight = 0;
        for (InternalWeightedAvg in : inputs) {
            sum += in.getSum();
            weight += in.getWeight();
        }
        assertEquals(sum, reduced.getSum(), 0.0000001);
        assertEquals(weight, reduced.getWeight(), 0.0000001);
        assertEquals(sum / weight, reduced.getValue(), 0.0000001);
    }

    @Override
    protected boolean supportsSampling() {
        return true;
    }

    @Override
    protected void assertSampled(InternalWeightedAvg sampled, InternalWeightedAvg reduced, SamplingContext samplingContext) {
        assertThat(sampled.getValue(), equalTo(reduced.getValue()));
    }

    @Override
    protected void assertFromXContent(InternalWeightedAvg avg, ParsedAggregation parsedAggregation) {
        ParsedWeightedAvg parsed = ((ParsedWeightedAvg) parsedAggregation);
        assertEquals(avg.getValue(), parsed.getValue(), Double.MIN_VALUE);
        // we don't print out VALUE_AS_STRING for avg.getCount() == 0, so we cannot get the exact same value back
        if (avg.getWeight() != 0) {
            assertEquals(avg.getValueAsString(), parsed.getValueAsString());
        }
    }

    @Override
    protected InternalWeightedAvg mutateInstance(InternalWeightedAvg instance) {
        String name = instance.getName();
        double sum = instance.getSum();
        double weight = instance.getWeight();
        DocValueFormat formatter = instance.getFormatter();
        Map<String, Object> metadata = instance.getMetadata();
        switch (between(0, 3)) {
            case 0 -> name += randomAlphaOfLength(5);
            case 1 -> {
                if (Double.isFinite(sum)) {
                    sum += between(1, 100);
                } else {
                    sum = between(1, 100);
                }
            }
            case 2 -> {
                if (Double.isFinite(weight)) {
                    weight += between(1, 100);
                } else {
                    weight = between(1, 100);
                }
            }
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
        return new InternalWeightedAvg(name, sum, weight, formatter, metadata);
    }
}
