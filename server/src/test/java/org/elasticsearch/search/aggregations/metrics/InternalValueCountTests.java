/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.test.InternalAggregationTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class InternalValueCountTests extends InternalAggregationTestCase<InternalValueCount> {

    @Override
    protected InternalValueCount createTestInstance(String name, Map<String, Object> metadata) {
        return new InternalValueCount(name, randomIntBetween(0, 100), metadata);
    }

    @Override
    protected void assertReduced(InternalValueCount reduced, List<InternalValueCount> inputs) {
        assertEquals(inputs.stream().mapToLong(InternalValueCount::getValue).sum(), reduced.getValue(), 0);
    }

    @Override
    protected boolean supportsSampling() {
        return true;
    }

    @Override
    protected void assertSampled(InternalValueCount sampled, InternalValueCount reduced, SamplingContext samplingContext) {
        assertThat(sampled.getValue(), equalTo(samplingContext.scaleUp(reduced.getValue())));
    }

    @Override
    protected void assertFromXContent(InternalValueCount valueCount, ParsedAggregation parsedAggregation) {
        assertEquals(valueCount.getValue(), ((ParsedValueCount) parsedAggregation).getValue(), 0);
        assertEquals(valueCount.getValueAsString(), ((ParsedValueCount) parsedAggregation).getValueAsString());
    }

    @Override
    protected InternalValueCount mutateInstance(InternalValueCount instance) {
        String name = instance.getName();
        long value = instance.getValue();
        Map<String, Object> metadata = instance.getMetadata();
        switch (between(0, 2)) {
            case 0:
                name += randomAlphaOfLength(5);
                break;
            case 1:
                if (Double.isFinite(value)) {
                    value += between(1, 100);
                } else {
                    value = between(1, 100);
                }
                break;
            case 2:
                if (metadata == null) {
                    metadata = Maps.newMapWithExpectedSize(1);
                } else {
                    metadata = new HashMap<>(instance.getMetadata());
                }
                metadata.put(randomAlphaOfLength(15), randomInt());
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }
        return new InternalValueCount(name, value, metadata);
    }
}
