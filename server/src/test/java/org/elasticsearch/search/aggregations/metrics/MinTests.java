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

public class MinTests extends InternalAggregationTestCase<Min> {
    @Override
    protected Min createTestInstance(String name, Map<String, Object> metadata) {
        double value = frequently() ? randomDouble() : randomFrom(new Double[] { Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY });
        DocValueFormat formatter = randomNumericDocValueFormat();
        return new Min(name, value, formatter, metadata);
    }

    @Override
    protected void assertReduced(Min reduced, List<Min> inputs) {
        assertEquals(inputs.stream().mapToDouble(Min::value).min().getAsDouble(), reduced.value(), 0);
    }

    @Override
    protected boolean supportsSampling() {
        return true;
    }

    @Override
    protected void assertSampled(Min sampled, Min reduced, SamplingContext samplingContext) {
        assertThat(sampled.value(), equalTo(reduced.value()));
    }

    @Override
    protected void assertFromXContent(Min min, ParsedAggregation parsedAggregation) {
        ParsedMin parsed = ((ParsedMin) parsedAggregation);
        if (Double.isInfinite(min.value()) == false) {
            assertEquals(min.value(), parsed.value(), Double.MIN_VALUE);
            assertEquals(min.getValueAsString(), parsed.getValueAsString());
        } else {
            // we write Double.NEGATIVE_INFINITY and Double.POSITIVE_INFINITY to xContent as 'null', so we
            // cannot differentiate between them. Also we cannot recreate the exact String representation
            assertEquals(parsed.value(), Double.POSITIVE_INFINITY, 0);
        }
    }

    @Override
    protected Min mutateInstance(Min instance) {
        String name = instance.getName();
        double value = instance.value();
        DocValueFormat formatter = instance.format;
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
        return new Min(name, value, formatter, metadata);
    }
}
