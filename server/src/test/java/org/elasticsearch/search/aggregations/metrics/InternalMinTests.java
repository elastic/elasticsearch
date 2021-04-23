/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.test.InternalAggregationTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InternalMinTests extends InternalAggregationTestCase<InternalMin> {
    @Override
    protected InternalMin createTestInstance(String name, Map<String, Object> metadata) {
        double value = frequently() ? randomDouble() : randomFrom(new Double[] { Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY });
        DocValueFormat formatter = randomNumericDocValueFormat();
        return new InternalMin(name, value, formatter, metadata);
    }

    @Override
    protected void assertReduced(InternalMin reduced, List<InternalMin> inputs) {
        assertEquals(inputs.stream().mapToDouble(InternalMin::value).min().getAsDouble(), reduced.value(), 0);
    }

    @Override
    protected void assertFromXContent(InternalMin min, ParsedAggregation parsedAggregation) {
        ParsedMin parsed = ((ParsedMin) parsedAggregation);
        if (Double.isInfinite(min.getValue()) == false) {
            assertEquals(min.getValue(), parsed.getValue(), Double.MIN_VALUE);
            assertEquals(min.getValueAsString(), parsed.getValueAsString());
        } else {
            // we write Double.NEGATIVE_INFINITY and Double.POSITIVE_INFINITY to xContent as 'null', so we
            // cannot differentiate between them. Also we cannot recreate the exact String representation
            assertEquals(parsed.getValue(), Double.POSITIVE_INFINITY, 0);
        }
    }

    @Override
    protected InternalMin mutateInstance(InternalMin instance) {
        String name = instance.getName();
        double value = instance.getValue();
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
                metadata = new HashMap<>(1);
            } else {
                metadata = new HashMap<>(instance.getMetadata());
            }
            metadata.put(randomAlphaOfLength(15), randomInt());
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }
        return new InternalMin(name, value, formatter, metadata);
    }
}
