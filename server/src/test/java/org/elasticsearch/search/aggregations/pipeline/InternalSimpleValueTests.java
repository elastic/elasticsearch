/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.test.InternalAggregationTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InternalSimpleValueTests extends InternalAggregationTestCase<InternalSimpleValue>{

    @Override
    protected InternalSimpleValue createTestInstance(String name, Map<String, Object> metadata) {
        DocValueFormat formatter = randomNumericDocValueFormat();
        double value = frequently() ? randomDoubleBetween(0, 100000, true)
                : randomFrom(new Double[] { Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NaN });
        return new InternalSimpleValue(name, value, formatter, metadata);
    }

    @Override
    public void testReduceRandom() {
        expectThrows(UnsupportedOperationException.class, () -> createTestInstance("name", null).reduce(null, null));
    }

    @Override
    protected void assertReduced(InternalSimpleValue reduced, List<InternalSimpleValue> inputs) {
        // no test since reduce operation is unsupported
    }

    @Override
    protected void assertFromXContent(InternalSimpleValue simpleValue, ParsedAggregation parsedAggregation) {
        ParsedSimpleValue parsed = ((ParsedSimpleValue) parsedAggregation);
        if (Double.isInfinite(simpleValue.getValue()) == false && Double.isNaN(simpleValue.getValue()) == false) {
            assertEquals(simpleValue.getValue(), parsed.value(), 0);
            assertEquals(simpleValue.getValueAsString(), parsed.getValueAsString());
        } else {
            // we write Double.NEGATIVE_INFINITY, Double.POSITIVE amd Double.NAN to xContent as 'null', so we
            // cannot differentiate between them. Also we cannot recreate the exact String representation
            assertEquals(parsed.value(), Double.NaN, 0);
        }
    }

    @Override
    protected InternalSimpleValue mutateInstance(InternalSimpleValue instance) {
        String name = instance.getName();
        double value = instance.getValue();
        DocValueFormat formatter = instance.formatter();
        Map<String, Object> metadata = instance.getMetadata();
        switch (between(0, 2)) {
        case 0:
            name += randomAlphaOfLength(5);
            break;
        case 1:
            if (Double.isFinite(value)) {
                value += between(1, 100);
            } else {
                value = randomDoubleBetween(0, 100000, true);
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
        return new InternalSimpleValue(name, value, formatter, metadata);
    }

}
