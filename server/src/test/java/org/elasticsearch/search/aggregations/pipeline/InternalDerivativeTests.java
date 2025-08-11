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

public class InternalDerivativeTests extends InternalAggregationTestCase<InternalDerivative> {

    @Override
    protected InternalDerivative createTestInstance(String name, Map<String, Object> metadata) {
        DocValueFormat formatter = randomNumericDocValueFormat();
        double value = frequently()
            ? randomDoubleBetween(-100000, 100000, true)
            : randomFrom(new Double[] { Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NaN });
        double normalizationFactor = frequently() ? randomDoubleBetween(0, 100000, true) : 0;
        return new InternalDerivative(name, value, normalizationFactor, formatter, metadata);
    }

    @Override
    public void testReduceRandom() {
        expectThrows(UnsupportedOperationException.class, () -> createTestInstance("name", null).reduce(null, null));
    }

    @Override
    protected void assertReduced(InternalDerivative reduced, List<InternalDerivative> inputs) {
        // no test since reduce operation is unsupported
    }

    @Override
    protected void assertFromXContent(InternalDerivative derivative, ParsedAggregation parsedAggregation) {
        ParsedDerivative parsed = ((ParsedDerivative) parsedAggregation);
        if (Double.isInfinite(derivative.getValue()) == false && Double.isNaN(derivative.getValue()) == false) {
            assertEquals(derivative.getValue(), parsed.value(), Double.MIN_VALUE);
            assertEquals(derivative.getValueAsString(), parsed.getValueAsString());
        } else {
            // we write Double.NEGATIVE_INFINITY, Double.POSITIVE amd Double.NAN to xContent as 'null', so we
            // cannot differentiate between them. Also we cannot recreate the exact String representation
            assertEquals(parsed.value(), Double.NaN, Double.MIN_VALUE);
        }
    }

    @Override
    protected InternalDerivative mutateInstance(InternalDerivative instance) {
        String name = instance.getName();
        double value = instance.getValue();
        double normalizationFactor = instance.getNormalizationFactor();
        DocValueFormat formatter = instance.formatter();
        Map<String, Object> metadata = instance.getMetadata();
        switch (between(0, 3)) {
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
                normalizationFactor += between(1, 100);
                break;
            case 3:
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
        return new InternalDerivative(name, value, normalizationFactor, formatter, metadata);
    }
}
