/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.aggregations.pipeline;

import org.elasticsearch.aggregations.AggregationsPlugin;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.test.InternalAggregationTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DerivativeResultTests extends InternalAggregationTestCase<Derivative> {
    @Override
    protected SearchPlugin registerPlugin() {
        return new AggregationsPlugin();
    }

    @Override
    protected Derivative createTestInstance(String name, Map<String, Object> metadata) {
        DocValueFormat formatter = randomNumericDocValueFormat();
        double value = frequently()
            ? randomDoubleBetween(-100000, 100000, true)
            : randomFrom(new Double[] { Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NaN });
        double normalizationFactor = frequently() ? randomDoubleBetween(0, 100000, true) : 0;
        return new Derivative(name, value, normalizationFactor, formatter, metadata);
    }

    @Override
    public void testReduceRandom() {
        expectThrows(UnsupportedOperationException.class, () -> createTestInstance("name", null).getReducer(null, 0));
    }

    @Override
    protected void assertReduced(Derivative reduced, List<Derivative> inputs) {
        // no test since reduce operation is unsupported
    }

    @Override
    protected Derivative mutateInstance(Derivative instance) {
        String name = instance.getName();
        double value = instance.getValue();
        double normalizationFactor = instance.getNormalizationFactor();
        DocValueFormat formatter = instance.formatter();
        Map<String, Object> metadata = instance.getMetadata();
        switch (between(0, 3)) {
            case 0 -> name += randomAlphaOfLength(5);
            case 1 -> {
                if (Double.isFinite(value)) {
                    value += between(1, 100);
                } else {
                    value = randomDoubleBetween(0, 100000, true);
                }
            }
            case 2 -> normalizationFactor += between(1, 100);
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
        return new Derivative(name, value, normalizationFactor, formatter, metadata);
    }
}
