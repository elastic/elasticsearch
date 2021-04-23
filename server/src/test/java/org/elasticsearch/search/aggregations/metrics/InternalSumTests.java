/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.test.InternalAggregationTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InternalSumTests extends InternalAggregationTestCase<InternalSum> {

    @Override
    protected InternalSum createTestInstance(String name, Map<String, Object> metadata) {
        double value = frequently() ? randomDouble() : randomFrom(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NaN);
        DocValueFormat formatter = randomFrom(new DocValueFormat.Decimal("###.##"), DocValueFormat.RAW);
        return new InternalSum(name, value, formatter, metadata);
    }

    @Override
    protected void assertReduced(InternalSum reduced, List<InternalSum> inputs) {
        double expectedSum = inputs.stream().mapToDouble(InternalSum::getValue).sum();
        assertEquals(expectedSum, reduced.getValue(), 0.0001d);
    }

    public void testSummationAccuracy() {
        // Summing up a normal array and expect an accurate value
        double[] values = new double[]{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.9, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7};
        verifySummationOfDoubles(values, 13.5, 0d);

        // Summing up an array which contains NaN and infinities and expect a result same as naive summation
        int n = randomIntBetween(5, 10);
        values = new double[n];
        double sum = 0;
        for (int i = 0; i < n; i++) {
            values[i] = frequently()
                ? randomFrom(Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY)
                : randomDoubleBetween(Double.MIN_VALUE, Double.MAX_VALUE, true);
            sum += values[i];
        }
        verifySummationOfDoubles(values, sum, TOLERANCE);

        // Summing up some big double values and expect infinity result
        n = randomIntBetween(5, 10);
        double[] largeValues = new double[n];
        for (int i = 0; i < n; i++) {
            largeValues[i] = Double.MAX_VALUE;
        }
        verifySummationOfDoubles(largeValues, Double.POSITIVE_INFINITY, 0d);

        for (int i = 0; i < n; i++) {
            largeValues[i] = -Double.MAX_VALUE;
        }
        verifySummationOfDoubles(largeValues, Double.NEGATIVE_INFINITY, 0d);
    }

    private void verifySummationOfDoubles(double[] values, double expected, double delta) {
        List<InternalAggregation> aggregations = new ArrayList<>(values.length);
        for (double value : values) {
            aggregations.add(new InternalSum("dummy1", value, null, null));
        }
        InternalSum internalSum = new InternalSum("dummy", 0, null, null);
        InternalSum reduced = internalSum.reduce(aggregations, null);
        assertEquals(expected, reduced.value(), delta);
    }

    @Override
    protected void assertFromXContent(InternalSum sum, ParsedAggregation parsedAggregation) {
        ParsedSum parsed = ((ParsedSum) parsedAggregation);
        assertEquals(sum.getValue(), parsed.getValue(), Double.MIN_VALUE);
        assertEquals(sum.getValueAsString(), parsed.getValueAsString());
    }

    @Override
    protected InternalSum mutateInstance(InternalSum instance) {
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
        return new InternalSum(name, value, formatter, metadata);
    }
}
