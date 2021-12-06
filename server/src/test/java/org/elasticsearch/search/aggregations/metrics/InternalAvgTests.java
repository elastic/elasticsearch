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

public class InternalAvgTests extends InternalAggregationTestCase<InternalAvg> {

    @Override
    protected InternalAvg createTestInstance(String name, Map<String, Object> metadata) {
        DocValueFormat formatter = randomNumericDocValueFormat();
        long count = frequently() ? randomNonNegativeLong() % 100000 : 0;
        return new InternalAvg(name, randomDoubleBetween(0, 100000, true), count, formatter, metadata);
    }

    @Override
    protected void assertReduced(InternalAvg reduced, List<InternalAvg> inputs) {
        double sum = 0;
        long counts = 0;
        for (InternalAvg in : inputs) {
            sum += in.getSum();
            counts += in.getCount();
        }
        assertEquals(counts, reduced.getCount());
        assertEquals(sum, reduced.getSum(), 0.0000001);
        assertEquals(sum / counts, reduced.value(), 0.0000001);
    }

    public void testSummationAccuracy() {
        double[] values = new double[] { 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.9, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7 };
        verifyAvgOfDoubles(values, 0.9, 0d);

        int n = randomIntBetween(5, 10);
        values = new double[n];
        double sum = 0;
        for (int i = 0; i < n; i++) {
            values[i] = frequently()
                ? randomFrom(Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY)
                : randomDoubleBetween(Double.MIN_VALUE, Double.MAX_VALUE, true);
            sum += values[i];
        }
        verifyAvgOfDoubles(values, sum / n, TOLERANCE);

        // Summing up some big double values and expect infinity result
        n = randomIntBetween(5, 10);
        double[] largeValues = new double[n];
        for (int i = 0; i < n; i++) {
            largeValues[i] = Double.MAX_VALUE;
        }
        verifyAvgOfDoubles(largeValues, Double.POSITIVE_INFINITY, 0d);

        for (int i = 0; i < n; i++) {
            largeValues[i] = -Double.MAX_VALUE;
        }
        verifyAvgOfDoubles(largeValues, Double.NEGATIVE_INFINITY, 0d);
    }

    private void verifyAvgOfDoubles(double[] values, double expected, double delta) {
        List<InternalAggregation> aggregations = new ArrayList<>(values.length);
        for (double value : values) {
            aggregations.add(new InternalAvg("dummy1", value, 1, null, null));
        }
        InternalAvg internalAvg = new InternalAvg("dummy2", 0, 0, null, null);
        InternalAvg reduced = internalAvg.reduce(aggregations, null);
        assertEquals(expected, reduced.getValue(), delta);
    }

    @Override
    protected void assertFromXContent(InternalAvg avg, ParsedAggregation parsedAggregation) {
        ParsedAvg parsed = ((ParsedAvg) parsedAggregation);
        assertEquals(avg.getValue(), parsed.getValue(), Double.MIN_VALUE);
        // we don't print out VALUE_AS_STRING for avg.getCount() == 0, so we cannot get the exact same value back
        if (avg.getCount() != 0) {
            assertEquals(avg.getValueAsString(), parsed.getValueAsString());
        }
    }

    @Override
    protected InternalAvg mutateInstance(InternalAvg instance) {
        String name = instance.getName();
        double sum = instance.getSum();
        long count = instance.getCount();
        DocValueFormat formatter = instance.getFormatter();
        Map<String, Object> metadata = instance.getMetadata();
        switch (between(0, 2)) {
            case 0:
                name += randomAlphaOfLength(5);
                break;
            case 1:
                if (Double.isFinite(sum)) {
                    sum += between(1, 100);
                } else {
                    sum = between(1, 100);
                }
                break;
            case 2:
                if (Double.isFinite(count)) {
                    count += between(1, 100);
                } else {
                    count = between(1, 100);
                }
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
        return new InternalAvg(name, sum, count, formatter, metadata);
    }
}
