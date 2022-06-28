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
import org.elasticsearch.search.aggregations.metrics.ExtendedStats.Bounds;
import org.elasticsearch.test.InternalAggregationTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InternalExtendedStatsTests extends InternalAggregationTestCase<InternalExtendedStats> {

    private double sigma;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        this.sigma = randomDoubleBetween(0, 10, true);
    }

    @Override
    protected InternalExtendedStats createTestInstance(String name, Map<String, Object> metadata) {
        long count = frequently() ? randomIntBetween(1, Integer.MAX_VALUE) : 0;
        double min = randomDoubleBetween(-1000000, 1000000, true);
        double max = randomDoubleBetween(-1000000, 1000000, true);
        double sum = randomDoubleBetween(-1000000, 1000000, true);
        DocValueFormat format = randomNumericDocValueFormat();
        return createInstance(name, count, sum, min, max, randomDoubleBetween(0, 1000000, true), sigma, format, metadata);
    }

    protected InternalExtendedStats createInstance(
        String name,
        long count,
        double sum,
        double min,
        double max,
        double sumOfSqrs,
        double sigma,
        DocValueFormat formatter,
        Map<String, Object> metadata
    ) {
        return new InternalExtendedStats(name, count, sum, min, max, sumOfSqrs, sigma, formatter, metadata);

    }

    @Override
    protected void assertReduced(InternalExtendedStats reduced, List<InternalExtendedStats> inputs) {
        long expectedCount = 0;
        double expectedSum = 0;
        double expectedSumOfSquare = 0;
        double expectedMin = Double.POSITIVE_INFINITY;
        double expectedMax = Double.NEGATIVE_INFINITY;
        for (InternalExtendedStats stats : inputs) {
            assertEquals(sigma, stats.getSigma(), 0);
            expectedCount += stats.getCount();
            if (Double.compare(stats.getMin(), expectedMin) < 0) {
                expectedMin = stats.getMin();
            }
            if (Double.compare(stats.getMax(), expectedMax) > 0) {
                expectedMax = stats.getMax();
            }
            expectedSum += stats.getSum();
            expectedSumOfSquare += stats.getSumOfSquares();
        }
        assertEquals(sigma, reduced.getSigma(), 0);
        assertEquals(expectedCount, reduced.getCount());
        // The order in which you add double values in java can give different results. The difference can
        // be larger for large sum values, so we make the delta in the assertion depend on the values magnitude
        assertEquals(expectedSum, reduced.getSum(), Math.abs(expectedSum) * 1e-10);
        assertEquals(expectedMin, reduced.getMin(), 0d);
        assertEquals(expectedMax, reduced.getMax(), 0d);
        // summing squared values, see reason for delta above
        assertEquals(expectedSumOfSquare, reduced.getSumOfSquares(), expectedSumOfSquare * 1e-14);
    }

    @Override
    protected void assertFromXContent(InternalExtendedStats aggregation, ParsedAggregation parsedAggregation) {
        assertTrue(parsedAggregation instanceof ParsedExtendedStats);
        ParsedExtendedStats parsed = (ParsedExtendedStats) parsedAggregation;
        InternalStatsTests.assertStats(aggregation, parsed);

        long count = aggregation.getCount();
        // for count == 0, fields are rendered as `null`, so we test that we parse to default values used also in the reduce phase
        assertEquals(count > 0 ? aggregation.getSumOfSquares() : 0, parsed.getSumOfSquares(), 0);
        assertEquals(count > 0 ? aggregation.getVariance() : 0, parsed.getVariance(), 0);
        assertEquals(count > 0 ? aggregation.getVariancePopulation() : 0, parsed.getVariancePopulation(), 0);
        assertEquals(count > 0 ? aggregation.getVarianceSampling() : 0, parsed.getVarianceSampling(), 0);
        assertEquals(count > 0 ? aggregation.getStdDeviation() : 0, parsed.getStdDeviation(), 0);
        assertEquals(count > 0 ? aggregation.getStdDeviationPopulation() : 0, parsed.getStdDeviationPopulation(), 0);
        assertEquals(count > 0 ? aggregation.getStdDeviationSampling() : 0, parsed.getStdDeviationSampling(), 0);
        assertEquals(count > 0 ? aggregation.getStdDeviationBound(Bounds.LOWER) : 0, parsed.getStdDeviationBound(Bounds.LOWER), 0);
        assertEquals(count > 0 ? aggregation.getStdDeviationBound(Bounds.UPPER) : 0, parsed.getStdDeviationBound(Bounds.UPPER), 0);
        assertEquals(
            count > 0 ? aggregation.getStdDeviationBound(Bounds.LOWER_POPULATION) : 0,
            parsed.getStdDeviationBound(Bounds.LOWER_POPULATION),
            0
        );
        assertEquals(
            count > 0 ? aggregation.getStdDeviationBound(Bounds.UPPER_POPULATION) : 0,
            parsed.getStdDeviationBound(Bounds.UPPER_POPULATION),
            0
        );
        assertEquals(
            count > 0 ? aggregation.getStdDeviationBound(Bounds.LOWER_SAMPLING) : 0,
            parsed.getStdDeviationBound(Bounds.LOWER_SAMPLING),
            0
        );
        assertEquals(
            count > 0 ? aggregation.getStdDeviationBound(Bounds.UPPER_SAMPLING) : 0,
            parsed.getStdDeviationBound(Bounds.UPPER_SAMPLING),
            0
        );
        // also as_string values are only rendered for count != 0
        if (count > 0) {
            assertEquals(aggregation.getSumOfSquaresAsString(), parsed.getSumOfSquaresAsString());
            assertEquals(aggregation.getVarianceAsString(), parsed.getVarianceAsString());
            assertEquals(aggregation.getVariancePopulationAsString(), parsed.getVariancePopulationAsString());
            assertEquals(aggregation.getVarianceSamplingAsString(), parsed.getVarianceSamplingAsString());
            assertEquals(aggregation.getStdDeviationAsString(), parsed.getStdDeviationAsString());
            assertEquals(aggregation.getStdDeviationPopulationAsString(), parsed.getStdDeviationPopulationAsString());
            assertEquals(aggregation.getStdDeviationSamplingAsString(), parsed.getStdDeviationSamplingAsString());
            assertEquals(aggregation.getStdDeviationBoundAsString(Bounds.LOWER), parsed.getStdDeviationBoundAsString(Bounds.LOWER));
            assertEquals(aggregation.getStdDeviationBoundAsString(Bounds.UPPER), parsed.getStdDeviationBoundAsString(Bounds.UPPER));
            assertEquals(
                aggregation.getStdDeviationBoundAsString(Bounds.LOWER_POPULATION),
                parsed.getStdDeviationBoundAsString(Bounds.LOWER_POPULATION)
            );
            assertEquals(
                aggregation.getStdDeviationBoundAsString(Bounds.UPPER_POPULATION),
                parsed.getStdDeviationBoundAsString(Bounds.UPPER_POPULATION)
            );
            assertEquals(
                aggregation.getStdDeviationBoundAsString(Bounds.LOWER_SAMPLING),
                parsed.getStdDeviationBoundAsString(Bounds.LOWER_SAMPLING)
            );
            assertEquals(
                aggregation.getStdDeviationBoundAsString(Bounds.UPPER_SAMPLING),
                parsed.getStdDeviationBoundAsString(Bounds.UPPER_SAMPLING)
            );
        }
    }

    @Override
    protected InternalExtendedStats mutateInstance(InternalExtendedStats instance) {
        String name = instance.getName();
        long count = instance.getCount();
        double sum = instance.getSum();
        double min = instance.getMin();
        double max = instance.getMax();
        double sumOfSqrs = instance.getSumOfSquares();
        double sigma = instance.getSigma();
        DocValueFormat formatter = instance.format;
        Map<String, Object> metadata = instance.getMetadata();
        switch (between(0, 7)) {
            case 0:
                name += randomAlphaOfLength(5);
                break;
            case 1:
                if (Double.isFinite(count)) {
                    count += between(1, 100);
                } else {
                    count = between(1, 100);
                }
                break;
            case 2:
                if (Double.isFinite(sum)) {
                    sum += between(1, 100);
                } else {
                    sum = between(1, 100);
                }
                break;
            case 3:
                if (Double.isFinite(min)) {
                    min += between(1, 100);
                } else {
                    min = between(1, 100);
                }
                break;
            case 4:
                if (Double.isFinite(max)) {
                    max += between(1, 100);
                } else {
                    max = between(1, 100);
                }
                break;
            case 5:
                if (Double.isFinite(sumOfSqrs)) {
                    sumOfSqrs += between(1, 100);
                } else {
                    sumOfSqrs = between(1, 100);
                }
                break;
            case 6:
                if (Double.isFinite(sigma)) {
                    sigma += between(1, 10);
                } else {
                    sigma = between(1, 10);
                }
                break;
            case 7:
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
        return new InternalExtendedStats(name, count, sum, min, max, sumOfSqrs, sigma, formatter, metadata);
    }

    public void testSummationAccuracy() {
        double[] values = new double[] { 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.9, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7 };
        verifySumOfSqrsOfDoubles(values, 13.5, 0d);

        int n = randomIntBetween(5, 10);
        values = new double[n];
        double sum = 0;
        for (int i = 0; i < n; i++) {
            values[i] = frequently()
                ? randomFrom(Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY)
                : randomDoubleBetween(Double.MIN_VALUE, Double.MAX_VALUE, true);
            sum += values[i];
        }
        verifySumOfSqrsOfDoubles(values, sum, TOLERANCE);

        // Summing up some big double values and expect infinity result
        n = randomIntBetween(5, 10);
        double[] largeValues = new double[n];
        for (int i = 0; i < n; i++) {
            largeValues[i] = Double.MAX_VALUE;
        }
        verifySumOfSqrsOfDoubles(largeValues, Double.POSITIVE_INFINITY, 0d);

        for (int i = 0; i < n; i++) {
            largeValues[i] = -Double.MAX_VALUE;
        }
        verifySumOfSqrsOfDoubles(largeValues, Double.NEGATIVE_INFINITY, 0d);
    }

    private void verifySumOfSqrsOfDoubles(double[] values, double expectedSumOfSqrs, double delta) {
        List<InternalAggregation> aggregations = new ArrayList<>(values.length);
        double sigma = randomDouble();
        for (double sumOfSqrs : values) {
            aggregations.add(new InternalExtendedStats("dummy1", 1, 0.0, 0.0, 0.0, sumOfSqrs, sigma, null, null));
        }
        InternalExtendedStats stats = new InternalExtendedStats("dummy", 1, 0.0, 0.0, 0.0, 0.0, sigma, null, null);
        InternalExtendedStats reduced = stats.reduce(aggregations, null);
        assertEquals(expectedSumOfSqrs, reduced.getSumOfSquares(), delta);
    }
}
