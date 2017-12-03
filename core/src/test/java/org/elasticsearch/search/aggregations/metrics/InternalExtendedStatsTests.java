/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStats.Bounds;
import org.elasticsearch.search.aggregations.metrics.stats.extended.InternalExtendedStats;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ParsedExtendedStats;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.test.InternalAggregationTestCase;

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
    protected InternalExtendedStats createTestInstance(String name, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) {
        long count = frequently() ? randomIntBetween(1, Integer.MAX_VALUE) : 0;
        double min = randomDoubleBetween(-1000000, 1000000, true);
        double max = randomDoubleBetween(-1000000, 1000000, true);
        double sum = randomDoubleBetween(-1000000, 1000000, true);
        DocValueFormat format = randomNumericDocValueFormat();
        return createInstance(name, count, sum, min, max, randomDoubleBetween(0, 1000000, true),
                sigma, format, pipelineAggregators, metaData);
    }

    protected InternalExtendedStats createInstance(String name, long count, double sum, double min, double max, double sumOfSqrs,
            double sigma, DocValueFormat formatter, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        return new InternalExtendedStats(name, count, sum, min, max, sumOfSqrs, sigma, formatter, pipelineAggregators, metaData);
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
        assertEquals(expectedSum, reduced.getSum(), Math.abs(expectedSum) * 1e-11);
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
        // for count == 0, fields are rendered as `null`, so  we test that we parse to default values used also in the reduce phase
        assertEquals(count > 0 ? aggregation.getSumOfSquares() : 0 , parsed.getSumOfSquares(), 0);
        assertEquals(count > 0 ? aggregation.getVariance() : 0 , parsed.getVariance(), 0);
        assertEquals(count > 0 ? aggregation.getStdDeviation() : 0 , parsed.getStdDeviation(), 0);
        assertEquals(count > 0 ? aggregation.getStdDeviationBound(Bounds.LOWER) : 0 , parsed.getStdDeviationBound(Bounds.LOWER), 0);
        assertEquals(count > 0 ? aggregation.getStdDeviationBound(Bounds.UPPER) : 0 , parsed.getStdDeviationBound(Bounds.UPPER), 0);
        // also as_string values are only rendered for count != 0
        if (count > 0) {
            assertEquals(aggregation.getSumOfSquaresAsString(), parsed.getSumOfSquaresAsString());
            assertEquals(aggregation.getVarianceAsString(), parsed.getVarianceAsString());
            assertEquals(aggregation.getStdDeviationAsString(), parsed.getStdDeviationAsString());
            assertEquals(aggregation.getStdDeviationBoundAsString(Bounds.LOWER), parsed.getStdDeviationBoundAsString(Bounds.LOWER));
            assertEquals(aggregation.getStdDeviationBoundAsString(Bounds.UPPER), parsed.getStdDeviationBoundAsString(Bounds.UPPER));
        }
    }

    @Override
    protected Writeable.Reader<InternalExtendedStats> instanceReader() {
        return InternalExtendedStats::new;
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
        List<PipelineAggregator> pipelineAggregators = instance.pipelineAggregators();
        Map<String, Object> metaData = instance.getMetaData();
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
            if (metaData == null) {
                metaData = new HashMap<>(1);
            } else {
                metaData = new HashMap<>(instance.getMetaData());
            }
            metaData.put(randomAlphaOfLength(15), randomInt());
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }
        return new InternalExtendedStats(name, count, sum, min, max, sumOfSqrs, sigma, formatter, pipelineAggregators, metaData);
    }
}
