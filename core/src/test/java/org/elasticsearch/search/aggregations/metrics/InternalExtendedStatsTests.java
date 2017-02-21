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
import org.elasticsearch.search.aggregations.InternalAggregationTestCase;
import org.elasticsearch.search.aggregations.metrics.stats.extended.InternalExtendedStats;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.junit.Before;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class InternalExtendedStatsTests extends InternalAggregationTestCase<InternalExtendedStats> {
    private double sigma;

    @Before
    public void randomSigma() {
        this.sigma = randomDoubleBetween(0, 10, true);
    }

    @Override
    protected InternalExtendedStats createTestInstance(String name, List<PipelineAggregator> pipelineAggregators,
                                                       Map<String, Object> metaData) {
        long count = randomIntBetween(1, 50);
        double[] minMax = new double[2];
        minMax[0] = randomDouble();
        minMax[0] = randomDouble();
        double sum = randomDoubleBetween(0, 100, true);
        return new InternalExtendedStats(name, count, sum, minMax[0], minMax[1],
            randomDouble(), sigma, DocValueFormat.RAW,
            pipelineAggregators, Collections.emptyMap());
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
        assertEquals(expectedSum, reduced.getSum(), 1e-10);
        assertEquals(expectedMin, reduced.getMin(), 0d);
        assertEquals(expectedMax, reduced.getMax(), 0d);
        assertEquals(expectedSumOfSquare, reduced.getSumOfSquares(), 1e-10);
    }

    @Override
    protected Writeable.Reader<InternalExtendedStats> instanceReader() {
        return InternalExtendedStats::new;
    }
}
