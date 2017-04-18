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

package org.elasticsearch.search.aggregations.metrics.percentiles.tdigest;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregationTestCase;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.util.List;
import java.util.Map;

public class InternalTDigestPercentilesTests extends InternalAggregationTestCase<InternalTDigestPercentiles> {

    private final double[] percents = randomPercents();

    @Override
    protected InternalTDigestPercentiles createTestInstance(String name,
                                                            List<PipelineAggregator> pipelineAggregators,
                                                            Map<String, Object> metaData) {
        boolean keyed = randomBoolean();
        DocValueFormat format = DocValueFormat.RAW;
        TDigestState state = new TDigestState(100);

        int numValues = randomInt(10);
        for (int i = 0; i < numValues; ++i) {
            state.add(randomDouble() * 100);
        }
        assertEquals(state.centroidCount(), numValues);
        return new InternalTDigestPercentiles(name, percents, state, keyed, format, pipelineAggregators, metaData);
    }

    @Override
    protected void assertReduced(InternalTDigestPercentiles reduced, List<InternalTDigestPercentiles> inputs) {
        final TDigestState expectedState = new TDigestState(reduced.state.compression());

        long totalCount = 0;
        for (InternalTDigestPercentiles input : inputs) {
            assertArrayEquals(reduced.keys, input.keys, 0d);
            expectedState.add(input.state);
            totalCount += input.state.size();
        }

        assertEquals(totalCount, reduced.state.size());
        if (totalCount > 0) {
            assertEquals(expectedState.quantile(0), reduced.state.quantile(0), 0d);
            assertEquals(expectedState.quantile(1), reduced.state.quantile(1), 0d);
        }
    }

    @Override
    protected Writeable.Reader<InternalTDigestPercentiles> instanceReader() {
        return InternalTDigestPercentiles::new;
    }

    private static double[] randomPercents() {
        List<Double> randomCdfValues = randomSubsetOf(randomIntBetween(1, 7), 0.01d, 0.05d, 0.25d, 0.50d, 0.75d, 0.95d, 0.99d);
        double[] percents = new double[randomCdfValues.size()];
        for (int i = 0; i < randomCdfValues.size(); i++) {
            percents[i] = randomCdfValues.get(i);
        }
        return percents;
    }
}
