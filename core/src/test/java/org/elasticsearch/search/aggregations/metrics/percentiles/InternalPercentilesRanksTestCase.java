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

package org.elasticsearch.search.aggregations.metrics.percentiles;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregationTestCase;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.util.List;
import java.util.Map;

public abstract class InternalPercentilesRanksTestCase<T extends InternalAggregation> extends InternalAggregationTestCase<T> {

    @Override
    protected final T createTestInstance(String name, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        final boolean keyed = randomBoolean();
        final DocValueFormat format = randomFrom(DocValueFormat.RAW, new DocValueFormat.Decimal("###.##"));
        List<Double> randomCdfValues = randomSubsetOf(randomIntBetween(1, 5), 0.01d, 0.05d, 0.25d, 0.50d, 0.75d, 0.95d, 0.99d);
        double[] cdfValues = new double[randomCdfValues.size()];
        for (int i = 0; i < randomCdfValues.size(); i++) {
            cdfValues[i] = randomCdfValues.get(i);
        }
        return createTestInstance(name, pipelineAggregators, metaData, cdfValues, keyed, format);
    }

    protected abstract T createTestInstance(String name,  List<PipelineAggregator> aggregators, Map<String, Object> metadata,
                                            double[] cdfValues, boolean keyed, DocValueFormat format);

    @Override
    protected final void assertFromXContent(T aggregation, ParsedAggregation parsedAggregation) {
        assertTrue(aggregation instanceof PercentileRanks);
        PercentileRanks percentileRanks = (PercentileRanks) aggregation;

        assertTrue(parsedAggregation instanceof PercentileRanks);
        PercentileRanks parsedPercentileRanks = (PercentileRanks) parsedAggregation;

        for (Percentile percentile : percentileRanks) {
            Double value = percentile.getValue();
            assertEquals(percentileRanks.percent(value), parsedPercentileRanks.percent(value), 0);
            assertEquals(percentileRanks.percentAsString(value), parsedPercentileRanks.percentAsString(value));
        }

        Class<? extends ParsedPercentileRanks> parsedClass = parsedParsedPercentileRanksClass();
        assertNotNull(parsedClass);
        assertTrue(parsedClass.isInstance(parsedAggregation));
    }

    protected abstract Class<? extends ParsedPercentileRanks> parsedParsedPercentileRanksClass();
}
