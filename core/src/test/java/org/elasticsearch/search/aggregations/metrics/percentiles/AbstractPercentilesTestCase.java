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
import org.elasticsearch.search.aggregations.Aggregation.CommonFields;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.test.InternalAggregationTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

public abstract class AbstractPercentilesTestCase<T extends InternalAggregation & Iterable<Percentile>>
        extends InternalAggregationTestCase<T> {

    private double[] percents;
    private boolean keyed;
    private DocValueFormat docValueFormat;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        percents = randomPercents(false);
        keyed = randomBoolean();
        docValueFormat = randomNumericDocValueFormat();
    }

    @Override
    protected T createTestInstance(String name, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        int numValues = randomInt(100);
        double[] values = new double[numValues];
        for (int i = 0; i < numValues; ++i) {
            values[i] = randomDouble();
        }
        return createTestInstance(name, pipelineAggregators, metaData, keyed, docValueFormat, percents, values);
    }

    protected abstract T createTestInstance(String name, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData,
                                            boolean keyed, DocValueFormat format, double[] percents, double[] values);

    protected abstract Class<? extends ParsedPercentiles> implementationClass();

    public void testPercentilesIterators() throws IOException {
        final T aggregation = createTestInstance();
        final Iterable<Percentile> parsedAggregation = parseAndAssert(aggregation, false, false);

        Iterator<Percentile> it = aggregation.iterator();
        Iterator<Percentile> parsedIt = parsedAggregation.iterator();
        while (it.hasNext()) {
            assertEquals(it.next(), parsedIt.next());
        }
    }

    public static double[] randomPercents(boolean sorted) {
        List<Double> randomCdfValues = randomSubsetOf(randomIntBetween(1, 7), 0.01d, 0.05d, 0.25d, 0.50d, 0.75d, 0.95d, 0.99d);
        double[] percents = new double[randomCdfValues.size()];
        for (int i = 0; i < randomCdfValues.size(); i++) {
            percents[i] = randomCdfValues.get(i);
        }
        if (sorted) {
            Arrays.sort(percents);
        }
        return percents;
    }

    @Override
    protected Predicate<String> excludePathsFromXContentInsertion() {
        return path -> path.endsWith(CommonFields.VALUES.getPreferredName());
    }
}
