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

package org.elasticsearch.search.aggregations.pipeline.bucketmetrics.percentile;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregationTestCase;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentile;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.search.aggregations.metrics.percentiles.InternalPercentilesTestCase.randomPercents;

public class InternalPercentilesBucketTests extends InternalAggregationTestCase<InternalPercentilesBucket> {

    @Override
    protected InternalPercentilesBucket createTestInstance(String name, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) {
        return createTestInstance(name, pipelineAggregators, metaData, randomPercents());
    }

    private static InternalPercentilesBucket createTestInstance(String name, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData, double[] percents) {
        DocValueFormat format = randomNumericDocValueFormat();
        final double[] percentiles = new double[percents.length];
        for (int i = 0; i < percents.length; ++i) {
            percentiles[i] = frequently() ? randomDouble() : Double.NaN;
        }
        return new InternalPercentilesBucket(name, percents, percentiles, format, pipelineAggregators, metaData);
    }

    @Override
    public void testReduceRandom() {
        expectThrows(UnsupportedOperationException.class,
                () -> createTestInstance("name", Collections.emptyList(), null).reduce(null, null));
    }

    @Override
    protected void assertReduced(InternalPercentilesBucket reduced, List<InternalPercentilesBucket> inputs) {
        // no test since reduce operation is unsupported
    }

    @Override
    protected Writeable.Reader<InternalPercentilesBucket> instanceReader() {
        return InternalPercentilesBucket::new;
    }

    /**
     * check that we don't rely on the percent array order and that the iterator returns the values in the original order
     */
    public void testPercentOrder() {
        final double[] percents =  new double[]{ 0.50, 0.25, 0.01, 0.99, 0.60 };
        InternalPercentilesBucket aggregation = createTestInstance("test", Collections.emptyList(), Collections.emptyMap(), percents);
        Iterator<Percentile> iterator = aggregation.iterator();
        for (double percent : percents) {
            assertTrue(iterator.hasNext());
            Percentile percentile = iterator.next();
            assertEquals(percent, percentile.getPercent(), 0.0d);
            assertEquals(aggregation.percentile(percent), percentile.getValue(), 0.0d);
        }
    }

    public void testErrorOnDifferentArgumentSize() {
        final double[] percents =  new double[]{ 0.1, 0.2, 0.3};
        final double[] percentiles =  new double[]{ 0.10, 0.2};
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new InternalPercentilesBucket("test", percents,
                percentiles, DocValueFormat.RAW, Collections.emptyList(), Collections.emptyMap()));
        assertEquals("The number of provided percents and percentiles didn't match. percents: [0.1, 0.2, 0.3], percentiles: [0.1, 0.2]",
                e.getMessage());
    }
}
