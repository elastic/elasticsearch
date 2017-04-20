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

package org.elasticsearch.search.aggregations.metrics.percentiles.hdr;

import org.HdrHistogram.DoubleHistogram;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.metrics.percentiles.InternalPercentilesTestCase;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentile;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

public class InternalHDRPercentilesTests extends InternalPercentilesTestCase<InternalHDRPercentiles> {

    @Override
    protected InternalHDRPercentiles createTestInstance(String name,
                                                        List<PipelineAggregator> pipelineAggregators,
                                                        Map<String, Object>  metaData,
                                                        boolean keyed, DocValueFormat format, double[] percents, double[] values) {

        final DoubleHistogram state = new DoubleHistogram(3);
        Arrays.stream(values).forEach(state::recordValue);

        return new InternalHDRPercentiles(name, percents, state, keyed, format, pipelineAggregators, metaData);
    }

    @Override
    protected void assertReduced(InternalHDRPercentiles reduced, List<InternalHDRPercentiles> inputs) {
        // it is hard to check the values due to the inaccuracy of the algorithm
        long totalCount = 0;
        for (InternalHDRPercentiles ranks : inputs) {
            totalCount += ranks.state.getTotalCount();
        }
        assertEquals(totalCount, reduced.state.getTotalCount());
    }

    @Override
    protected Writeable.Reader<InternalHDRPercentiles> instanceReader() {
        return InternalHDRPercentiles::new;
    }

    public void testIterator() {
        final double[] percents =  randomPercents();
        final double[] values = new double[frequently() ? randomIntBetween(1, 10) : 0];
        for (int i = 0; i < values.length; ++i) {
            values[i] = randomDouble();
        }

        InternalHDRPercentiles aggregation =
                createTestInstance("test", emptyList(), emptyMap(), false, randomNumericDocValueFormat(), percents, values);

        Iterator<Percentile> iterator = aggregation.iterator();
        for (double percent : percents) {
            assertTrue(iterator.hasNext());

            Percentile percentile = iterator.next();
            assertEquals(percent, percentile.getPercent(), 0.0d);
            assertEquals(aggregation.percentile(percent), percentile.getValue(), 0.0d);
        }
    }
}
