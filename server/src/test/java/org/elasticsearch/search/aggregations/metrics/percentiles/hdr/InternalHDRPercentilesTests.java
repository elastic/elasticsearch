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
import org.elasticsearch.search.aggregations.metrics.percentiles.ParsedPercentiles;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentile;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.util.Arrays;
import java.util.HashMap;
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

    @Override
    protected Class<? extends ParsedPercentiles> implementationClass() {
        return ParsedHDRPercentiles.class;
    }

    public void testIterator() {
        final double[] percents =  randomPercents(false);
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

    @Override
    protected InternalHDRPercentiles mutateInstance(InternalHDRPercentiles instance) {
        String name = instance.getName();
        double[] percents = instance.keys;
        DoubleHistogram state = instance.state;
        boolean keyed = instance.keyed;
        DocValueFormat formatter = instance.formatter();
        List<PipelineAggregator> pipelineAggregators = instance.pipelineAggregators();
        Map<String, Object> metaData = instance.getMetaData();
        switch (between(0, 4)) {
        case 0:
            name += randomAlphaOfLength(5);
            break;
        case 1:
            percents = Arrays.copyOf(percents, percents.length + 1);
            percents[percents.length - 1] = randomDouble() * 100;
            Arrays.sort(percents);
            break;
        case 2:
            state = new DoubleHistogram(state);
            for (int i = 0; i < between(10, 100); i++) {
                state.recordValue(randomDouble());
            }
            break;
        case 3:
            keyed = keyed == false;
            break;
        case 4:
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
        return new InternalHDRPercentiles(name, percents, state, keyed, formatter, pipelineAggregators, metaData);
    }
}
