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

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.aggregations.metrics.stats.InternalStats;
import org.elasticsearch.search.aggregations.metrics.stats.ParsedStats;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.test.InternalAggregationTestCase;

import java.util.List;
import java.util.Map;

public class InternalStatsTests extends InternalAggregationTestCase<InternalStats> {

    @Override
    protected InternalStats createTestInstance(String name, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        long count = frequently() ? randomIntBetween(1, Integer.MAX_VALUE) : 0;
        double min = randomDoubleBetween(-1000000, 1000000, true);
        double max = randomDoubleBetween(-1000000, 1000000, true);
        double sum = randomDoubleBetween(-1000000, 1000000, true);
        DocValueFormat format = randomNumericDocValueFormat();
        return createInstance(name, count, sum, min, max, format, pipelineAggregators, metaData);
    }

    protected InternalStats createInstance(String name, long count, double sum, double min, double max, DocValueFormat formatter,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        return new InternalStats(name, count, sum, min, max, formatter, pipelineAggregators, metaData);
    }

    @Override
    protected void assertFromXContent(InternalStats aggregation, ParsedAggregation parsedAggregation) {
        assertTrue(parsedAggregation instanceof ParsedStats);
        ParsedStats parsed = (ParsedStats) parsedAggregation;
        assertStats(aggregation, parsed);
    }

    static void assertStats(InternalStats aggregation, ParsedStats parsed) {
        long count = aggregation.getCount();
        assertEquals(count, parsed.getCount());
        // for count == 0, fields are rendered as `null`, so  we test that we parse to default values used also in the reduce phase
        assertEquals(count > 0 ? aggregation.getMin() : Double.POSITIVE_INFINITY , parsed.getMin(), 0);
        assertEquals(count > 0 ? aggregation.getMax() : Double.NEGATIVE_INFINITY, parsed.getMax(), 0);
        assertEquals(count > 0 ? aggregation.getSum() : 0, parsed.getSum(), 0);
        assertEquals(count > 0 ? aggregation.getAvg() : 0, parsed.getAvg(), 0);
        // also as_string values are only rendered for count != 0
        if (count > 0) {
            assertEquals(aggregation.getMinAsString(), parsed.getMinAsString());
            assertEquals(aggregation.getMaxAsString(), parsed.getMaxAsString());
            assertEquals(aggregation.getSumAsString(), parsed.getSumAsString());
            assertEquals(aggregation.getAvgAsString(), parsed.getAvgAsString());
        }
    }
}

