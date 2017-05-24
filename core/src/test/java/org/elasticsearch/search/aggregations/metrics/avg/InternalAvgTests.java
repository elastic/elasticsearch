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

package org.elasticsearch.search.aggregations.metrics.avg;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.test.InternalAggregationTestCase;

import java.util.List;
import java.util.Map;

public class InternalAvgTests extends InternalAggregationTestCase<InternalAvg> {

    @Override
    protected InternalAvg createTestInstance(String name, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        DocValueFormat formatter = randomNumericDocValueFormat();
        long count = frequently() ? randomNonNegativeLong() % 100000 : 0;
        return new InternalAvg(name, randomDoubleBetween(0, 100000, true), count, formatter, pipelineAggregators, metaData);
    }

    @Override
    protected void assertFromXContent(InternalAvg avg, ParsedAggregation parsedAggregation) {
        ParsedAvg parsed = ((ParsedAvg) parsedAggregation);
        assertEquals(avg.getValue(), parsed.getValue(), Double.MIN_VALUE);
        // we don't print out VALUE_AS_STRING for avg.getCount() == 0, so we cannot get the exact same value back
        if (avg.getCount() != 0) {
            assertEquals(avg.getValueAsString(), parsed.getValueAsString());
        }
    }
}
