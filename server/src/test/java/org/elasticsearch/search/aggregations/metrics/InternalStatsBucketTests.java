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
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.InternalStatsBucket;
import org.elasticsearch.search.aggregations.pipeline.ParsedStatsBucket;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class InternalStatsBucketTests extends InternalStatsTests {

    @Override
    protected InternalStatsBucket createInstance(String name, long count, double sum, double min, double max,
            DocValueFormat formatter, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        return new InternalStatsBucket(name, count, sum, min, max, formatter, pipelineAggregators, metaData);
    }

    @Override
    public void testReduceRandom() {
        expectThrows(UnsupportedOperationException.class,
                () -> createTestInstance("name", Collections.emptyList(), null).reduce(null, null));
    }

    @Override
    protected void assertReduced(InternalStats reduced, List<InternalStats> inputs) {
        // no test since reduce operation is unsupported
    }

    @Override
    protected Writeable.Reader<InternalStats> instanceReader() {
        return InternalStatsBucket::new;
    }

    @Override
    protected void assertFromXContent(InternalStats aggregation, ParsedAggregation parsedAggregation) {
        super.assertFromXContent(aggregation, parsedAggregation);
        assertTrue(parsedAggregation instanceof ParsedStatsBucket);
    }
}
