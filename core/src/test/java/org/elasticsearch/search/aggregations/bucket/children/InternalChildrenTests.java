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

package org.elasticsearch.search.aggregations.bucket.children;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregationTestCase;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.metrics.max.InternalMax;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class InternalChildrenTests extends InternalAggregationTestCase<InternalChildren> {

    @Override
    protected InternalChildren createTestInstance(String name, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) {
        // we shouldn't use the full long range here since we sum doc count on reduce, and don't want to overflow the long range there
        long docCount = randomIntBetween(0, Integer.MAX_VALUE);
        int numAggregations = randomIntBetween(0, 20);
        List<InternalAggregation> aggs = new ArrayList<>(numAggregations);
        for (int i = 0; i < numAggregations; i++) {
            aggs.add(new InternalMax(randomAsciiOfLength(5), randomDouble(),
                    randomFrom(DocValueFormat.BOOLEAN, DocValueFormat.GEOHASH, DocValueFormat.IP, DocValueFormat.RAW), pipelineAggregators,
                    metaData));
        }
        // don't randomize the name parameter, since InternalSingleBucketAggregation#doReduce asserts its the same for all reduced aggs
        return new InternalChildren("childAgg", docCount, new InternalAggregations(aggs), pipelineAggregators, metaData);
    }

    @Override
    protected void assertReduced(InternalChildren reduced, List<InternalChildren> inputs) {
        long expectedDocCount = 0;
        for (Children input : inputs) {
            expectedDocCount += input.getDocCount();
        }
        assertEquals(expectedDocCount, reduced.getDocCount());
    }

    @Override
    protected Reader<InternalChildren> instanceReader() {
        return InternalChildren::new;
    }

}
