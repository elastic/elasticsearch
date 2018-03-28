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

package org.elasticsearch.search.aggregations.bucket.filter;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.test.InternalMultiBucketAggregationTestCase;
import org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilters.InternalBucket;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class InternalFiltersTests extends InternalMultiBucketAggregationTestCase<InternalFilters> {

    private boolean keyed;
    private List<String> keys;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        keyed = randomBoolean();
        keys = new ArrayList<>();
        int numBuckets = randomNumberOfBuckets();
        for (int i = 0; i < numBuckets; i++) {
            if (keyed) {
                keys.add(randomAlphaOfLength(5));
            } else {
                // this is what the FiltersAggregationBuilder ctor does when not providing KeyedFilter
                keys.add(String.valueOf(i));
            }
        }
    }

    @Override
    protected InternalFilters createTestInstance(String name, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData,
            InternalAggregations aggregations) {
        final List<InternalFilters.InternalBucket> buckets = new ArrayList<>();
        for (int i = 0; i < keys.size(); ++i) {
            String key = keys.get(i);
            int docCount = randomIntBetween(0, 1000);
            buckets.add(new InternalFilters.InternalBucket(key, docCount, aggregations, keyed));
        }
        return new InternalFilters(name, buckets, keyed, pipelineAggregators, metaData);
    }

    @Override
    protected void assertReduced(InternalFilters reduced, List<InternalFilters> inputs) {
        final Map<String, Long> expectedCounts = new TreeMap<>();
        for (InternalFilters input : inputs) {
            for (InternalFilters.InternalBucket bucket : input.getBuckets()) {
                expectedCounts.compute(bucket.getKeyAsString(),
                        (key, oldValue) -> (oldValue == null ? 0 : oldValue) + bucket.getDocCount());
            }
        }
        final Map<String, Long> actualCounts = new TreeMap<>();
        for (InternalFilters.InternalBucket bucket : reduced.getBuckets()) {
            actualCounts.compute(bucket.getKeyAsString(),
                    (key, oldValue) -> (oldValue == null ? 0 : oldValue) + bucket.getDocCount());
        }
        assertEquals(expectedCounts, actualCounts);
    }

    @Override
    protected Reader<InternalFilters> instanceReader() {
        return InternalFilters::new;
    }

    @Override
    protected Class<? extends ParsedMultiBucketAggregation> implementationClass() {
        return ParsedFilters.class;
    }

    @Override
    protected InternalFilters mutateInstance(InternalFilters instance) {
        String name = instance.getName();
        List<InternalBucket> buckets = instance.getBuckets();
        List<PipelineAggregator> pipelineAggregators = instance.pipelineAggregators();
        Map<String, Object> metaData = instance.getMetaData();
        switch (between(0, 2)) {
        case 0:
            name += randomAlphaOfLength(5);
            break;
        case 1:
            buckets = new ArrayList<>(buckets);
            buckets.add(new InternalFilters.InternalBucket("test", randomIntBetween(0, 1000), InternalAggregations.EMPTY, keyed));
            break;
        case 2:
        default:
            if (metaData == null) {
                metaData = new HashMap<>(1);
            } else {
                metaData = new HashMap<>(instance.getMetaData());
            }
            metaData.put(randomAlphaOfLength(15), randomInt());
            break;
        }
        return new InternalFilters(name, buckets, keyed, pipelineAggregators, metaData);
    }
}
