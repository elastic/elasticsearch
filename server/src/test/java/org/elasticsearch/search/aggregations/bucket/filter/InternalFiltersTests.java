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

import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilters.InternalBucket;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.PipelineTree;
import org.elasticsearch.test.InternalMultiBucketAggregationTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.sameInstance;

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
    protected InternalFilters createTestInstance(String name, Map<String, Object> metadata, InternalAggregations aggregations) {
        final List<InternalFilters.InternalBucket> buckets = new ArrayList<>();
        for (int i = 0; i < keys.size(); ++i) {
            String key = keys.get(i);
            int docCount = randomIntBetween(0, 1000);
            buckets.add(new InternalFilters.InternalBucket(key, docCount, aggregations, keyed));
        }
        return new InternalFilters(name, buckets, keyed, metadata);
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
    protected Class<? extends ParsedMultiBucketAggregation> implementationClass() {
        return ParsedFilters.class;
    }

    @Override
    protected InternalFilters mutateInstance(InternalFilters instance) {
        String name = instance.getName();
        List<InternalBucket> buckets = instance.getBuckets();
        Map<String, Object> metadata = instance.getMetadata();
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
            if (metadata == null) {
                metadata = new HashMap<>(1);
            } else {
                metadata = new HashMap<>(instance.getMetadata());
            }
            metadata.put(randomAlphaOfLength(15), randomInt());
            break;
        }
        return new InternalFilters(name, buckets, keyed, metadata);
    }

    public void testReducePipelinesReturnsSameInstanceWithoutPipelines() {
        InternalFilters test = createTestInstance();
        assertThat(test.reducePipelines(test, emptyReduceContextBuilder().forFinalReduction(), PipelineTree.EMPTY), sameInstance(test));
    }

    public void testReducePipelinesReducesBucketPipelines() {
        /*
         * Tests that a pipeline buckets by creating a mock pipeline that
         * replaces "inner" with "dummy".
         */
        InternalFilters dummy = createTestInstance();
        InternalFilters inner = createTestInstance();

        InternalAggregations sub = new InternalAggregations(List.of(inner));
        InternalFilters test = createTestInstance("test", emptyMap(), sub);
        PipelineAggregator mockPipeline = new PipelineAggregator(null, null, null) {
            @Override
            public InternalAggregation reduce(InternalAggregation aggregation, ReduceContext reduceContext) {
                return dummy;
            }
        };
        PipelineTree tree = new PipelineTree(Map.of(inner.getName(), new PipelineTree(emptyMap(), List.of(mockPipeline))), emptyList());
        InternalFilters reduced = (InternalFilters) test.reducePipelines(test, emptyReduceContextBuilder().forFinalReduction(), tree);
        for (InternalFilters.InternalBucket bucket : reduced.getBuckets()) {
            assertThat(bucket.getAggregations().get(dummy.getName()), sameInstance(dummy));
        }
    }
}
