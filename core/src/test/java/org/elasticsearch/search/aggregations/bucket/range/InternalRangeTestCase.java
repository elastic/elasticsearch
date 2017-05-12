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

package org.elasticsearch.search.aggregations.bucket.range;

import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregationTestCase;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.junit.Before;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public abstract class InternalRangeTestCase<T extends InternalAggregation & Range> extends InternalMultiBucketAggregationTestCase<T> {

    private boolean keyed;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        keyed = randomBoolean();
    }

    @Override
    protected T createTestInstance(String name,
                                   List<PipelineAggregator> pipelineAggregators,
                                   Map<String, Object> metaData,
                                   InternalAggregations aggregations) {
        return createTestInstance(name, pipelineAggregators, metaData, aggregations, keyed);
    }

    protected abstract T createTestInstance(String name,
                                            List<PipelineAggregator> pipelineAggregators,
                                            Map<String, Object> metaData,
                                            InternalAggregations aggregations,
                                            boolean keyed);
    @Override
    protected void assertReduced(T reduced, List<T> inputs) {
        final Map<String, Long> expectedCounts = new TreeMap<>();
        for (T input : inputs) {
            for (Range.Bucket bucket : input.getBuckets()) {
                expectedCounts.compute(bucket.getKeyAsString(),
                        (key, oldValue) -> (oldValue == null ? 0 : oldValue) + bucket.getDocCount());

            }
        }
        final Map<String, Long> actualCounts = new TreeMap<>();
        for (Range.Bucket bucket : reduced.getBuckets()) {
            actualCounts.compute(bucket.getKeyAsString(),
                    (key, oldValue) -> (oldValue == null ? 0 : oldValue) + bucket.getDocCount());
        }
        assertEquals(expectedCounts, actualCounts);
    }

    @Override
    protected void assertBucket(MultiBucketsAggregation.Bucket expected, MultiBucketsAggregation.Bucket actual, boolean checkOrder) {
        super.assertBucket(expected, actual, checkOrder);

        assertTrue(expected instanceof InternalRange.Bucket);
        assertTrue(actual instanceof ParsedRange.ParsedBucket);

        Range.Bucket expectedRange = (Range.Bucket) expected;
        Range.Bucket actualRange = (Range.Bucket) actual;

        assertEquals(expectedRange.getFrom(), actualRange.getFrom());
        assertEquals(expectedRange.getFromAsString(), actualRange.getFromAsString());
        assertEquals(expectedRange.getTo(), actualRange.getTo());
        assertEquals(expectedRange.getToAsString(), actualRange.getToAsString());
    }
}
