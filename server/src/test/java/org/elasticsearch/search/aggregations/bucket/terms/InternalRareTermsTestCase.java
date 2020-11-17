/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.test.InternalMultiBucketAggregationTestCase;
import org.junit.Before;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class InternalRareTermsTestCase extends InternalMultiBucketAggregationTestCase<InternalRareTerms<?, ?>> {

    private long maxDocCount;

    @Before
    public void init() {
        maxDocCount = randomIntBetween(1, 5);
    }

    @Override
    protected final InternalRareTerms<?, ?> createTestInstance(String name,
                                                               Map<String, Object> metadata,
                                                               InternalAggregations aggregations) {
        return createTestInstance(name, metadata, aggregations, maxDocCount);
    }

    protected abstract InternalRareTerms<?, ?> createTestInstance(String name,
                                                                  Map<String, Object> metadata,
                                                                  InternalAggregations aggregations,
                                                                  long maxDocCount);

    @Override
    protected InternalRareTerms<?, ?> createUnmappedInstance(String name, Map<String, Object> metadata) {
        return new UnmappedRareTerms(name, metadata);
    }

    @Override
    protected void assertReduced(InternalRareTerms<?, ?> reduced, List<InternalRareTerms<?, ?>> inputs) {
        Map<Object, Long> reducedCounts = toCounts(reduced.getBuckets().stream());
        Map<Object, Long> totalCounts = toCounts(inputs.stream().map(RareTerms::getBuckets).flatMap(List::stream));

        Map<Object, Long> expectedReducedCounts = new HashMap<>(totalCounts);
        expectedReducedCounts.keySet().retainAll(reducedCounts.keySet());
        assertEquals(expectedReducedCounts, reducedCounts);
    }

    private static Map<Object, Long> toCounts(Stream<? extends RareTerms.Bucket> buckets) {
        return buckets.collect(Collectors.toMap(
            RareTerms.Bucket::getKey,
            RareTerms.Bucket::getDocCount,
            Long::sum));
    }
}
