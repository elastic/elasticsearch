/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
    protected final InternalRareTerms<?, ?> createTestInstance(
        String name,
        Map<String, Object> metadata,
        InternalAggregations aggregations
    ) {
        return createTestInstance(name, metadata, aggregations, maxDocCount);
    }

    protected abstract InternalRareTerms<?, ?> createTestInstance(
        String name,
        Map<String, Object> metadata,
        InternalAggregations aggregations,
        long maxDocCount
    );

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
        return buckets.collect(Collectors.toMap(RareTerms.Bucket::getKey, RareTerms.Bucket::getDocCount, Long::sum));
    }
}
