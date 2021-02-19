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
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public abstract class InternalTermsTestCase extends InternalMultiBucketAggregationTestCase<InternalTerms<?, ?>> {

    protected boolean showDocCount;
    protected long docCountError;

    @Before
    public void init() {
        showDocCount = randomBoolean();
        docCountError = showDocCount ? randomInt(1000) : -1;
    }

    @Override
    protected InternalTerms<?, ?> createTestInstance(String name, Map<String, Object> metadata, InternalAggregations aggregations) {
        return createTestInstance(name, metadata, aggregations, showDocCount, docCountError);
    }

    protected abstract InternalTerms<?, ?> createTestInstance(String name,
                                                              Map<String, Object> metadata,
                                                              InternalAggregations aggregations,
                                                              boolean showTermDocCountError,
                                                              long docCountError);

    @Override
    protected InternalTerms<?, ?> createUnmappedInstance(String name, Map<String, Object> metadata) {
        InternalTerms<?, ?> testInstance = createTestInstance(name, metadata);
        return new UnmappedTerms(name, testInstance.order, testInstance.requiredSize, testInstance.minDocCount, metadata);
    }

    @Override
    protected void assertReduced(InternalTerms<?, ?> reduced, List<InternalTerms<?, ?>> inputs) {
        final int requiredSize = inputs.get(0).requiredSize;
        Map<Object, Long> reducedCounts = toCounts(reduced.getBuckets().stream());
        Map<Object, Long> totalCounts = toCounts(inputs.stream().map(Terms::getBuckets).flatMap(List::stream));

        assertEquals(reducedCounts.size() == requiredSize,
                totalCounts.size() >= requiredSize);

        Map<Object, Long> expectedReducedCounts = new HashMap<>(totalCounts);
        expectedReducedCounts.keySet().retainAll(reducedCounts.keySet());
        assertEquals(expectedReducedCounts, reducedCounts);

        final long minFinalcount = reduced.getBuckets().isEmpty()
                ? -1
                : reduced.getBuckets().get(reduced.getBuckets().size() - 1).getDocCount();
        Map<Object, Long> evictedTerms = new HashMap<>(totalCounts);
        evictedTerms.keySet().removeAll(reducedCounts.keySet());
        Optional<Entry<Object, Long>> missingTerm = evictedTerms.entrySet().stream()
                .filter(e -> e.getValue() > minFinalcount).findAny();
        if (missingTerm.isPresent()) {
            fail("Missed term: " + missingTerm + " from " + reducedCounts);
        }

        final long reducedTotalDocCount = reduced.getSumOfOtherDocCounts()
                + reduced.getBuckets().stream().mapToLong(Terms.Bucket::getDocCount).sum();
        final long expectedTotalDocCount = inputs.stream().map(Terms::getBuckets)
                .flatMap(List::stream).mapToLong(Terms.Bucket::getDocCount).sum();
        assertEquals(expectedTotalDocCount, reducedTotalDocCount);
        for (InternalTerms<?, ?> terms : inputs) {
            assertThat(reduced.reduceOrder, equalTo(terms.order));
            assertThat(reduced.order, equalTo(terms.order));
        }
    }

    private static Map<Object, Long> toCounts(Stream<? extends Terms.Bucket> buckets) {
        return buckets.collect(Collectors.toMap(
                Terms.Bucket::getKey,
                Terms.Bucket::getDocCount,
                Long::sum));
    }
}
