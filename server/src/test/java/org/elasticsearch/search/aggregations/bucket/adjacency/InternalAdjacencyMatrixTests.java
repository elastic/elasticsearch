/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.adjacency;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.test.InternalMultiBucketAggregationTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class InternalAdjacencyMatrixTests extends InternalMultiBucketAggregationTestCase<InternalAdjacencyMatrix> {

    private List<String> keys;

    @Override
    protected boolean supportsSampling() {
        return true;
    }

    @Override
    protected int maxNumberOfBuckets() {
        return 10;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        keys = new ArrayList<>();
        // InternalAdjacencyMatrix represents the upper triangular matrix:
        // 2 filters (matrix of 2x2) generates 3 buckets
        // 3 filters generates 6 buckets
        // 4 filters generates 10 buckets
        int numFilters = randomIntBetween(2, 4);
        String[] filters = new String[numFilters];
        for (int i = 0; i < numFilters; i++) {
            filters[i] = randomAlphaOfLength(5);
        }
        for (int i = 0; i < filters.length; i++) {
            keys.add(filters[i]);
            for (int j = i + 1; j < filters.length; j++) {
                if (filters[i].compareTo(filters[j]) <= 0) {
                    keys.add(filters[i] + "&" + filters[j]);
                } else {
                    keys.add(filters[j] + "&" + filters[i]);
                }
            }
        }
    }

    @Override
    protected InternalAdjacencyMatrix createTestInstance(String name, Map<String, Object> metadata, InternalAggregations aggregations) {
        final List<InternalAdjacencyMatrix.InternalBucket> buckets = new ArrayList<>();
        for (int i = 0; i < keys.size(); ++i) {
            String key = keys.get(i);
            int docCount = randomIntBetween(0, 1000);
            buckets.add(new InternalAdjacencyMatrix.InternalBucket(key, docCount, aggregations));
        }
        return new InternalAdjacencyMatrix(name, buckets, metadata);
    }

    @Override
    protected void assertReduced(InternalAdjacencyMatrix reduced, List<InternalAdjacencyMatrix> inputs) {
        final Map<String, Long> expectedCounts = new TreeMap<>();
        for (InternalAdjacencyMatrix input : inputs) {
            for (InternalAdjacencyMatrix.InternalBucket bucket : input.getBuckets()) {
                if (bucket.getDocCount() > 0) {
                    expectedCounts.compute(
                        bucket.getKeyAsString(),
                        (key, oldValue) -> (oldValue == null ? 0 : oldValue) + bucket.getDocCount()
                    );
                }
            }
        }
        final Map<String, Long> actualCounts = new TreeMap<>();
        for (InternalAdjacencyMatrix.InternalBucket bucket : reduced.getBuckets()) {
            actualCounts.compute(bucket.getKeyAsString(), (key, oldValue) -> (oldValue == null ? 0 : oldValue) + bucket.getDocCount());
        }
        assertEquals(expectedCounts, actualCounts);
    }

    @Override
    protected Class<ParsedAdjacencyMatrix> implementationClass() {
        return ParsedAdjacencyMatrix.class;
    }

    @Override
    protected InternalAdjacencyMatrix mutateInstance(InternalAdjacencyMatrix instance) {
        String name = instance.getName();
        List<InternalAdjacencyMatrix.InternalBucket> buckets = instance.getBuckets();
        Map<String, Object> metadata = instance.getMetadata();
        switch (between(0, 2)) {
            case 0 -> name += randomAlphaOfLength(5);
            case 1 -> {
                buckets = new ArrayList<>(buckets);
                buckets.add(
                    new InternalAdjacencyMatrix.InternalBucket(randomAlphaOfLength(10), randomNonNegativeLong(), InternalAggregations.EMPTY)
                );
            }
            case 2 -> {
                if (metadata == null) {
                    metadata = Maps.newMapWithExpectedSize(1);
                } else {
                    metadata = new HashMap<>(instance.getMetadata());
                }
                metadata.put(randomAlphaOfLength(15), randomInt());
            }
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new InternalAdjacencyMatrix(name, buckets, metadata);
    }
}
