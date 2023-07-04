/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.aggregations.bucket.adjacency;

import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.aggregations.bucket.AggregationTestCase;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.startsWith;

public class AdjacencyMatrixAggregatorTests extends AggregationTestCase {

    public void testTooManyFilters() {
        int maxFilters = IndexSearcher.getMaxClauseCount();
        int maxFiltersPlusOne = maxFilters + 1;

        Map<String, QueryBuilder> filters = Maps.newMapWithExpectedSize(maxFilters);
        for (int i = 0; i < maxFiltersPlusOne; i++) {
            filters.put("filter" + i, new MatchAllQueryBuilder());
        }
        AdjacencyMatrixAggregationBuilder tooBig = new AdjacencyMatrixAggregationBuilder("dummy", filters);
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> testCase(iw -> {}, r -> {}, new AggTestConfig(tooBig))
        );
        assertThat(
            ex.getMessage(),
            startsWith(
                "Number of filters is too large, must be less than or equal to: [" + maxFilters + "] but was [" + maxFiltersPlusOne + "]."
            )
        );
    }

    public void testNoFilters() throws IOException {
        AdjacencyMatrixAggregationBuilder aggregationBuilder = new AdjacencyMatrixAggregationBuilder("dummy", Map.of());
        testCase(iw -> iw.addDocument(List.of()), r -> {
            InternalAdjacencyMatrix result = (InternalAdjacencyMatrix) r;
            assertThat(result.getBuckets(), equalTo(List.of()));
        }, new AggTestConfig(aggregationBuilder));
    }

    public void testAFewFilters() throws IOException {
        AdjacencyMatrixAggregationBuilder aggregationBuilder = new AdjacencyMatrixAggregationBuilder(
            "dummy",
            Map.of("a", new MatchAllQueryBuilder(), "b", new MatchAllQueryBuilder())
        );
        testCase(iw -> iw.addDocument(List.of()), r -> {
            InternalAdjacencyMatrix result = (InternalAdjacencyMatrix) r;
            assertThat(result.getBuckets(), hasSize(3));
            InternalAdjacencyMatrix.InternalBucket a = result.getBucketByKey("a");
            InternalAdjacencyMatrix.InternalBucket b = result.getBucketByKey("b");
            InternalAdjacencyMatrix.InternalBucket ab = result.getBucketByKey("a&b");
            assertThat(a.getDocCount(), equalTo(1L));
            assertThat(b.getDocCount(), equalTo(1L));
            assertThat(ab.getDocCount(), equalTo(1L));
        }, new AggTestConfig(aggregationBuilder));
    }
}
