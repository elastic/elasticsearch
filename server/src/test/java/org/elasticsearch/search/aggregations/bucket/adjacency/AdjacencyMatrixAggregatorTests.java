/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.adjacency;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.List;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class AdjacencyMatrixAggregatorTests extends AggregatorTestCase {
    public void testTooManyFilters() throws Exception {
        int maxFilters = IndexSettings.MAX_ADJACENCY_MATRIX_FILTERS_SETTING.get(Settings.EMPTY);
        int maxFiltersPlusOne = maxFilters + 1;

        Map<String, QueryBuilder> filters = new HashMap<>(maxFilters);
        for (int i = 0; i < maxFiltersPlusOne; i++) {
            filters.put("filter" + i, new MatchAllQueryBuilder());
        }
        AdjacencyMatrixAggregationBuilder tooBig = new AdjacencyMatrixAggregationBuilder("dummy", filters);
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> testCase(tooBig, new MatchAllDocsQuery(), iw -> {}, r -> {})
        );
        assertThat(
            ex.getMessage(),
            equalTo(
                "Number of filters is too large, must be less than or equal to: ["
                    + maxFilters
                    + "] but was ["
                    + maxFiltersPlusOne
                    + "]."
                    + "This limit can be set by changing the ["
                    + IndexSettings.MAX_ADJACENCY_MATRIX_FILTERS_SETTING.getKey()
                    + "] index level setting."
            )
        );
    }

    public void testNoFilters() throws IOException {
        AdjacencyMatrixAggregationBuilder aggregationBuilder = new AdjacencyMatrixAggregationBuilder(
            "dummy",
            org.elasticsearch.core.Map.of()
        );
        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> iw.addDocument(List.of()), r -> {
            InternalAdjacencyMatrix result = (InternalAdjacencyMatrix) r;
            assertThat(result.getBuckets(), equalTo(List.of()));
        });
    }

    public void testAFewFilters() throws IOException {
        AdjacencyMatrixAggregationBuilder aggregationBuilder = new AdjacencyMatrixAggregationBuilder(
            "dummy",
            org.elasticsearch.core.Map.of("a", new MatchAllQueryBuilder(), "b", new MatchAllQueryBuilder())
        );
        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> iw.addDocument(List.of()), r -> {
            InternalAdjacencyMatrix result = (InternalAdjacencyMatrix) r;
            assertThat(result.getBuckets(), hasSize(3));
            InternalAdjacencyMatrix.InternalBucket a = result.getBucketByKey("a");
            InternalAdjacencyMatrix.InternalBucket b = result.getBucketByKey("b");
            InternalAdjacencyMatrix.InternalBucket ab = result.getBucketByKey("a&b");
            assertThat(a.getDocCount(), equalTo(1L));
            assertThat(b.getDocCount(), equalTo(1L));
            assertThat(ab.getDocCount(), equalTo(1L));
        });
    }
}
