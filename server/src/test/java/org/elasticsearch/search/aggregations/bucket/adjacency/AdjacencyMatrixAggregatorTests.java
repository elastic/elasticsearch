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

package org.elasticsearch.search.aggregations.bucket.adjacency;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.AggregatorTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class AdjacencyMatrixAggregatorTests extends AggregatorTestCase {
    public void testTooManyFilters() throws Exception {
        int maxFilters = SearchModule.INDICES_MAX_CLAUSE_COUNT_SETTING.get(Settings.EMPTY);
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
                    + SearchModule.INDICES_MAX_CLAUSE_COUNT_SETTING.getKey()
                    + "] setting."
            )
        );
    }

    public void testNoFilters() throws IOException {
        AdjacencyMatrixAggregationBuilder aggregationBuilder = new AdjacencyMatrixAggregationBuilder("dummy", Map.of());
        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> iw.addDocument(List.of()), r -> {
            InternalAdjacencyMatrix result = (InternalAdjacencyMatrix) r;
            assertThat(result.getBuckets(), equalTo(List.of()));
        });
    }

    public void testAFewFilters() throws IOException {
        AdjacencyMatrixAggregationBuilder aggregationBuilder = new AdjacencyMatrixAggregationBuilder(
            "dummy",
            Map.of("a", new MatchAllQueryBuilder(), "b", new MatchAllQueryBuilder())
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
