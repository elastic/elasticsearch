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

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.BaseAggregationTestCase;
import org.elasticsearch.search.aggregations.bucket.adjacency.AdjacencyMatrixAggregationBuilder;

import java.util.HashMap;
import java.util.Map;

public class AdjacencyMatrixTests extends BaseAggregationTestCase<AdjacencyMatrixAggregationBuilder> {

    @Override
    protected AdjacencyMatrixAggregationBuilder createTestAggregatorBuilder() {

        int size = randomIntBetween(1, 20);
        AdjacencyMatrixAggregationBuilder factory;
        Map<String, QueryBuilder> filters = new HashMap<>(size);
        for (String key : randomUnique(() -> randomAlphaOfLengthBetween(1, 20), size)) {
            filters.put(key, QueryBuilders.termQuery(randomAlphaOfLengthBetween(5, 20), randomAlphaOfLengthBetween(5, 20)));
        }
        factory = new AdjacencyMatrixAggregationBuilder(randomAlphaOfLengthBetween(1, 20), filters)
                .separator(randomFrom("&","+","\t"));       
        return factory;
    }

    /**
     * Test that when passing in keyed filters as a map they are equivalent
     */
    public void testFiltersSameMap() {
        Map<String, QueryBuilder> original = new HashMap<>();
        original.put("bbb", new MatchNoneQueryBuilder());
        original.put("aaa", new MatchNoneQueryBuilder());
        AdjacencyMatrixAggregationBuilder builder;
        builder = new AdjacencyMatrixAggregationBuilder("my-agg", original);
        assertEquals(original, builder.filters());
        assert original != builder.filters();
    }
}
