/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.aggregations.bucket.adjacency;

import org.elasticsearch.aggregations.bucket.AggregationBuilderTestCase;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.HashMap;
import java.util.Map;

public class AdjacencyMatrixTests extends AggregationBuilderTestCase<AdjacencyMatrixAggregationBuilder> {

    @Override
    protected AdjacencyMatrixAggregationBuilder createTestAggregatorBuilder() {

        int size = randomIntBetween(1, 20);
        AdjacencyMatrixAggregationBuilder factory;
        Map<String, QueryBuilder> filters = Maps.newMapWithExpectedSize(size);
        for (String key : randomUnique(() -> randomAlphaOfLengthBetween(1, 20), size)) {
            filters.put(key, QueryBuilders.termQuery(randomAlphaOfLengthBetween(5, 20), randomAlphaOfLengthBetween(5, 20)));
        }
        factory = new AdjacencyMatrixAggregationBuilder(randomAlphaOfLengthBetween(1, 20), filters).separator(randomFrom("&", "+", "\t"));
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
