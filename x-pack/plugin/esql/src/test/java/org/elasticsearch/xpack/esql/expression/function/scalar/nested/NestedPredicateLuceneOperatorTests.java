/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

/** Evaluates the same predicate through the nested query that is exactly equivalent to it. */
public class NestedPredicateLuceneOperatorTests extends NestedPredicateOperatorTestCase {
    @Override
    NestedPredicate predicate() {
        NestedQueryBuilder query = new NestedQueryBuilder("items", QueryBuilders.rangeQuery("items.price").gt(THRESHOLD), ScoreMode.None)
            .ignoreUnmapped(true);
        return nested(priceAboveThreshold(), query);
    }

    @Override
    boolean matches(int parent) {
        return highestPrice(parent) > THRESHOLD;
    }
}
