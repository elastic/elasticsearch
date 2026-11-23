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
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

/**
 * Evaluates the predicate through the nested query that is exactly equivalent to it. The body is false for every child, so
 * only the nested query can produce a match: a predicate that ignored it would fail every parent that is expected to match.
 */
public class NestedPredicateLuceneOperatorTests extends NestedPredicateOperatorTestCase {
    @Override
    NestedPredicate predicate() {
        NestedQueryBuilder query = new NestedQueryBuilder("items", QueryBuilders.rangeQuery("items.price").gt(THRESHOLD), ScoreMode.None)
            .ignoreUnmapped(true);
        return nested(priceAbove(new Literal(Source.EMPTY, Long.MAX_VALUE, DataType.LONG)), query);
    }
}
