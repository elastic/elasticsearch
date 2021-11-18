/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.queryableexpression;

import org.apache.lucene.search.Query;

import java.util.function.LongFunction;

/**
 * {@code long} flavored expression.
 */
public interface LongQueryableExpression extends QueryableExpression {

    interface LongQueries {
        Query approximateExists();

        Query approximateTermQuery(long term);

        Query approximateRangeQuery(long lower, long upper);
    }

    static LongQueryableExpression field(String name, LongQueries queries) {
        return new AbstractLongQueryableExpression.Field(name, queries);
    }

    interface IntQueries {
        Query approximateExists();

        Query approximateTermQuery(int term);

        Query approximateRangeQuery(int lower, int upper);
    }

    static LongQueryableExpression field(String name, IntQueries queries) {
        return field(name, new AbstractLongQueryableExpression.IntQueriesToLongQueries(queries));
    }

    /**
     * Build a query that approximates a term query on this expression.
     */
    Query approximateTermQuery(long term);

    /**
     * Build a query that approximates a range query on this expression.
     */
    Query approximateRangeQuery(long lower, long upper);

    /**
     * Transform this expression if it is a constant or return
     * {@link UnqueryableExpression} if it is not.
     */
    QueryableExpression mapConstant(LongFunction<QueryableExpression> map);
}
