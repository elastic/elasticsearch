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
 * Approximates a script performing an unknown operation on a field with an `exists` query.
 */
public class UnknownOperation implements QueryableExpression, LongQueryableExpression {

    private final Queries queries;
    private final String field;

    public UnknownOperation(String field, Queries queries) {
        this.field = field;
        this.queries = queries;
    }

    @Override
    public QueryableExpression unknownOp() {
        return this;
    }

    @Override
    public QueryableExpression unknownOp(QueryableExpression rhs) {
        return this;
    }

    @Override
    public QueryableExpression add(QueryableExpression rhs) {
        return this;
    }

    @Override
    public QueryableExpression multiply(QueryableExpression rhs) {
        return this;
    }

    @Override
    public QueryableExpression divide(QueryableExpression rhs) {
        return this;
    }

    @Override
    public LongQueryableExpression castToLong() {
        return this;
    }

    @Override
    public QueryableExpression mapNumber(MapNumber map) {
        return this;
    }

    @Override
    public Query approximateTermQuery(long term) {
        return queries.approximateExists();
    }

    @Override
    public Query approximateRangeQuery(long lower, long upper) {
        return queries.approximateExists();
    }

    @Override
    public QueryableExpression mapConstant(LongFunction<QueryableExpression> map) {
        return this;
    }

    @Override
    public String toString() {
        return "unknown(" + field + ")";
    }
}
