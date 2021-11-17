/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.queryableexpression;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;

import java.util.List;
import java.util.function.LongFunction;
import java.util.stream.Collectors;

/**
 * Approximates a script performing an unknown operation on a field with an `exists` query.
 */
public class UnknownOperationExpression implements QueryableExpression, LongQueryableExpression, StringQueryableExpression {

    private final List<QueryableExpression> args;

    public UnknownOperationExpression(QueryableExpression... args) {
        this.args = List.of(args);
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
    public StringQueryableExpression castToString() {
        return this;
    }

    @Override
    public QueryableExpression mapNumber(MapNumber map) {
        return this;
    }

    @Override
    public Query approximateExists() {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        args.stream().map(QueryableExpression::approximateExists).forEach(query -> builder.add(query, BooleanClause.Occur.MUST));
        return builder.build();
    }

    @Override
    public Query approximateTermQuery(long term) {
        return approximateExists();
    }

    @Override
    public Query approximateRangeQuery(long lower, long upper) {
        return approximateExists();
    }

    @Override
    public QueryableExpression mapConstant(LongFunction<QueryableExpression> map) {
        return this;
    }

    @Override
    public Query approximateTermQuery(String term) {
        return approximateExists();
    }

    @Override
    public Query approximateSubstringQuery(String term) {
        return approximateExists();
    }

    @Override
    public StringQueryableExpression substring() {
        return this;
    }

    @Override
    public String toString() {
        return "unknown(" + args.stream().map(Object::toString).collect(Collectors.joining(", ")) + ")";
    }
}
