/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.queryableexpression;

import org.apache.lucene.search.MatchAllDocsQuery;

import java.util.function.Function;

/**
 * Delays building {@link QueryableExpression} until the mapping lookup and
 * script parameters are available.
 */
public interface QueryableExpressionBuilder {
    QueryableExpression build(Function<String, QueryableExpression> lookup, Function<String, Object> params);

    /**
     * An expression that can not be queried, so it always returned
     * {@link MatchAllDocsQuery}.
     */
    QueryableExpressionBuilder UNQUERYABLE = (lookup, params) -> UnqueryableExpression.UNQUERYABLE;

    static QueryableExpressionBuilder constant(Object c) {
        return (lookup, params) -> buildConstant(c);
    }

    static QueryableExpressionBuilder field(String name) {
        return (lookup, params) -> lookup.apply(name);
    }

    static QueryableExpressionBuilder unknownOp(QueryableExpressionBuilder child) {
        return ((lookup, params) -> child.build(lookup, params).unknownOp());
    }

    static QueryableExpressionBuilder unknownOp(QueryableExpressionBuilder lhs, QueryableExpressionBuilder rhs) {
        return (lookup, params) -> lhs.build(lookup, params).unknownOp(rhs.build(lookup, params));
    }

    static QueryableExpressionBuilder param(String name) {
        return (lookup, params) -> buildConstant(params.apply(name));
    }

    static QueryableExpressionBuilder add(QueryableExpressionBuilder lhs, QueryableExpressionBuilder rhs) {
        return (lookup, params) -> lhs.build(lookup, params).add(rhs.build(lookup, params));
    }

    static QueryableExpressionBuilder subtract(QueryableExpressionBuilder lhs, QueryableExpressionBuilder rhs) {
        return add(lhs, multiply(rhs, constant(-1)));
    }

    static QueryableExpressionBuilder multiply(QueryableExpressionBuilder lhs, QueryableExpressionBuilder rhs) {
        return (lookup, params) -> lhs.build(lookup, params).multiply(rhs.build(lookup, params));
    }

    static QueryableExpressionBuilder divide(QueryableExpressionBuilder lhs, QueryableExpressionBuilder rhs) {
        return (lookup, params) -> lhs.build(lookup, params).divide(rhs.build(lookup, params));
    }

    static QueryableExpressionBuilder substring(QueryableExpressionBuilder target) {
        return (lookup, params) -> target.build(lookup, params).castToString().substring();
    }

    private static QueryableExpression buildConstant(Object c) {
        if (c instanceof Long) {
            return new AbstractLongQueryableExpression.Constant((Long) c);
        }
        if (c instanceof Integer) {
            return new IntConstant((Integer) c);
        }
        return QueryableExpression.UNQUERYABLE;
    }
}
