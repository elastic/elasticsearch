/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.queryableexpression;

import org.apache.lucene.search.MatchAllDocsQuery;

import java.util.Map;
import java.util.function.Function;

/**
 * Delays building {@link QueryableExpression} until the mapping lookup and
 * script parameters are available.
 */
public interface DelayedQueryableExpression {
    QueryableExpression undelay(Function<String, QueryableExpression> lookup, Map<String, Object> params);

    /**
     * An expression that can not be queried, so it always returned
     * {@link MatchAllDocsQuery}.
     */
    DelayedQueryableExpression UNQUERYABLE = (lookup, params) -> UnqueryableExpression.UNQUERYABLE;

    static DelayedQueryableExpression constant(Object c) {
        return (lookup, params) -> QueryableExpression.constant(c);
    }

    static DelayedQueryableExpression field(String name) {
        return (lookup, params) -> lookup.apply(name);
    }

    static DelayedQueryableExpression param(String name) {
        return (lookup, params) -> QueryableExpression.constant(params.get(name));
    }

    static DelayedQueryableExpression add(DelayedQueryableExpression lhs, DelayedQueryableExpression rhs) {
        return (lookup, params) -> lhs.undelay(lookup, params).add(rhs.undelay(lookup, params));
    }

    static DelayedQueryableExpression subtract(DelayedQueryableExpression lhs, DelayedQueryableExpression rhs) {
        return UNQUERYABLE;  // TODO subtract for real
    }

    static DelayedQueryableExpression multiply(DelayedQueryableExpression lhs, DelayedQueryableExpression rhs) {
        return (lookup, params) -> lhs.undelay(lookup, params).multiply(rhs.undelay(lookup, params));
    }

    static DelayedQueryableExpression divide(DelayedQueryableExpression lhs, DelayedQueryableExpression rhs) {
        return (lookup, params) -> lhs.undelay(lookup, params).divide(rhs.undelay(lookup, params));
    }
}
