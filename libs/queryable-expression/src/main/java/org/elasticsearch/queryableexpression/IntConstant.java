/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.queryableexpression;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;

import java.util.function.IntFunction;

/**
 * Constant {@link IntQueryableExpression}.
 */
class IntConstant implements QueryableExpression, IntQueryableExpression {
    private final int n;

    IntConstant(int n) {
        this.n = n;
    }

    @Override
    public Query approximateExists() {
        return new MatchAllDocsQuery();
    }

    @Override
    public QueryableExpression unknownOp() {
        return UNQUERYABLE;
    }

    @Override
    public QueryableExpression unknownOp(QueryableExpression rhs) {
        return rhs.unknownOp();
    }

    @Override
    public QueryableExpression add(QueryableExpression rhs) {
        return rhs.mapNumber(new MapNumber() {
            @Override
            public QueryableExpression withLong(LongQueryableExpression lqe) {
                return castToLong().add(lqe);
            }

            @Override
            public QueryableExpression withInt(IntQueryableExpression iqe) {
                return iqe.mapConstant(c -> new IntConstant(n + c));
            }
        });
    }

    @Override
    public QueryableExpression multiply(QueryableExpression rhs) {
        return rhs.mapNumber(new MapNumber() {
            @Override
            public QueryableExpression withLong(LongQueryableExpression lqe) {
                return castToLong().multiply(lqe);
            }

            @Override
            public QueryableExpression withInt(IntQueryableExpression iqe) {
                return iqe.mapConstant(c -> new IntConstant(n * c));
            }
        });
    }

    @Override
    public QueryableExpression divide(QueryableExpression rhs) {
        return rhs.mapNumber(new MapNumber() {
            @Override
            public QueryableExpression withLong(LongQueryableExpression lqe) {
                return castToLong().divide(lqe);
            }

            @Override
            public QueryableExpression withInt(IntQueryableExpression iqe) {
                return iqe.mapConstant(c -> new IntConstant(n / c));
            }
        });
    }

    @Override
    public LongQueryableExpression castToLong() {
        return new AbstractLongQueryableExpression.Constant(n);
    }

    @Override
    public StringQueryableExpression castToString() {
        return UnqueryableExpression.UNQUERYABLE;
    }

    @Override
    public QueryableExpression mapConstant(IntFunction<QueryableExpression> map) {
        return map.apply(n);
    }

    @Override
    public QueryableExpression mapNumber(MapNumber map) {
        return map.withInt(this);
    }

    @Override
    public String toString() {
        return Integer.toString(n);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        IntConstant other = (IntConstant) obj;
        return n == other.n;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(n);
    }
}
