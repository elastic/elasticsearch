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

import java.util.function.LongFunction;

public abstract class QueryableExpression {
    /**
     * An expression that can not be queried, so it always returned
     * {@link MatchAllDocsQuery}.
     */
    public static final QueryableExpression UNQUERYABLE = new Unqueryable();

    public static QueryableExpression constant(long c) {
        return new LongQueryableExpression.Constant(c);
    }

    public abstract QueryableExpression add(QueryableExpression rhs);

    public abstract QueryableExpression multiply(QueryableExpression rhs);

    public abstract QueryableExpression divide(QueryableExpression rhs);

    public abstract Query approximateTermQuery(long term);

    public abstract Query approximateRangeQuery(long lower, long upper);

    protected abstract QueryableExpression asConstantLong(LongFunction<QueryableExpression> map);

    @Override
    public abstract String toString();

    private static class Unqueryable extends QueryableExpression {
        private Unqueryable() {}

        @Override
        protected final QueryableExpression asConstantLong(LongFunction<QueryableExpression> map) {
            return UNQUERYABLE;
        }

        @Override
        public QueryableExpression add(QueryableExpression rhs) {
            return UNQUERYABLE;
        }

        @Override
        public QueryableExpression multiply(QueryableExpression rhs) {
            return UNQUERYABLE;
        }

        @Override
        public QueryableExpression divide(QueryableExpression rhs) {
            return UNQUERYABLE;
        }

        @Override
        public Query approximateTermQuery(long term) {
            return new MatchAllDocsQuery();
        }

        @Override
        public Query approximateRangeQuery(long lower, long upper) {
            return new MatchAllDocsQuery();
        }

        @Override
        public String toString() {
            return getClass().getSimpleName();
        }
    }

    abstract static class Chain extends QueryableExpression {
        protected final QueryableExpression next;

        Chain(QueryableExpression next) {
            this.next = next;
        }

        @Override
        protected QueryableExpression asConstantLong(LongFunction<QueryableExpression> map) {
            return UNQUERYABLE;
        }
    }
}
