/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.queryableexpression;

import org.apache.lucene.search.Query;

public abstract class AbstractStringQueryableExpression implements StringQueryableExpression {
    @Override
    public QueryableExpression add(QueryableExpression rhs) {
        return UnqueryableExpression.UNQUERYABLE;
    }

    @Override
    public QueryableExpression multiply(QueryableExpression rhs) {
        return UnqueryableExpression.UNQUERYABLE;
    }

    @Override
    public QueryableExpression divide(QueryableExpression rhs) {
        return UnqueryableExpression.UNQUERYABLE;
    }

    @Override
    public final LongQueryableExpression castToLong() {
        return UnqueryableExpression.UNQUERYABLE;
    }

    @Override
    public final StringQueryableExpression castToString() {
        return this;
    }

    @Override
    public QueryableExpression mapNumber(MapNumber map) {
        return UNQUERYABLE;
    }

    @Override
    public abstract String toString();

    /**
     * A queryable field who's values are exposed to the script as {@link String}s.
     */
    static class Field extends AbstractStringQueryableExpression {
        private final String name;
        private final Queries queries;

        Field(String name, Queries queries) {
            this.name = name;
            this.queries = queries;
        }

        @Override
        public QueryableExpression unknownOp() {
            return new UnknownOperationExpression(this);
        }

        @Override
        public QueryableExpression unknownOp(QueryableExpression rhs) {
            return new UnknownOperationExpression(this, rhs);
        }

        @Override
        public Query approximateExists() {
            return queries.approximateExists();
        }

        @Override
        public Query approximateTermQuery(String term) {
            return queries.approximateTermQuery(term);
        }

        @Override
        public Query approximateSubstringQuery(String term) {
            return queries.approximateSubstringQuery(term);
        }

        @Override
        public StringQueryableExpression substring() {
            return new Substring(this);
        }

        @Override
        public String toString() {
            return name;
        }
    }

    /**
     * Models substring operations by looking for any matching substring.
     */
    static class Substring extends AbstractStringQueryableExpression {
        private final StringQueryableExpression next;

        Substring(StringQueryableExpression next) {
            this.next = next;
        }

        @Override
        public QueryableExpression unknownOp() {
            return next.unknownOp();
        }

        @Override
        public QueryableExpression unknownOp(QueryableExpression rhs) {
            return next.unknownOp(rhs);
        }

        @Override
        public Query approximateExists() {
            return next.approximateExists();
        }

        @Override
        public Query approximateTermQuery(String term) {
            return next.approximateSubstringQuery(term);
        }

        @Override
        public Query approximateSubstringQuery(String term) {
            return next.approximateSubstringQuery(term);
        }

        @Override
        public StringQueryableExpression substring() {
            return new Substring(this);
        }

        @Override
        public String toString() {
            return next + ".substring()";
        }
    }
}
