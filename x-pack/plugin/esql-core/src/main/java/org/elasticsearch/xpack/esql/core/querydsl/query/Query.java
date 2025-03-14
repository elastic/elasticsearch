/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.querydsl.query;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * Intermediate representation of queries that is rewritten to fetch
 * otherwise unreferenced nested fields and then used to build
 * Elasticsearch {@link QueryBuilder}s.
 * <p>
 *     Our expression language spits out one of three values for any
 *     comparison, {@code true}, {@code false}, and {@code null}.
 *     Lucene's queries either match or don't match. They don't have
 *     a concept of {@code null}, at least not in the sense we need.
 *     The Lucene queries produced by {@link #asBuilder()} produce
 *     queries that do not match documents who's comparison would
 *     return {@code null}. This is what we want in {@code WHERE}
 *     style operations. But when you need to negate the matches you
 *     need to make only {@code false} return values into matches -
 *     {@code null} returns should continue to not match. You can
 *     do that with the {@link #negate} method.
 * </p>
 */
public abstract class Query {

    // Boosting used to remove scoring from queries that don't contribute to it
    public static final float NO_SCORE_BOOST = 0.0f;

    private final Source source;

    protected Query(Source source) {
        if (source == null) {
            throw new IllegalArgumentException("location must be specified");
        }
        this.source = source;
    }

    /**
     * Location in the source statement.
     */
    public Source source() {
        return source;
    }

    /**
     * Convert to an Elasticsearch {@link QueryBuilder} all set up to execute
     * the query. This ensures that queries have appropriate boosting for scoring.
     */
    public final QueryBuilder toQueryBuilder() {
        QueryBuilder builder = asBuilder();
        if (scorable() == false) {
            builder = unscore(builder);
        }
        return builder;
    }

    /**
     * Used internally to convert to retrieve a {@link QueryBuilder} by queries.
     */
    protected abstract QueryBuilder asBuilder();

    /**
     * Used by {@link Query#toString()} to produce a pretty string.
     */
    protected abstract String innerToString();

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        Query other = (Query) obj;
        return source.equals(other.source);
    }

    @Override
    public int hashCode() {
        return source.hashCode();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + source + "[" + innerToString() + "]";
    }

    /**
     * Negate this query, returning a query that includes documents that would
     * return {@code false} when running the represented operation. The default
     * implementation just returns a {@link NotQuery} wrapping {@code this} because
     * most queries don't model underlying operations that can return {@code null}.
     * Queries that model expressions that can return {@code null} must make sure
     * all documents that would return {@code null} are still excluded from the match.
     */
    public Query negate(Source source) {
        return new NotQuery(source, this);
    }

    /**
     * Defines whether a query should contribute to the overall score
     */
    public boolean scorable() {
        return false;
    }

    /**
     * Removes score from a query builder, so score is not affected by the query
     */
    public static QueryBuilder unscore(QueryBuilder builder) {
        return builder.boost(NO_SCORE_BOOST);
    }
}
