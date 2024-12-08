/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.querydsl.query;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.sort.NestedSortBuilder;
import org.elasticsearch.xpack.ql.tree.Source;

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
     * Does this query contain a particular nested field?
     */
    public abstract boolean containsNestedField(String path, String field);

    /**
     * Rewrite this query to one that contains the specified nested field.
     * <p>
     * Used to make sure that we fetch nested fields even if they aren't
     * explicitly part of the query.
     * @return a new query if we could add the nested field, the same query
     *      instance otherwise
     */
    public abstract Query addNestedField(String path, String field, String format, boolean hasDocValues);

    /**
     * Attach the one and only one matching nested query's filter to this
     * sort.
     */
    public abstract void enrichNestedSort(NestedSortBuilder sort);

    /**
     * Convert to an Elasticsearch {@link QueryBuilder} all set up to execute
     * the query.
     */
    public abstract QueryBuilder asBuilder();

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
}
