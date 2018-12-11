/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.query;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.sort.NestedSortBuilder;
import org.elasticsearch.xpack.sql.tree.Location;

/**
 * Intermediate representation of queries that is rewritten to fetch
 * otherwise unreferenced nested fields and then used to build
 * Elasticsearch {@link QueryBuilder}s.
 */
public abstract class Query {
    private final Location location;

    Query(Location location) {
        if (location == null) {
            throw new IllegalArgumentException("location must be specified");
        }
        this.location = location;
    }

    /**
     * Location in the source statement.
     */
    public Location location() {
        return location;
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
        return location.equals(other.location);
    }

    @Override
    public int hashCode() {
        return location.hashCode();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + location + "[" + innerToString() + "]";
    }
}
