/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.query;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.sort.NestedSortBuilder;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.Objects;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;

public class NotQuery extends Query {
    private final Query child;

    public NotQuery(Source source, Query child) {
        super(source);
        if (child == null) {
            throw new IllegalArgumentException("child is required");
        }
        this.child = child;
    }

    public Query child() {
        return child;
    }

    @Override
    public boolean containsNestedField(String path, String field) {
        return child.containsNestedField(path, field);
    }

    @Override
    public Query addNestedField(String path, String field, String format, boolean hasDocValues) {
        Query rewrittenChild = child.addNestedField(path, field, format, hasDocValues);
        if (child == rewrittenChild) {
            return this;
        }
        return new NotQuery(source(), child);
    }

    @Override
    public void enrichNestedSort(NestedSortBuilder sort) {
        child.enrichNestedSort(sort);
    }

    @Override
    public QueryBuilder asBuilder() {
        return boolQuery().mustNot(child.asBuilder());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), child.hashCode());
    }

    @Override
    public boolean equals(Object obj) {
        if (false == super.equals(obj)) {
            return false;
        }
        NotQuery other = (NotQuery) obj;
        return child.equals(other.child);
    }

    @Override
    protected String innerToString() {
        return child.toString();
    }
}
