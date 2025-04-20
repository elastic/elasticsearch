/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.querydsl.query;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.Objects;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;

/**
 * Query that inverts the set of matched documents.
 */
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
    protected QueryBuilder asBuilder() {
        return boolQuery().mustNot(child.toQueryBuilder());
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

    @Override
    public Query negate(Source source) {
        return child;
    }
}
