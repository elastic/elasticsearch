/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.querydsl.query;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.Objects;

/**
 * Matches the parents that have at least one {@code nested} object matching {@code child}. A shard that does not map
 * {@code path} has no such parent, so it matches nothing rather than failing.
 */
public class NestedQuery extends Query {
    private final String path;
    private final Query child;
    private final ScoreMode scoreMode;

    public NestedQuery(Source source, String path, Query child, ScoreMode scoreMode) {
        super(source);
        this.path = Objects.requireNonNull(path, "path is required");
        this.child = Objects.requireNonNull(child, "child is required");
        this.scoreMode = Objects.requireNonNull(scoreMode, "scoreMode is required");
    }

    public String path() {
        return path;
    }

    public Query child() {
        return child;
    }

    public ScoreMode scoreMode() {
        return scoreMode;
    }

    @Override
    protected QueryBuilder asBuilder() {
        return QueryBuilders.nestedQuery(path, child.toQueryBuilder(), scoreMode).ignoreUnmapped(true);
    }

    @Override
    public boolean scorable() {
        return scoreMode != ScoreMode.None && child.scorable();
    }

    @Override
    public boolean containsPlan() {
        return child.containsPlan();
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), path, child, scoreMode);
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj)
            && obj instanceof NestedQuery other
            && path.equals(other.path)
            && child.equals(other.child)
            && scoreMode == other.scoreMode;
    }

    @Override
    protected String innerToString() {
        return "nested(" + path + "[" + scoreMode + "], " + child + ")";
    }
}
