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
 * A query that scopes an inner query to the objects of a {@code nested} field, i.e. the ES|QL translation
 * of {@code NESTED_ANY(path, u -> predicate)}. It wraps the translated predicate in an Elasticsearch
 * {@code nested} query on {@code path}.
 * <p>
 * When several nested objects of the same parent match, their scores are reduced to the parent's score by
 * {@link ScoreMode}. The default is {@link ScoreMode#Max} — the existential semantics of {@code NESTED_ANY}
 * ("at least one object matches") map most naturally to "how well does the best-matching object match",
 * rather than Elasticsearch's own {@code nested}-query default of {@code Avg}. Scoring is transparent: this
 * wrapper is scorable only when its child is and the mode is not {@link ScoreMode#None}, so in a pure filter
 * context it collapses to no score.
 */
public class NestedQuery extends Query {

    public static final ScoreMode DEFAULT_SCORE_MODE = ScoreMode.Max;

    private final String path;
    private final Query child;
    private final ScoreMode scoreMode;

    public NestedQuery(Source source, String path, Query child, ScoreMode scoreMode) {
        super(source);
        if (path == null) {
            throw new IllegalArgumentException("path is required");
        }
        if (child == null) {
            throw new IllegalArgumentException("child is required");
        }
        if (scoreMode == null) {
            throw new IllegalArgumentException("scoreMode is required");
        }
        this.path = path;
        this.child = child;
        this.scoreMode = scoreMode;
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
        return QueryBuilders.nestedQuery(path, child.toQueryBuilder(), scoreMode);
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
        if (false == super.equals(obj)) {
            return false;
        }
        NestedQuery other = (NestedQuery) obj;
        return path.equals(other.path) && child.equals(other.child) && scoreMode == other.scoreMode;
    }

    @Override
    protected String innerToString() {
        return "nested(" + path + "[" + scoreMode + "], " + child + ")";
    }
}
