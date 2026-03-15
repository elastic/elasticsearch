/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.querydsl.query;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermsSetQueryBuilder;

import java.util.List;
import java.util.Objects;

import org.elasticsearch.xpack.esql.core.tree.Source;


/**
 * Query wrapper for Elasticsearch's TermsSetQueryBuilder.
 * Used by ES|QL to push down mv_contains operations to Lucene.
 */
public class TermsSetQuery extends Query {

    private final String field;
    private final List<Object> terms;
    private final int minimumShouldMatch;

    public TermsSetQuery(Source source, String field, List<Object> terms, int minimumShouldMatch) {
        super(source);
        this.field = field;
        this.terms = terms;
        this.minimumShouldMatch = minimumShouldMatch;
    }

    public String field() {
        return field;
    }

    public List<Object> terms() {
        return terms;
    }

    public int minimumShouldMatch() {
        return minimumShouldMatch;
    }

    @Override
    protected QueryBuilder asBuilder() {
        TermsSetQueryBuilder builder = new TermsSetQueryBuilder(field, terms);
        builder.setMinimumShouldMatch(String.valueOf(minimumShouldMatch));
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), field, terms, minimumShouldMatch);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        if (super.equals(obj) == false) {
            return false;
        }

        TermsSetQuery other = (TermsSetQuery) obj;
        return Objects.equals(field, other.field)
            && Objects.equals(terms, other.terms)
            && minimumShouldMatch == other.minimumShouldMatch;
    }

    @Override
    protected String innerToString() {
        return field + ":" + terms + " (min:" + minimumShouldMatch + ")";
    }

    @Override
    public boolean containsPlan() {
        return false;
    }
}
