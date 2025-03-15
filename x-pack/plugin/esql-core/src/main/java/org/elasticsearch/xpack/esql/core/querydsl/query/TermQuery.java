/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.querydsl.query;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.Objects;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;

/**
 * Term query. It can be considered for scoring or not - filters that use term query as implementation will not use scoring,
 * but the Term full text function will
 */
public class TermQuery extends Query {

    private final String term;
    private final Object value;
    private final boolean caseInsensitive;
    private final boolean scorable;

    public TermQuery(Source source, String term, Object value) {
        this(source, term, value, false);
    }

    public TermQuery(Source source, String term, Object value, boolean caseInsensitive) {
        this(source, term, value, caseInsensitive, false);
    }

    public TermQuery(Source source, String term, Object value, boolean caseInsensitive, boolean scorable) {
        super(source);
        this.term = term;
        this.value = value;
        this.caseInsensitive = caseInsensitive;
        this.scorable = scorable;
    }

    public String term() {
        return term;
    }

    public Object value() {
        return value;
    }

    public Boolean caseInsensitive() {
        return caseInsensitive;
    }

    @Override
    protected QueryBuilder asBuilder() {
        TermQueryBuilder qb = termQuery(term, value);
        // ES does not allow case_insensitive to be set to "false", it should be either "true" or not specified
        return caseInsensitive == false ? qb : qb.caseInsensitive(caseInsensitive);
    }

    @Override
    public int hashCode() {
        return Objects.hash(term, value, caseInsensitive, scorable);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        TermQuery other = (TermQuery) obj;
        return Objects.equals(term, other.term)
            && Objects.equals(value, other.value)
            && Objects.equals(caseInsensitive, other.caseInsensitive)
            && scorable == other.scorable;
    }

    @Override
    protected String innerToString() {
        return term + ":" + value;
    }

    @Override
    public boolean scorable() {
        return scorable;
    }
}
