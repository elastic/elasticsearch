/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.querydsl.query;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.Objects;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;

public class TermQuery extends LeafQuery {

    private final String term;
    private final Object value;
    private final boolean caseInsensitive;

    public TermQuery(Source source, String term, Object value) {
        this(source, term, value, false);
    }

    public TermQuery(Source source, String term, Object value, boolean caseInsensitive) {
        super(source);
        this.term = term;
        this.value = value;
        this.caseInsensitive = caseInsensitive;
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
    public QueryBuilder asBuilder() {
        TermQueryBuilder qb = termQuery(term, value);
        // ES does not allow case_insensitive to be set to "false", it should be either "true" or not specified
        return caseInsensitive == false ? qb : qb.caseInsensitive(caseInsensitive);
    }

    @Override
    public int hashCode() {
        return Objects.hash(term, value, caseInsensitive);
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
                && Objects.equals(caseInsensitive, other.caseInsensitive);
    }

    @Override
    protected String innerToString() {
        return term + ":" + value;
    }
}
