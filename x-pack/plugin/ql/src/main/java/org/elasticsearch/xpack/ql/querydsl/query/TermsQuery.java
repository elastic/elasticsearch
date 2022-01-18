/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.querydsl.query;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.index.query.QueryBuilders.termsQuery;

public class TermsQuery extends LeafQuery {

    private final String term;
    private final Set<Object> values;

    public TermsQuery(Source source, String term, Set<Object> values) {
        super(source);
        this.term = term;
        this.values = values;
    }

    @Override
    public QueryBuilder asBuilder() {
        return termsQuery(term, values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(term, values);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        TermsQuery other = (TermsQuery) obj;
        return Objects.equals(term, other.term) && Objects.equals(values, other.values);
    }

    @Override
    protected String innerToString() {
        return term + ":" + values;
    }
}
