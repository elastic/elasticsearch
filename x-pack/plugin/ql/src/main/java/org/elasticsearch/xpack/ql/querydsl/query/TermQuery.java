/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.querydsl.query;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.Objects;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;

public class TermQuery extends LeafQuery {

    private final String term;
    private final Object value;

    public TermQuery(Source source, String term, Object value) {
        super(source);
        this.term = term;
        this.value = value;
    }

    public String term() {
        return term;
    }

    public Object value() {
        return value;
    }

    @Override
    public QueryBuilder asBuilder() {
        return termQuery(term, value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(term, value);
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
                && Objects.equals(value, other.value);
    }

    @Override
    protected String innerToString() {
        return term + ":" + value;
    }
}
