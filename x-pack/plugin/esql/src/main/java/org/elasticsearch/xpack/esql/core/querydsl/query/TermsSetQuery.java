/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.querydsl.query;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermsSetQueryBuilder;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.Objects;
import java.util.Set;

public class TermsSetQuery extends Query {
    private final String field;
    private final Set<Object> values;

    public TermsSetQuery(Source source, String field, Set<Object> values) {
        super(source);
        this.field = field;
        this.values = values;
    }

    @Override
    protected QueryBuilder asBuilder() {
        return new TermsSetQueryBuilder(field, values.stream().toList()).setMinimumShouldMatch(String.valueOf(values.size()));
    }

    @Override
    protected String innerToString() {
        return "terms_set(" + field + ", " + values + ")";
    }

    @Override
    public boolean containsPlan() {
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, values);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        TermsSetQuery other = (TermsSetQuery) obj;
        return field.equals(other.field) && values.equals(other.values);
    }
}
