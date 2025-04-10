/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.querydsl.query;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.Objects;

import static org.elasticsearch.index.query.QueryBuilders.wildcardQuery;

public class WildcardQuery extends Query {

    private final String field, query;
    private final boolean caseInsensitive;

    public WildcardQuery(Source source, String field, String query) {
        this(source, field, query, false);
    }

    public WildcardQuery(Source source, String field, String query, boolean caseInsensitive) {
        super(source);
        this.field = field;
        this.query = query;
        this.caseInsensitive = caseInsensitive;
    }

    public String field() {
        return field;
    }

    public String query() {
        return query;
    }

    public Boolean caseInsensitive() {
        return caseInsensitive;
    }

    @Override
    protected QueryBuilder asBuilder() {
        WildcardQueryBuilder wb = wildcardQuery(field, query);
        // ES does not allow case_insensitive to be set to "false", it should be either "true" or not specified
        return caseInsensitive == false ? wb : wb.caseInsensitive(caseInsensitive);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, query, caseInsensitive);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        WildcardQuery other = (WildcardQuery) obj;
        return Objects.equals(field, other.field)
            && Objects.equals(query, other.query)
            && Objects.equals(caseInsensitive, other.caseInsensitive);
    }

    @Override
    protected String innerToString() {
        return field + ":" + query;
    }
}
