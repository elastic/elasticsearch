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

import static org.elasticsearch.index.query.QueryBuilders.regexpQuery;

public class RegexQuery extends LeafQuery {

    private final String field, regex;
    private final boolean caseInsensitive;

    public RegexQuery(Source source, String field, String regex) {
        this(source, field, regex, false);
    }

    public RegexQuery(Source source, String field, String regex, boolean caseInsensitive) {
        super(source);
        this.field = field;
        this.regex = regex;
        this.caseInsensitive = caseInsensitive;
    }

    public String field() {
        return field;
    }

    public String regex() {
        return regex;
    }

    public Boolean caseInsensitive() {
        return caseInsensitive;
    }

    @Override
    public QueryBuilder asBuilder() {
        return regexpQuery(field, regex).caseInsensitive(caseInsensitive);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, regex, caseInsensitive);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        RegexQuery other = (RegexQuery) obj;
        return Objects.equals(field, other.field) && Objects.equals(regex, other.regex) && caseInsensitive == other.caseInsensitive;
    }

    @Override
    protected String innerToString() {
        return field + "~ /" + regex + "/";
    }
}
