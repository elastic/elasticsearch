/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.querydsl.query;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RegexpQueryBuilder;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.Objects;

import static org.elasticsearch.index.query.QueryBuilders.regexpQuery;

public class RegexQuery extends Query {

    private final String field, regex;
    private final int syntaxFlags;
    private final boolean caseInsensitive;

    public RegexQuery(Source source, String field, String regex, boolean caseInsensitive) {
        this(source, field, regex, RegexpQueryBuilder.DEFAULT_FLAGS_VALUE, caseInsensitive);
    }

    /** A regex parsed with the given {@link org.apache.lucene.util.automaton.RegExp} syntax flags. */
    public RegexQuery(Source source, String field, String regex, int syntaxFlags, boolean caseInsensitive) {
        super(source);
        this.field = field;
        this.regex = regex;
        this.syntaxFlags = syntaxFlags;
        this.caseInsensitive = caseInsensitive;
    }

    public String field() {
        return field;
    }

    public String regex() {
        return regex;
    }

    public int syntaxFlags() {
        return syntaxFlags;
    }

    public Boolean caseInsensitive() {
        return caseInsensitive;
    }

    @Override
    protected QueryBuilder asBuilder() {
        return regexpQuery(field, regex).flags(syntaxFlags).caseInsensitive(caseInsensitive);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, regex, syntaxFlags, caseInsensitive);
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
        return Objects.equals(field, other.field)
            && Objects.equals(regex, other.regex)
            && syntaxFlags == other.syntaxFlags
            && caseInsensitive == other.caseInsensitive;
    }

    @Override
    protected String innerToString() {
        return field + "~ /" + regex + "/";
    }

    @Override
    public boolean containsPlan() {
        return false;
    }
}
