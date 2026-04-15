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

public class WildcardQuery extends Query {

    private final String field, query;
    private final boolean caseInsensitive;
    private final boolean forceStringMatch;

    /**
     * Escapes characters that have special meaning in wildcard query syntax ({@code *}, {@code ?}, {@code \}).
     * This is different from {@link org.apache.lucene.queryparser.classic.QueryParser#escape} which escapes
     * Lucene query parser syntax characters - a superset of the wildcard-special characters.
     */
    public static String escapeWildcardSyntax(String term) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < term.length();) {
            final int c = term.codePointAt(i);
            int length = Character.charCount(c);
            if (c == '*' || c == '?' || c == '\\') {
                result.append("\\");
            }
            result.appendCodePoint(c);
            i += length;
        }
        return result.toString();
    }

    public WildcardQuery(Source source, String field, String query, boolean caseInsensitive, boolean forceStringMatch) {
        super(source);
        this.field = field;
        this.query = query;
        this.caseInsensitive = caseInsensitive;
        this.forceStringMatch = forceStringMatch;
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
        WildcardQueryBuilder wb = new WildcardQueryBuilder(field, query, forceStringMatch);
        // ES does not allow case_insensitive to be set to "false", it should be either "true" or not specified
        return caseInsensitive == false ? wb : wb.caseInsensitive(caseInsensitive);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, query, caseInsensitive, forceStringMatch);
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
            && Objects.equals(caseInsensitive, other.caseInsensitive)
            && Objects.equals(forceStringMatch, other.forceStringMatch);
    }

    @Override
    protected String innerToString() {
        return field + ":" + query;
    }

    @Override
    public boolean containsPlan() {
        return false;
    }
}
