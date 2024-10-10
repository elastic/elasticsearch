/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression.predicate.regex;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.xpack.ql.util.StringUtils;

import java.util.Objects;

/**
 * Similar to basic regex, supporting '?' wildcard for single character (same as regex  ".")
 * and '*' wildcard for multiple characters (same as regex ".*")
 * <p>
 * Allows escaping based on a regular char
 *
 */
public class WildcardPattern extends AbstractStringPattern {

    private final String wildcard;
    private final String regex;

    public WildcardPattern(String pattern) {
        this.wildcard = pattern;
        // early initialization to force string validation
        this.regex = StringUtils.wildcardToJavaPattern(pattern, '\\');
    }

    public String pattern() {
        return wildcard;
    }

    @Override
    public Automaton createAutomaton() {
        return WildcardQuery.toAutomaton(new Term(null, wildcard), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
    }

    @Override
    public String asJavaRegex() {
        return regex;
    }

    /**
     * Returns the pattern in (Lucene) wildcard format.
     */
    public String asLuceneWildcard() {
        return wildcard;
    }

    /**
     * Returns the pattern in (IndexNameExpressionResolver) wildcard format.
     */
    public String asIndexNameWildcard() {
        return wildcard;
    }

    @Override
    public int hashCode() {
        return Objects.hash(wildcard);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        WildcardPattern other = (WildcardPattern) obj;
        return Objects.equals(wildcard, other.wildcard);
    }
}
