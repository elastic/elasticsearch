/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression.predicate.regex;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.esql.core.util.StringUtils;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.util.StringUtils.luceneWildcardToRegExp;

/**
 * Similar to basic regex, supporting '?' wildcard for single character (same as regex  ".")
 * and '*' wildcard for multiple characters (same as regex ".*")
 * <p>
 * Allows escaping based on a regular char.
 *
 */
public class WildcardPattern extends AbstractStringPattern implements Writeable {

    private final String wildcard;
    private final String regex;

    public WildcardPattern(String pattern) {
        this.wildcard = pattern;
        // early initialization to force string validation
        this.regex = StringUtils.wildcardToJavaPattern(pattern, '\\');
    }

    public WildcardPattern(StreamInput in) throws IOException {
        this(in.readString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(wildcard);
    }

    public String pattern() {
        return wildcard;
    }

    @Override
    protected Automaton doCreateAutomaton(boolean ignoreCase) {
        return ignoreCase
            ? Operations.determinize(
                new RegExp(luceneWildcardToRegExp(wildcard), RegExp.ALL | RegExp.DEPRECATED_COMPLEMENT, RegExp.CASE_INSENSITIVE)
                    .toAutomaton(),
                Operations.DEFAULT_DETERMINIZE_WORK_LIMIT
            )
            : WildcardQuery.toAutomaton(new Term(null, wildcard), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
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

    /**
     * Extracts the fixed literal prefix before the first unescaped wildcard ({@code *} or {@code ?}).
     * Escaped wildcards ({@code \*}, {@code \?}) are treated as literal characters.
     * Returns {@code null} if the prefix is empty (pattern starts with a wildcard or is empty).
     */
    public String extractPrefix() {
        StringBuilder prefix = new StringBuilder();
        for (int i = 0; i < wildcard.length(); i++) {
            char c = wildcard.charAt(i);
            if (c == '\\' && i + 1 < wildcard.length()) {
                prefix.append(wildcard.charAt(i + 1));
                i++;
            } else if (c == '*' || c == '?') {
                break;
            } else {
                prefix.append(c);
            }
        }
        return prefix.isEmpty() ? null : prefix.toString();
    }

    /**
     * Extracts the fixed literal suffix after the last unescaped wildcard ({@code *} or {@code ?}).
     * Escaped wildcards ({@code \*}, {@code \?}) are treated as literal characters.
     * Returns {@code null} if the suffix is empty (pattern ends with a wildcard or is empty).
     */
    public String extractSuffix() {
        int lastWildcard = -1;
        for (int i = 0; i < wildcard.length(); i++) {
            char c = wildcard.charAt(i);
            if (c == '\\' && i + 1 < wildcard.length()) {
                i++;
            } else if (c == '*' || c == '?') {
                lastWildcard = i;
            }
        }
        int start = lastWildcard + 1;
        StringBuilder suffix = new StringBuilder();
        for (int i = start; i < wildcard.length(); i++) {
            char c = wildcard.charAt(i);
            if (c == '\\' && i + 1 < wildcard.length()) {
                suffix.append(wildcard.charAt(i + 1));
                i++;
            } else {
                suffix.append(c);
            }
        }
        return suffix.isEmpty() ? null : suffix.toString();
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
