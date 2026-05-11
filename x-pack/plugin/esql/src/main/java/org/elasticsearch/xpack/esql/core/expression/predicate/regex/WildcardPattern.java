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
     * If this pattern has the shape {@code literal*} — exactly one unescaped
     * {@code *} as the final character, no other wildcards — returns the
     * literal prefix (with backslash escapes unwrapped). Returns {@code null}
     * otherwise. Callers can use this to short-circuit to a {@code startsWith}
     * byte comparison instead of building an {@code Automaton}.
     */
    public String matchesPrefix() {
        return prefixOrSuffix(true);
    }

    /**
     * If this pattern has the shape {@code *literal} — leading unescaped
     * {@code *}, no other wildcards — returns the literal suffix (with
     * backslash escapes unwrapped). Returns {@code null} otherwise.
     */
    public String matchesSuffix() {
        return prefixOrSuffix(false);
    }

    /**
     * If this pattern has the shape {@code *literal*} — leading and trailing
     * unescaped {@code *}, no other wildcards in between — returns the inner
     * literal (with backslash escapes unwrapped). Returns {@code null}
     * otherwise. Callers can use this to short-circuit to a substring search
     * instead of building an {@code Automaton}. An empty pattern ({@code **})
     * returns an empty string; a substring search for the empty string
     * matches every input.
     */
    public String matchesContains() {
        final int n = wildcard.length();
        if (n < 2 || wildcard.charAt(0) != '*' || wildcard.charAt(n - 1) != '*') {
            return null;
        }
        StringBuilder out = new StringBuilder(n - 2);
        int i = 1;
        final int last = n - 1;
        while (i < last) {
            char c = wildcard.charAt(i);
            if (c == '\\') {
                if (i + 1 >= last) {
                    // Dangling escape right before the trailing '*' — let the
                    // general automaton path handle it.
                    return null;
                }
                out.append(wildcard.charAt(i + 1));
                i += 2;
                continue;
            }
            if (c == '*' || c == '?') {
                return null;
            }
            out.append(c);
            i++;
        }
        return out.toString();
    }

    private String prefixOrSuffix(boolean prefix) {
        final int n = wildcard.length();
        if (n == 0) {
            return null;
        }
        if (prefix == false && wildcard.charAt(0) != '*') {
            return null;
        }
        // The single allowed unescaped wildcard slot.
        final int wildcardSlot = prefix ? n - 1 : 0;
        final int litStart = prefix ? 0 : 1;
        StringBuilder out = new StringBuilder(n);
        int i = litStart;
        while (i < n) {
            char c = wildcard.charAt(i);
            if (c == '\\') {
                if (i + 1 >= n) {
                    // Dangling escape — let the general automaton path handle it.
                    return null;
                }
                out.append(wildcard.charAt(i + 1));
                i += 2;
                continue;
            }
            if (c == '*' || c == '?') {
                if (c == '*' && i == wildcardSlot) {
                    return out.toString();
                }
                return null;
            }
            out.append(c);
            i++;
        }
        // For the suffix shape the leading '*' was already verified at i==0.
        // For the prefix shape, a clean walk to the end without hitting the
        // trailing '*' means there was no wildcard at all.
        return prefix ? null : out.toString();
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
