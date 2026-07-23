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
import org.elasticsearch.xpack.esql.core.tree.Node;
import org.elasticsearch.xpack.esql.core.tree.NodeStringMapper;
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

    /**
     * Renders the wildcard quoted, routing literal runs through {@code mapper.column} while keeping
     * the wildcard metacharacters ({@code * ? % _}) and escapes structural. Under
     * {@link NodeStringMapper#IDENTITY} this reproduces the raw pattern.
     */
    @Override
    public void nodeString(StringBuilder sb, Node.NodeStringFormat format, NodeStringMapper mapper) {
        sb.append('"');
        StringBuilder run = new StringBuilder();
        for (int i = 0; i < wildcard.length(); i++) {
            char c = wildcard.charAt(i);
            if (c == '\\' && i + 1 < wildcard.length()) {
                run.append(wildcard.charAt(++i));
                continue;
            }
            if (c == '*' || c == '?' || c == '%' || c == '_') {
                if (run.length() > 0) {
                    sb.append(mapper.column(run.toString()));
                    run.setLength(0);
                }
                sb.append(c);
            } else {
                run.append(c);
            }
        }
        if (run.length() > 0) {
            sb.append(mapper.column(run.toString()));
        }
        sb.append('"');
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
     * Classify this pattern into one of the affix-only fast-path shapes,
     * unwrapping backslash escapes in a single pass. The result is one of
     * {@link Shape.Prefix}, {@link Shape.Suffix}, {@link Shape.Contains}, or
     * {@link Shape.General#INSTANCE}. The first three carry the
     * literal segment so callers can dispatch to a {@code startsWith} /
     * {@code endsWith} / substring-search evaluator without building an
     * {@code Automaton}; {@code General} signals that the pattern does not
     * match any simple shape (multiple unescaped {@code *}s with text
     * between them, any unescaped {@code ?}, escaped-only patterns with no
     * wildcards, dangling backslashes, etc.) and the caller should fall
     * back to the automaton path.
     */
    public Shape shape() {
        final int n = wildcard.length();
        if (n == 0) {
            return Shape.General.INSTANCE;
        }
        StringBuilder unescaped = new StringBuilder(n);
        int starsCount = 0;
        boolean firstStarAtStart = false;
        boolean lastStarAtEnd = false;
        int i = 0;
        while (i < n) {
            char c = wildcard.charAt(i);
            if (c == '\\') {
                if (i + 1 >= n) {
                    // Dangling escape — let the general automaton path handle it.
                    return Shape.General.INSTANCE;
                }
                unescaped.append(wildcard.charAt(i + 1));
                i += 2;
                continue;
            }
            if (c == '?') {
                return Shape.General.INSTANCE;
            }
            if (c == '*') {
                starsCount++;
                if (starsCount == 1) {
                    firstStarAtStart = (i == 0);
                }
                lastStarAtEnd = (i == n - 1);
                i++;
                continue;
            }
            unescaped.append(c);
            i++;
        }
        if (starsCount == 0) {
            // Exact-match string — no specific fast path; fall through.
            return Shape.General.INSTANCE;
        }
        if (starsCount == 1) {
            if (firstStarAtStart) {
                return new Shape.Suffix(unescaped.toString());
            }
            if (lastStarAtEnd) {
                return new Shape.Prefix(unescaped.toString());
            }
            // Single star with non-empty body on both sides — prefix*suffix.
            // No fast path for this shape here.
            return Shape.General.INSTANCE;
        }
        if (starsCount == 2 && firstStarAtStart && lastStarAtEnd) {
            return new Shape.Contains(unescaped.toString());
        }
        // More than two unescaped stars, or two stars not pinned to both ends.
        return Shape.General.INSTANCE;
    }

    /**
     * Classification of a {@link WildcardPattern} into the simple fast-path
     * shapes. The three concrete records carry the unescaped literal
     * segment; {@link General} is the fall-through case for everything else.
     */
    public sealed interface Shape permits Shape.Prefix, Shape.Suffix, Shape.Contains, Shape.General {
        /** {@code literal*} — pattern matches values that start with {@code literal}. */
        record Prefix(String literal) implements Shape {}

        /** {@code *literal} — pattern matches values that end with {@code literal}. */
        record Suffix(String literal) implements Shape {}

        /** {@code *literal*} — pattern matches values that contain {@code literal}. */
        record Contains(String literal) implements Shape {}

        /**
         * Anything that does not match {@link Prefix}, {@link Suffix}, or
         * {@link Contains}. Callers should dispatch to the automaton path.
         */
        enum General implements Shape {
            INSTANCE
        }
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
