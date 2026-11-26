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
                new RegExp(luceneWildcardToRegExp(wildcard), RegExp.ALL, RegExp.CASE_INSENSITIVE).toAutomaton(),
                Operations.DEFAULT_DETERMINIZE_WORK_LIMIT
            )
            : Operations.determinize(WildcardQuery.toAutomaton(new Term(null, wildcard)), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
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
     * Classify this pattern into one of the affix fast-path shapes, unwrapping
     * backslash escapes and collapsing adjacent unescaped {@code *} runs in a
     * single pass. The result is one of {@link Shape.Prefix}, {@link Shape.Suffix},
     * {@link Shape.Contains}, {@link Shape.Affix}, or {@link Shape.General#INSTANCE}.
     * Prefix/suffix/contains keep the simple pinned shapes so existing evaluators
     * keep firing; {@link Shape.Affix} covers {@code prefix*suffix},
     * {@code prefix*middle*}, {@code *middle*suffix}, and
     * {@code prefix*middle*suffix}. {@code General} is the fall-through for
     * three or more star-groups, any unescaped {@code ?}, escaped-only patterns
     * with no wildcards, dangling backslashes, and the empty pattern — callers
     * should then use the automaton path.
     */
    public Shape shape() {
        final int n = wildcard.length();
        if (n == 0) {
            return Shape.General.INSTANCE;
        }
        // Literal segments split by maximal runs of unescaped '*'. At most two
        // star-groups (three segments) are an Affix/Contains/Prefix/Suffix;
        // a third group is General.
        String prefix = "";
        String middle = "";
        StringBuilder cur = new StringBuilder(n);
        int starGroups = 0;
        int starChars = 0;
        boolean lastWasStar = false;
        int i = 0;
        while (i < n) {
            char c = wildcard.charAt(i);
            if (c == '\\') {
                if (i + 1 >= n) {
                    // Dangling escape — let the general automaton path handle it.
                    return Shape.General.INSTANCE;
                }
                cur.append(wildcard.charAt(i + 1));
                lastWasStar = false;
                i += 2;
                continue;
            }
            if (c == '?') {
                return Shape.General.INSTANCE;
            }
            if (c == '*') {
                starChars++;
                if (lastWasStar) {
                    i++;
                    continue;
                }
                if (starGroups == 3) {
                    return Shape.General.INSTANCE;
                }
                String lit = cur.toString();
                cur.setLength(0);
                if (starGroups == 0) {
                    prefix = lit;
                } else if (starGroups == 1) {
                    middle = lit;
                }
                starGroups++;
                lastWasStar = true;
                i++;
                continue;
            }
            cur.append(c);
            lastWasStar = false;
            i++;
        }
        if (starGroups == 0 || starGroups >= 3) {
            return Shape.General.INSTANCE;
        }
        String suffix = cur.toString();
        if (starGroups == 1) {
            if (prefix.isEmpty()) {
                // "*" stays Suffix(""); collapsed "**" (and "***"…) historically
                // classified as Contains("") — keep that so evaluator-name pins
                // on the empty-contains shape stay green.
                if (suffix.isEmpty() && starChars >= 2) {
                    return new Shape.Contains("");
                }
                return new Shape.Suffix(suffix);
            }
            if (suffix.isEmpty()) {
                return new Shape.Prefix(prefix);
            }
            return new Shape.Affix(prefix, "", suffix);
        }
        if (prefix.isEmpty() && suffix.isEmpty()) {
            return new Shape.Contains(middle);
        }
        return new Shape.Affix(prefix, middle, suffix);
    }

    /**
     * Classification of a {@link WildcardPattern} into the fast-path shapes.
     * {@link Prefix}, {@link Suffix}, and {@link Contains} carry one unescaped
     * literal segment; {@link Affix} carries prefix, middle, and suffix
     * (any of which may be empty); {@link General} is the fall-through.
     */
    public sealed interface Shape permits Shape.Prefix, Shape.Suffix, Shape.Contains, Shape.Affix, Shape.General {
        /** {@code literal*} — pattern matches values that start with {@code literal}. */
        record Prefix(String literal) implements Shape {}

        /** {@code *literal} — pattern matches values that end with {@code literal}. */
        record Suffix(String literal) implements Shape {}

        /** {@code *literal*} — pattern matches values that contain {@code literal}. */
        record Contains(String literal) implements Shape {}

        /**
         * {@code prefix*middle*suffix} and the two-star degenerates
         * ({@code prefix*suffix}, {@code prefix*middle*}, {@code *middle*suffix}).
         * Empty strings mark a missing component. Simple pinned shapes are
         * classified as {@link Prefix}/{@link Suffix}/{@link Contains} instead.
         */
        record Affix(String prefix, String middle, String suffix) implements Shape {}

        /**
         * Anything that does not match {@link Prefix}, {@link Suffix},
         * {@link Contains}, or {@link Affix}. Callers should dispatch to the
         * automaton path.
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
