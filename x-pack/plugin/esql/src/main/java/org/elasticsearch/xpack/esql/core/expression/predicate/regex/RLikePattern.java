/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression.predicate.regex;

import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.esql.core.tree.Node;
import org.elasticsearch.xpack.esql.core.tree.NodeStringMapper;

import java.io.IOException;
import java.util.Objects;

public class RLikePattern extends AbstractStringPattern implements Writeable {

    /** The {@link RegExp} syntax flags {@code RLIKE} parses its pattern with. */
    public static final int DEFAULT_SYNTAX_FLAGS = RegExp.ALL | RegExp.DEPRECATED_COMPLEMENT;

    static final TransportVersion ESQL_RLIKE_SYNTAX_FLAGS = TransportVersion.fromName("esql_rlike_syntax_flags");

    private final String regexpPattern;
    private final int syntaxFlags;

    public RLikePattern(String regexpPattern) {
        this(regexpPattern, DEFAULT_SYNTAX_FLAGS);
    }

    /**
     * A pattern parsed with other {@link RegExp} syntax flags than {@code RLIKE}'s, for a caller with its own regex
     * dialect: PromQL passes {@link RegExp#NONE} so that Lucene-only operators read as the literals RE2 takes them for.
     */
    public RLikePattern(String regexpPattern, int syntaxFlags) {
        this.regexpPattern = regexpPattern;
        this.syntaxFlags = syntaxFlags;
    }

    public RLikePattern(StreamInput in) throws IOException {
        this(in.readString(), in.getTransportVersion().supports(ESQL_RLIKE_SYNTAX_FLAGS) ? in.readVInt() : DEFAULT_SYNTAX_FLAGS);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(regexpPattern);
        if (out.getTransportVersion().supports(ESQL_RLIKE_SYNTAX_FLAGS)) {
            out.writeVInt(syntaxFlags);
        } else if (syntaxFlags != DEFAULT_SYNTAX_FLAGS) {
            // an older node would parse the pattern with the default flags and match other values
            throw new UnsupportedOperationException("version does not support regex syntax flags");
        }
    }

    @Override
    protected Automaton doCreateAutomaton(boolean ignoreCase) {
        int matchFlags = ignoreCase ? RegExp.CASE_INSENSITIVE : 0;
        return Operations.determinize(
            new RegExp(regexpPattern, syntaxFlags, matchFlags).toAutomaton(),
            Operations.DEFAULT_DETERMINIZE_WORK_LIMIT
        );
    }

    @Override
    public String asJavaRegex() {
        return regexpPattern;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RLikePattern that = (RLikePattern) o;
        return Objects.equals(regexpPattern, that.regexpPattern) && syntaxFlags == that.syntaxFlags;
    }

    @Override
    public int hashCode() {
        return Objects.hash(regexpPattern, syntaxFlags);
    }

    public String pattern() {
        return regexpPattern;
    }

    /** The {@link RegExp} syntax flags the pattern is parsed with. */
    public int syntaxFlags() {
        return syntaxFlags;
    }

    private static final String REGEX_METACHARACTERS = ".*+?()[]{}|^$\\";

    /**
     * Renders the regex quoted, routing maximal literal runs through {@code mapper.column} while
     * passing regex metacharacters ({@code . * + ? ( ) [ ] {@literal { } } | ^ $ \}) through
     * unchanged. Every non-metacharacter ends up inside a {@code column} call, so no literal content
     * survives anonymization; under {@link NodeStringMapper#IDENTITY} this reproduces the raw regex.
     */
    @Override
    public void nodeString(StringBuilder sb, Node.NodeStringFormat format, NodeStringMapper mapper) {
        sb.append('"');
        StringBuilder run = new StringBuilder();
        for (int i = 0; i < regexpPattern.length(); i++) {
            char c = regexpPattern.charAt(i);
            if (REGEX_METACHARACTERS.indexOf(c) >= 0) {
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
}
