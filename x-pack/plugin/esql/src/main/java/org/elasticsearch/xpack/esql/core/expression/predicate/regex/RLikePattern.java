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

    /**
     * The regular expression dialect a pattern is written in.
     */
    public enum Syntax {
        /** Lucene's {@link RegExp} syntax, which {@code RLIKE} takes. */
        LUCENE,
        /** RE2 syntax, which PromQL label matchers take; see {@link Re2RegExp}. */
        RE2
    }

    static final TransportVersion ESQL_RLIKE_RE2_SYNTAX = TransportVersion.fromName("esql_rlike_re2_syntax");

    private final String regexpPattern;
    private final Syntax syntax;

    public RLikePattern(String regexpPattern) {
        this(regexpPattern, Syntax.LUCENE);
    }

    public RLikePattern(String regexpPattern, Syntax syntax) {
        this.regexpPattern = regexpPattern;
        this.syntax = syntax;
    }

    public RLikePattern(StreamInput in) throws IOException {
        this(in.readString(), in.getTransportVersion().supports(ESQL_RLIKE_RE2_SYNTAX) ? in.readEnum(Syntax.class) : Syntax.LUCENE);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(regexpPattern);
        if (out.getTransportVersion().supports(ESQL_RLIKE_RE2_SYNTAX)) {
            out.writeEnum(syntax);
        } else if (syntax != Syntax.LUCENE) {
            // an older node would read the pattern as Lucene syntax and match other values
            throw new UnsupportedOperationException("version does not support RE2 regex syntax");
        }
    }

    @Override
    protected Automaton doCreateAutomaton(boolean ignoreCase) {
        Automaton automaton = switch (syntax) {
            case LUCENE -> new RegExp(regexpPattern, RegExp.ALL | RegExp.DEPRECATED_COMPLEMENT, ignoreCase ? RegExp.CASE_INSENSITIVE : 0)
                .toAutomaton();
            case RE2 -> new Re2RegExp(regexpPattern, ignoreCase).toAutomaton();
        };
        return Operations.determinize(automaton, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
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
        return Objects.equals(regexpPattern, that.regexpPattern) && syntax == that.syntax;
    }

    @Override
    public int hashCode() {
        return Objects.hash(regexpPattern, syntax);
    }

    public String pattern() {
        return regexpPattern;
    }

    public Syntax syntax() {
        return syntax;
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
