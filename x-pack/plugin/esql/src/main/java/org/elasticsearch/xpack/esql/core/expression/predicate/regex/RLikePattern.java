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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.esql.core.tree.Node;
import org.elasticsearch.xpack.esql.core.tree.NodeStringMapper;

import java.io.IOException;
import java.util.Objects;

public class RLikePattern extends AbstractStringPattern implements Writeable {

    private final String regexpPattern;

    public RLikePattern(String regexpPattern) {
        this.regexpPattern = regexpPattern;
    }

    public RLikePattern(StreamInput in) throws IOException {
        this(in.readString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(regexpPattern);
    }

    @Override
    protected Automaton doCreateAutomaton(boolean ignoreCase) {
        int matchFlags = ignoreCase ? RegExp.CASE_INSENSITIVE : 0;
        return Operations.determinize(
            new RegExp(regexpPattern, RegExp.ALL | RegExp.DEPRECATED_COMPLEMENT, matchFlags).toAutomaton(),
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
        return Objects.equals(regexpPattern, that.regexpPattern);
    }

    @Override
    public int hashCode() {
        return Objects.hash(regexpPattern);
    }

    public String pattern() {
        return regexpPattern;
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
