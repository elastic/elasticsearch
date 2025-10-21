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
    public Automaton createAutomaton(boolean ignoreCase) {
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
}
