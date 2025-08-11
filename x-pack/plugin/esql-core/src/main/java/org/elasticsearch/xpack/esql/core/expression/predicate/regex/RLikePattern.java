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

import java.util.Objects;

public class RLikePattern extends AbstractStringPattern {

    private final String regexpPattern;

    public RLikePattern(String regexpPattern) {
        this.regexpPattern = regexpPattern;
    }

    @Override
    public Automaton createAutomaton(boolean ignoreCase) {
        int matchFlags = ignoreCase ? RegExp.ASCII_CASE_INSENSITIVE : 0;
        return Operations.determinize(
            new RegExp(regexpPattern, RegExp.ALL, matchFlags).toAutomaton(),
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
