/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression.predicate.regex;

import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.lucene.util.automaton.CircuitBreakingOperations;
import org.elasticsearch.lucene.util.automaton.CircuitBreakingRegExp;

import java.util.Objects;

public class RLikePattern extends AbstractStringPattern {

    private final String regexpPattern;

    /**
     * The bound the {@code regexp} query applies through {@code index.max_regex_length}, at its default. Lucene's parser
     * recurses on nested groups, so the optimizer does not parse a longer pattern.
     */
    static final int MAX_COMPILED_LENGTH = IndexSettings.MAX_REGEX_LENGTH_SETTING.getDefault(Settings.EMPTY);

    private static final String LABEL = "rlike";

    private final int maxCompiledLength;

    public RLikePattern(String regexpPattern) {
        this(regexpPattern, MAX_COMPILED_LENGTH);
    }

    /** Tests lift the length bound to reach the overflow guard with a pattern the bound would skip first. */
    RLikePattern(String regexpPattern, int maxCompiledLength) {
        this.regexpPattern = regexpPattern;
        this.maxCompiledLength = maxCompiledLength;
    }

    /**
     * Runs on a coordinator thread during optimization, with no request breaker, so the build is bounded in length, memory
     * and work. A pattern past any of the bounds is not an error here: it yields no automaton, and the query runs
     * unsimplified.
     */
    @Override
    @SuppressForbidden(reason = "TODO: replace with manual depth tracking before the overflow occurs")
    public Automaton createAutomaton() {
        if (regexpPattern.length() > maxCompiledLength) {
            return null;
        }
        AutomatonBudget budget = new AutomatonBudget();
        try {
            Automaton nfa = new CircuitBreakingRegExp(regexpPattern, RegExp.ALL | RegExp.DEPRECATED_COMPLEMENT, 0).toAutomaton(
                budget,
                LABEL
            );
            budget.addEstimateBytesAndMaybeBreak(nfa.ramBytesUsed(), LABEL);
            return CircuitBreakingOperations.determinize(nfa, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT, budget, LABEL);
        } catch (CircuitBreakingException | TooComplexToDeterminizeException e) {
            return null;
        } catch (StackOverflowError e) { // TODO: unsafe - replace with manual depth tracking
            return null;
        }
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
}
