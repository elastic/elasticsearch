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
import org.elasticsearch.lucene.search.cost.RegexpNfaRamEstimator;
import org.elasticsearch.lucene.util.automaton.CircuitBreakingOperations;
import org.elasticsearch.xpack.ql.InvalidArgumentException;

import java.util.Objects;

public class RLikePattern extends AbstractStringPattern {

    private final String regexpPattern;

    public RLikePattern(String regexpPattern) {
        this.regexpPattern = regexpPattern;
    }

    /**
     * The NFA is built only after its estimated size fits the budget: a length limit alone does not bound it, since
     * {@code [ab]{1000}{1000}{1000}} is 22 characters and about a billion states. The estimator walks the parse tree
     * recursively, as the parser does, so both stay inside {@link #createAutomaton()}'s overflow guard.
     */
    @Override
    protected Automaton doCreateAutomaton() {
        checkLength(regexpPattern);
        RegExp re;
        try {
            re = new RegExp(regexpPattern, RegExp.ALL | RegExp.DEPRECATED_COMPLEMENT);
        } catch (NumberFormatException e) {
            throw new InvalidArgumentException(e, "Pattern repeat count is out of range");
        }
        AutomatonBudget budget = new AutomatonBudget();
        budget.addEstimateBytesAndMaybeBreak(RegexpNfaRamEstimator.estimateRamBytes(re), "rlike");
        return CircuitBreakingOperations.determinize(re.toAutomaton(), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT, budget, "rlike");
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
