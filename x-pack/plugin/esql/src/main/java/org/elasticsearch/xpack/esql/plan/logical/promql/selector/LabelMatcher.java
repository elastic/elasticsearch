/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql.selector;

import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.lucene.util.automaton.MinimizationOperations;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;

import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.util.StringUtils.EMPTY;
import static org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher.Matcher.NEQ;
import static org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher.Matcher.NREG;

/**
 * PromQL label matcher between a label name, a value pattern and match type (=, !=, =~, !~).
 *
 * Examples:
 *   {job="api"}              → [LabelMatcher("job", "api", EQ)]
 *   {status=~"5.."}          → [LabelMatcher("status", "5..", REG)]
 *   {env!~"test|dev"}        → [LabelMatcher("env", "test|dev", NREG)]
 */
public class LabelMatcher {

    public static final String NAME = "__name__";

    public enum Matcher {
        EQ("=", false),
        NEQ("!=", false),
        REG("=~", true),
        NREG("!~", true);

        public static Matcher from(String value) {
            switch (value) {
                case "=":
                    return EQ;
                case "!=":
                    return NEQ;
                case "=~":
                    return REG;
                case "!~":
                    return NREG;
                default:
                    return null;
            }
        }

        private final String value;
        private final boolean isRegex;

        Matcher(String value, boolean isRegex) {
            this.value = value;
            this.isRegex = isRegex;
        }

        public boolean isRegex() {
            return isRegex;
        }
    }

    private final String name;
    private final String value;
    private final Matcher matcher;

    private Automaton automaton;

    public LabelMatcher(String name, String value, Matcher matcher) {
        this.name = name;
        this.value = value;
        this.matcher = matcher;
    }

    public String name() {
        return name;
    }

    public String value() {
        return value;
    }

    public Matcher matcher() {
        return matcher;
    }

    public Automaton automaton() {
        if (automaton == null) {
            automaton = automaton(value, matcher);
        }
        return automaton;
    }

    // TODO: externalize this to allow pluggable strategies (such as caching across labels/requests)
    private static Automaton automaton(String value, Matcher matcher) {
        Automaton automaton;
        try {
            automaton = matcher.isRegex ? new RegExp(value).toAutomaton() : Automata.makeString(value);
            automaton = MinimizationOperations.minimize(automaton, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        } catch (IllegalArgumentException ex) {
            throw new QlIllegalArgumentException(ex, "Cannot parse regex {}", value);
        }
        // negate if needed
        if (matcher == NEQ || matcher == NREG) {
            automaton = Operations.complement(automaton, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        }
        return automaton;
    }

    public boolean matchesAll() {
        return Operations.isTotal(automaton());
    }

    public boolean matchesNone() {
        return Operations.isEmpty(automaton());
    }

    public boolean matchesEmpty() {
        return Operations.run(automaton(), EMPTY);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LabelMatcher label = (LabelMatcher) o;
        return matcher == label.matcher && Objects.equals(name, label.name) && Objects.equals(value, label.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, value, matcher);
    }

    @Override
    public String toString() {
        return name + matcher.value + value;
    }
}
