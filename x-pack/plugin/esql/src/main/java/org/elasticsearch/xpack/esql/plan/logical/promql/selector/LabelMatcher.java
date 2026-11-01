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
import org.elasticsearch.xpack.esql.core.tree.Node;
import org.elasticsearch.xpack.esql.core.tree.NodeStringMapper;
import org.elasticsearch.xpack.esql.core.tree.NodeStringRenderable;

import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.util.StringUtils.EMPTY;
import static org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher.Matcher.NEQ;
import static org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher.Matcher.NREG;

/**
 * PromQL label matcher between a label name, a value pattern and match type (=, !=, =~, !~).
 *
 * Examples:
 *   {job="api"}              -> [LabelMatcher("job", "api", EQ)]
 *   {status=~"5.."}          -> [LabelMatcher("status", "5..", REG)]
 *   {env!~"test|dev"}        -> [LabelMatcher("env", "test|dev", NREG)]
 *   {host=?_hosts}           -> [LabelMatcher("host", ["34.107.161.234", "140.248.133.94"], EQ)]
 */
public class LabelMatcher implements NodeStringRenderable {

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
    private final List<String> values;
    private final Matcher matcher;

    private Automaton automaton;

    public LabelMatcher(String name, String value, Matcher matcher) {
        this(name, List.of(value), matcher);
    }

    public LabelMatcher(String name, List<String> values, Matcher matcher) {
        this.name = name;
        this.values = values;
        this.matcher = matcher;
    }

    public String name() {
        return name;
    }

    /**
     * Returns the single value for this matcher.
     * For multi-value matchers, returns the first value.
     * Use {@link #values()} to get all values.
     */
    public String getFirstValue() {
        return values.getFirst();
    }

    /**
     * Returns all values for this matcher.
     * For single-value matchers, returns a singleton list.
     */
    public List<String> values() {
        return values;
    }

    /**
     * Returns true if this matcher has multiple values.
     */
    public boolean isMultiValue() {
        return values.size() > 1;
    }

    public Matcher matcher() {
        return matcher;
    }

    // TODO: externalize this to allow pluggable strategies (such as caching across labels/requests)
    public Automaton automaton() {
        if (automaton != null) {
            return automaton;
        }

        Automaton result;
        if (isMultiValue() && matcher.isRegex() == false) {
            // Multi-value exact match: union of all literal values
            List<Automaton> automata = values.stream().map(Automata::makeString).toList();
            result = Operations.union(automata);
        } else if (isMultiValue()) {
            // Multi-value regex: union of all regex patterns
            List<Automaton> automata = values.stream().map(v -> new RegExp(v).toAutomaton()).toList();
            result = Operations.union(automata);
        } else {
            // Single value
            String v = getFirstValue();
            try {
                result = matcher.isRegex() ? new RegExp(v).toAutomaton() : Automata.makeString(v);
            } catch (IllegalArgumentException ex) {
                throw new QlIllegalArgumentException(ex, "Cannot parse regex {}", v);
            }
        }
        result = MinimizationOperations.minimize(result, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        // negate if needed
        if (matcher == NEQ || matcher == NREG) {
            result = Operations.complement(result, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        }
        automaton = result;
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

    public boolean isNegation() {
        return matcher == NEQ || matcher == NREG;
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
        return matcher == label.matcher && Objects.equals(name, label.name) && Objects.equals(values, label.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, values, matcher);
    }

    @Override
    public String toString() {
        return name + matcher.value + values;
    }

    /**
     * Routes the label name and every match value through the mapper (the match operator is
     * structural and stays). Reproduces {@code toString()}'s {@code name op [v1, v2]} shape so that
     * under {@link NodeStringMapper#IDENTITY} this equals {@link #toString()}; under an anonymizing
     * mapper the label name and the (potentially sensitive) values tokenize.
     */
    @Override
    public void nodeString(StringBuilder sb, Node.NodeStringFormat format, NodeStringMapper mapper) {
        sb.append(mapper.column(name)).append(matcher.value).append('[');
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(mapper.column(values.get(i)));
        }
        sb.append(']');
    }
}
