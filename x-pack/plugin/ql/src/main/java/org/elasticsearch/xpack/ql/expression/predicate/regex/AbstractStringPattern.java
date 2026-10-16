/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression.predicate.regex;

import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.lucene.search.cost.RegexpNfaRamEstimator;
import org.elasticsearch.lucene.util.automaton.CircuitBreakingOperations;
import org.elasticsearch.xpack.ql.InvalidArgumentException;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractStringPattern implements StringPattern {

    /**
     * The bound the {@code regexp} query applies through {@code index.max_regex_length}, at its default. The optimizer compiles
     * a pattern before any index is consulted and for commands that never reach one, so it has no setting to read.
     */
    public static final int MAX_PATTERN_LENGTH = IndexSettings.MAX_REGEX_LENGTH_SETTING.getDefault(Settings.EMPTY);

    private Automaton automaton;

    /**
     * Compiles the pattern. Runs on a coordinator thread during optimization, where an {@link Error} takes the node down, so
     * Lucene's recursion on nesting and its determinization limit both surface as client errors instead.
     */
    public final Automaton createAutomaton() {
        try {
            return doCreateAutomaton();
        } catch (TooComplexToDeterminizeException e) {
            throw new InvalidArgumentException(e, "Pattern was too complex to determinize");
        } catch (StackOverflowError e) {
            throw new InvalidArgumentException("Pattern nesting is too deep to evaluate");
        }
    }

    protected abstract Automaton doCreateAutomaton();

    /** A pattern longer than the {@code regexp} query would accept is rejected before anything is parsed or built. */
    protected static void checkLength(String pattern) {
        if (pattern.length() > MAX_PATTERN_LENGTH) {
            throw new InvalidArgumentException(
                "Pattern length [" + pattern.length() + "] exceeds the allowed maximum of [" + MAX_PATTERN_LENGTH + "]"
            );
        }
    }

    /**
     * {@code WildcardQuery.toAutomaton} with its size estimated first and its determinization charged: a run of {@code *}
     * concatenates automata that accept the empty string, which is quadratic in transitions like a nested regex repeat.
     */
    protected static Automaton compileWildcard(String wildcard) {
        checkLength(wildcard);
        AutomatonBudget budget = new AutomatonBudget();
        budget.addEstimateBytesAndMaybeBreak(RegexpNfaRamEstimator.estimateWildcardRamBytes(wildcard), "wildcard");
        List<Automaton> automata = new ArrayList<>();
        for (int i = 0; i < wildcard.length();) {
            int c = wildcard.codePointAt(i);
            i += Character.charCount(c);
            switch (c) {
                case '*' -> automata.add(Automata.makeAnyString());
                case '?' -> automata.add(Automata.makeAnyChar());
                case '\\' -> {
                    if (i < wildcard.length()) {
                        int escaped = wildcard.codePointAt(i);
                        i += Character.charCount(escaped);
                        automata.add(Automata.makeChar(escaped));
                    } else {
                        automata.add(Automata.makeChar(c));
                    }
                }
                default -> automata.add(Automata.makeChar(c));
            }
        }
        return CircuitBreakingOperations.determinize(
            Operations.concatenate(automata),
            Operations.DEFAULT_DETERMINIZE_WORK_LIMIT,
            budget,
            "wildcard"
        );
    }

    private Automaton automaton() {
        if (automaton == null) {
            automaton = createAutomaton();
        }
        return automaton;
    }

    @Override
    public boolean matchesAll() {
        return Operations.isTotal(automaton());
    }

    @Override
    public String exactMatch() {
        IntsRef singleton = Operations.getSingleton(automaton());
        return singleton != null ? UnicodeUtil.newString(singleton.ints, singleton.offset, singleton.length) : null;
    }
}
