/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.atn.ATN;
import org.antlr.v4.runtime.atn.ATNState;
import org.antlr.v4.runtime.atn.PredicateTransition;
import org.antlr.v4.runtime.atn.RuleStopState;
import org.antlr.v4.runtime.atn.RuleTransition;
import org.antlr.v4.runtime.atn.Transition;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayDeque;
import java.util.BitSet;
import java.util.Deque;
import java.util.Set;
import java.util.TreeSet;

import static org.hamcrest.Matchers.empty;

/**
 * A semantic predicate evaluated before a lexer rule consumes any character, as in
 * {@code DEV_X : {this.isDevVersion()}? 'x'}, stops ANTLR from caching the DFA start state of the lexer mode.
 * Every query is then lexed through ATN simulation, including those that never use the token.
 * This test fails when any {@link EsqlBaseLexer} rule can reach a predicate without consuming a character.
 * The predicate belongs after the literal: {@code DEV_X : 'x' {this.isDevVersion()}?}.
 * <p>
 * Only start-state caching is checked. A predicate after the first character only stops caching the DFA edge
 * where it is evaluated, which is acceptable for keywords but not for common symbols (see {@code EsqlBaseLexer.g4}).
 */
public class LexerLeadingPredicateTests extends ESTestCase {

    public void testNoRuleEvaluatesAPredicateBeforeConsumingInput() {
        EsqlBaseLexer lexer = new EsqlBaseLexer(CharStreams.fromString(""));
        ATN atn = lexer.getATN();
        String[] ruleNames = lexer.getRuleNames();
        boolean[] matchesEmpty = rulesMatchingEmptyInput(atn);

        Set<String> offending = new TreeSet<>();
        for (ATNState modeStart : atn.modeToStartState) {
            for (int i = 0; i < modeStart.getNumberOfTransitions(); i++) {
                ATNState ruleStart = modeStart.transition(i).target;
                if (reachesPredicateBeforeInput(ruleStart, matchesEmpty)) {
                    offending.add(ruleNames[ruleStart.ruleIndex]);
                }
            }
        }

        assertThat(
            "lexer rules evaluate a predicate before consuming input, which disables DFA start-state caching and slows every query. "
                + "Move the predicate after the literal, e.g. 'x' {pred}?. See the note in EsqlBaseLexer.g4",
            offending,
            empty()
        );
    }

    /**
     * Rules that can finish without consuming a character. Whatever follows a call to such a rule is also reachable
     * before any input is consumed.
     */
    private static boolean[] rulesMatchingEmptyInput(ATN atn) {
        boolean[] matchesEmpty = new boolean[atn.ruleToStartState.length];
        boolean changed = true;
        while (changed) {
            changed = false;
            for (int rule = 0; rule < matchesEmpty.length; rule++) {
                if (matchesEmpty[rule] == false && reachesRuleStop(atn.ruleToStartState[rule], rule, matchesEmpty)) {
                    matchesEmpty[rule] = true;
                    changed = true;
                }
            }
        }
        return matchesEmpty;
    }

    private static boolean reachesRuleStop(ATNState start, int rule, boolean[] matchesEmpty) {
        BitSet visited = new BitSet();
        Deque<ATNState> pending = new ArrayDeque<>();
        pending.push(start);
        while (pending.isEmpty() == false) {
            ATNState state = pending.pop();
            if (state instanceof RuleStopState) {
                if (state.ruleIndex == rule) {
                    return true;
                }
                continue;
            }
            if (visited.get(state.stateNumber)) {
                continue;
            }
            visited.set(state.stateNumber);
            for (int i = 0; i < state.getNumberOfTransitions(); i++) {
                Transition transition = state.transition(i);
                if (transition instanceof RuleTransition call) {
                    if (matchesEmpty[call.target.ruleIndex]) {
                        pending.push(call.followState);
                    }
                } else if (transition.isEpsilon()) {
                    pending.push(transition.target);
                }
            }
        }
        return false;
    }

    private static boolean reachesPredicateBeforeInput(ATNState start, boolean[] matchesEmpty) {
        BitSet visited = new BitSet();
        Deque<ATNState> pending = new ArrayDeque<>();
        pending.push(start);
        while (pending.isEmpty() == false) {
            ATNState state = pending.pop();
            if (state instanceof RuleStopState || visited.get(state.stateNumber)) {
                continue;
            }
            visited.set(state.stateNumber);
            for (int i = 0; i < state.getNumberOfTransitions(); i++) {
                Transition transition = state.transition(i);
                if (transition instanceof PredicateTransition) {
                    return true;
                }
                if (transition instanceof RuleTransition call) {
                    pending.push(call.target);
                    if (matchesEmpty[call.target.ruleIndex]) {
                        pending.push(call.followState);
                    }
                } else if (transition.isEpsilon()) {
                    pending.push(transition.target);
                }
            }
        }
        return false;
    }
}
