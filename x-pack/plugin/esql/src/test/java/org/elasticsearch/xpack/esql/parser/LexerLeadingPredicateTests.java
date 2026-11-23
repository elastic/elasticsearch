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
import org.antlr.v4.runtime.atn.Transition;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayDeque;
import java.util.BitSet;
import java.util.Deque;
import java.util.Set;
import java.util.TreeSet;

/**
 * A semantic predicate evaluated before a lexer rule consumes any character, as in
 * {@code DEV_X : {this.isDevVersion()}? 'x'}, stops ANTLR from caching the DFA start state of the lexer mode.
 * Every query is then lexed through ATN simulation, including those that never use the token.
 * This test fails when any {@link EsqlBaseLexer} rule can reach a predicate without consuming a character.
 * The predicate belongs after the literal: {@code DEV_X : 'x' {this.isDevVersion()}?}.
 */
public class LexerLeadingPredicateTests extends ESTestCase {

    public void testNoRuleEvaluatesAPredicateBeforeConsumingInput() {
        EsqlBaseLexer lexer = new EsqlBaseLexer(CharStreams.fromString(""));
        ATN atn = lexer.getATN();
        String[] ruleNames = lexer.getRuleNames();

        Set<String> offending = new TreeSet<>();
        for (ATNState modeStart : atn.modeToStartState) {
            findLeadingPredicates(modeStart, ruleNames, offending);
        }

        assertTrue(
            "lexer rules evaluate a predicate before consuming input, which disables DFA start-state caching and slows every query. "
                + "Move the predicate after the literal, e.g. 'x' {pred}?. See the note in EsqlBaseLexer.g4. Rules: "
                + offending,
            offending.isEmpty()
        );
    }

    private static void findLeadingPredicates(ATNState start, String[] ruleNames, Set<String> offending) {
        BitSet visited = new BitSet();
        Deque<ATNState> pending = new ArrayDeque<>();
        pending.push(start);
        while (pending.isEmpty() == false) {
            ATNState state = pending.pop();
            if (visited.get(state.stateNumber) || state instanceof RuleStopState) {
                continue;
            }
            visited.set(state.stateNumber);
            for (int i = 0; i < state.getNumberOfTransitions(); i++) {
                Transition transition = state.transition(i);
                if (transition instanceof PredicateTransition) {
                    offending.add(ruleNames[state.ruleIndex]);
                } else if (transition.isEpsilon()) {
                    pending.push(transition.target);
                }
            }
        }
    }
}
