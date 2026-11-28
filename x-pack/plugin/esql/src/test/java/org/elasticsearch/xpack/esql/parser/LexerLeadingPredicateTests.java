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
import org.antlr.v4.runtime.atn.ATNType;
import org.antlr.v4.runtime.atn.AtomTransition;
import org.antlr.v4.runtime.atn.BasicState;
import org.antlr.v4.runtime.atn.EpsilonTransition;
import org.antlr.v4.runtime.atn.PredicateTransition;
import org.antlr.v4.runtime.atn.RuleStartState;
import org.antlr.v4.runtime.atn.RuleStopState;
import org.antlr.v4.runtime.atn.RuleTransition;
import org.antlr.v4.runtime.atn.TokensStartState;
import org.antlr.v4.runtime.atn.Transition;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayDeque;
import java.util.BitSet;
import java.util.Deque;
import java.util.Set;
import java.util.TreeSet;

import static org.hamcrest.Matchers.contains;
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

        Set<String> offending = leadingPredicateRules(lexer.getATN(), lexer.getRuleNames());

        assertThat(
            "lexer rules evaluate a predicate before consuming input, which disables DFA start-state caching and slows every query. "
                + "Move the predicate after the literal, e.g. 'x' {pred}?. See the note in EsqlBaseLexer.g4",
            offending,
            empty()
        );
    }

    public void testPredicateAfterRuleThatCanMatchEmptyInputIsReported() {
        FixtureAtn fixture = new FixtureAtn(true);

        assertThat(leadingPredicateRules(fixture.atn, FixtureAtn.RULE_NAMES), contains("X (predicate in X)"));
    }

    public void testPredicateAfterRuleThatConsumesInputIsIgnored() {
        FixtureAtn fixture = new FixtureAtn(false);

        assertThat(leadingPredicateRules(fixture.atn, FixtureAtn.RULE_NAMES), empty());
    }

    public void testPredicateInsideRuleThatCanMatchEmptyInputNamesTheRuleHoldingIt() {
        FixtureAtn fixture = new FixtureAtn(true, true);

        assertThat(leadingPredicateRules(fixture.atn, FixtureAtn.RULE_NAMES), contains("X (predicate in PREFIX)"));
    }

    /**
     * Token rules that can evaluate a predicate before consuming input, each with the rule holding the predicate.
     */
    private static Set<String> leadingPredicateRules(ATN atn, String[] ruleNames) {
        boolean[] matchesEmpty = rulesMatchingEmptyInput(atn);
        Set<String> offending = new TreeSet<>();
        for (ATNState modeStart : atn.modeToStartState) {
            for (int i = 0; i < modeStart.getNumberOfTransitions(); i++) {
                ATNState ruleStart = modeStart.transition(i).target;
                Integer owner = predicateRuleBeforeInput(ruleStart, matchesEmpty);
                if (owner != null) {
                    offending.add(ruleNames[ruleStart.ruleIndex] + " (predicate in " + ruleNames[owner] + ")");
                }
            }
        }
        return offending;
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

    private static Integer predicateRuleBeforeInput(ATNState start, boolean[] matchesEmpty) {
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
                    return state.ruleIndex;
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
        return null;
    }

    /**
     * The ATN of {@code X : PREFIX {pred}? 'x'} with {@code fragment PREFIX : 'p'?} (or {@code 'p'} when it cannot match
     * empty input). With {@code predicateInPrefix}, the grammar is {@code X : PREFIX 'x'} and {@code PREFIX : {pred}? 'p'?}.
     */
    private static class FixtureAtn {
        static final String[] RULE_NAMES = { "X", "PREFIX" };

        final ATN atn = new ATN(ATNType.LEXER, 0);

        FixtureAtn(boolean prefixCanBeEmpty) {
            this(prefixCanBeEmpty, false);
        }

        FixtureAtn(boolean prefixCanBeEmpty, boolean predicateInPrefix) {
            atn.ruleToStartState = new RuleStartState[2];
            atn.ruleToStopState = new RuleStopState[2];
            TokensStartState modeStart = add(new TokensStartState(), -1);
            atn.modeToStartState.add(modeStart);

            RuleStartState xStart = ruleStart(0);
            RuleStartState prefixStart = ruleStart(1);
            modeStart.addTransition(new EpsilonTransition(xStart));

            ATNState prefixBody = add(new BasicState(), 1);
            ATNState prefixConsumed = add(new BasicState(), 1);
            ATNState prefixAfterPredicate = add(new BasicState(), 1);
            prefixStart.addTransition(new EpsilonTransition(prefixBody));
            ATNState prefixLiteralStart = prefixBody;
            if (predicateInPrefix) {
                prefixBody.addTransition(new PredicateTransition(prefixAfterPredicate, 1, 0, false));
                prefixLiteralStart = prefixAfterPredicate;
            }
            prefixLiteralStart.addTransition(new AtomTransition(prefixConsumed, 'p'));
            prefixConsumed.addTransition(new EpsilonTransition(atn.ruleToStopState[1]));
            if (prefixCanBeEmpty) {
                prefixLiteralStart.addTransition(new EpsilonTransition(atn.ruleToStopState[1]));
            }

            ATNState call = add(new BasicState(), 0);
            ATNState afterCall = add(new BasicState(), 0);
            ATNState afterPredicate = add(new BasicState(), 0);
            ATNState consumed = add(new BasicState(), 0);
            xStart.addTransition(new EpsilonTransition(call));
            call.addTransition(new RuleTransition(prefixStart, 1, 0, afterCall));
            ATNState literalStart = afterCall;
            if (predicateInPrefix == false) {
                afterCall.addTransition(new PredicateTransition(afterPredicate, 0, 0, false));
                literalStart = afterPredicate;
            }
            literalStart.addTransition(new AtomTransition(consumed, 'x'));
            consumed.addTransition(new EpsilonTransition(atn.ruleToStopState[0]));
        }

        private RuleStartState ruleStart(int rule) {
            RuleStartState start = add(new RuleStartState(), rule);
            RuleStopState stop = add(new RuleStopState(), rule);
            start.stopState = stop;
            atn.ruleToStartState[rule] = start;
            atn.ruleToStopState[rule] = stop;
            return start;
        }

        private <T extends ATNState> T add(T state, int rule) {
            state.ruleIndex = rule;
            atn.addState(state);
            return state;
        }
    }
}
