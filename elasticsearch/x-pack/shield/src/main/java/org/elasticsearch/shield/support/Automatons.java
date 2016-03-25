/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.support;

import dk.brics.automaton.Automaton;
import dk.brics.automaton.BasicAutomata;
import dk.brics.automaton.BasicOperations;
import dk.brics.automaton.RegExp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static dk.brics.automaton.BasicOperations.minus;
import static dk.brics.automaton.BasicOperations.union;
import static dk.brics.automaton.MinimizationOperations.minimize;

/**
 *
 */
public final class Automatons {

    public static final Automaton EMPTY = BasicAutomata.makeEmpty();

    static final char WILDCARD_STRING = '*';     // String equality with support for wildcards
    static final char WILDCARD_CHAR = '?';       // Char equality with support for wildcards
    static final char WILDCARD_ESCAPE = '\\';    // Escape character

    private Automatons() {
    }

    /**
     * Builds and returns an automaton that will represent the union of all the given patterns.
     */
    public static Automaton patterns(String... patterns) {
        return patterns(Arrays.asList(patterns));
    }

    /**
     * Builds and returns an automaton that will represent the union of all the given patterns.
     */
    public static Automaton patterns(Collection<String> patterns) {
        if (patterns.isEmpty()) {
            return BasicAutomata.makeEmpty();
        }
        Automaton automaton = null;
        for (String pattern : patterns) {
            if (automaton == null) {
                automaton = pattern(pattern);
            } else {
                automaton = union(automaton, pattern(pattern));
            }
        }
        minimize(automaton); // minimal is also deterministic
        return automaton;
    }

    /**
     * Builds and returns an automaton that represents the given pattern.
     */
    static Automaton pattern(String pattern) {
        if (pattern.startsWith("/")) { // it's a lucene regexp
            if (pattern.length() == 1 || !pattern.endsWith("/")) {
                throw new IllegalArgumentException("invalid pattern [" + pattern + "]. patterns starting with '/' " +
                        "indicate regular expression pattern and therefore must also end with '/'." +
                        " other patterns (those that do not start with '/') will be treated as simple wildcard patterns");
            }
            String regex = pattern.substring(1, pattern.length() - 1);
            return new RegExp(regex).toAutomaton();
        }
        return wildcard(pattern);
    }

    /**
     * Builds and returns an automaton that represents the given pattern.
     */
    @SuppressWarnings("fallthrough") // explicit fallthrough at end of switch
    static Automaton wildcard(String text) {
        List<Automaton> automata = new ArrayList<>();
        for (int i = 0; i < text.length();) {
            final char c = text.charAt(i);
            int length = 1;
            switch(c) {
                case WILDCARD_STRING:
                    automata.add(BasicAutomata.makeAnyString());
                    break;
                case WILDCARD_CHAR:
                    automata.add(BasicAutomata.makeAnyChar());
                    break;
                case WILDCARD_ESCAPE:
                    // add the next codepoint instead, if it exists
                    if (i + length < text.length()) {
                        final char nextChar = text.charAt(i + length);
                        length += 1;
                        automata.add(BasicAutomata.makeChar(nextChar));
                        break;
                    } // else fallthru, lenient parsing with a trailing \
                default:
                    automata.add(BasicAutomata.makeChar(c));
            }
            i += length;
        }
        return BasicOperations.concatenate(automata);
    }

    public static Automaton unionAndDeterminize(Automaton a1, Automaton a2) {
        Automaton res = union(a1, a2);
        res.determinize();
        return res;
    }

    public static Automaton minusAndDeterminize(Automaton a1, Automaton a2) {
        Automaton res = minus(a1, a2);
        res.determinize();
        return res;
    }
}
