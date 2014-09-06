/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.support;

import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.RegExp;

import java.util.Collection;

import static org.apache.lucene.util.automaton.MinimizationOperations.minimize;
import static org.apache.lucene.util.automaton.Operations.*;

/**
 *
 */
public final class Automatons {

    private Automatons() {
    }

    public static Automaton patterns(String... patterns) {
        if (patterns.length == 0) {
            return Automata.makeEmpty();
        }
        Automaton automaton = new RegExp(patterns[0]).toAutomaton();
        for (String pattern : patterns) {
            automaton = union(automaton, new RegExp(pattern).toAutomaton());
        }
        return determinize(minimize(automaton));
    }

    public static Automaton patterns(Collection<String> patterns) {
        if (patterns.isEmpty()) {
            return Automata.makeEmpty();
        }
        Automaton automaton = null;
        for (String pattern : patterns) {
            if (automaton == null) {
                automaton = new RegExp(pattern).toAutomaton();
            } else {
                automaton = union(automaton, new RegExp(pattern).toAutomaton());
            }
        }
        return determinize(minimize(automaton));
    }

    public static Automaton unionAndDeterminize(Automaton a1, Automaton a2) {
        return determinize(union(a1, a2));
    }

    public static Automaton minusAndDeterminize(Automaton a1, Automaton a2) {
        return determinize(minus(a1, a2));
    }
}
