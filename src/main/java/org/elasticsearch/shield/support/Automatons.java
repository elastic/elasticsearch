/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.support;

import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.BasicAutomata;
import org.apache.lucene.util.automaton.MinimizationOperations;
import org.apache.lucene.util.automaton.RegExp;

import java.util.Collection;

/**
 *
 */
public final class Automatons {

    private Automatons() {
    }

    public static Automaton patterns(String... patterns) {
        if (patterns.length == 0) {
            return BasicAutomata.makeEmpty();
        }
        Automaton automaton = new RegExp(patterns[0]).toAutomaton();
        for (String pattern : patterns) {
            automaton = automaton.union(new RegExp(pattern).toAutomaton());
        }
        MinimizationOperations.minimize(automaton);
        return automaton;
    }

    public static Automaton patterns(Collection<String> patterns) {
        if (patterns.isEmpty()) {
            return BasicAutomata.makeEmpty();
        }
        Automaton automaton = null;
        for (String pattern : patterns) {
            if (automaton == null) {
                automaton = new RegExp(pattern).toAutomaton();
            } else {
                automaton = automaton.union(new RegExp(pattern).toAutomaton());
            }
        }
        MinimizationOperations.minimize(automaton);
        return automaton;
    }
}
