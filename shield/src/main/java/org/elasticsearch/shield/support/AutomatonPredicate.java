/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.support;

import com.google.common.base.Predicate;
import dk.brics.automaton.Automaton;
import dk.brics.automaton.RunAutomaton;

/**
*
*/
public class AutomatonPredicate implements Predicate<String> {

    private final RunAutomaton automaton;

    public AutomatonPredicate(Automaton automaton) {
        this(new RunAutomaton(automaton, false));
    }

    public AutomatonPredicate(RunAutomaton automaton) {
        this.automaton = automaton;
    }

    @Override
    public boolean apply(String input) {
        return automaton.run(input);
    }
}
