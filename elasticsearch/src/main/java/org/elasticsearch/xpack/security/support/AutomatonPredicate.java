/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.support;

import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;

import java.util.function.Predicate;

import static org.apache.lucene.util.automaton.Operations.DEFAULT_MAX_DETERMINIZED_STATES;

public class AutomatonPredicate implements Predicate<String> {

    private final CharacterRunAutomaton automaton;

    public AutomatonPredicate(Automaton automaton) {
        this.automaton = new CharacterRunAutomaton(automaton, DEFAULT_MAX_DETERMINIZED_STATES);
    }

    @Override
    public boolean test(String input) {
        return automaton.run(input);
    }
}
