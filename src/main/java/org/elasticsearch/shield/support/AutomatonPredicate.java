/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.support;

import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.elasticsearch.common.base.Predicate;

/**
*
*/
public class AutomatonPredicate implements Predicate<String> {

    private final CharacterRunAutomaton automaton;

    public AutomatonPredicate(Automaton automaton) {
        this(new CharacterRunAutomaton(automaton));
    }

    public AutomatonPredicate(CharacterRunAutomaton automaton) {
        this.automaton = automaton;
    }

    @Override
    public boolean apply(String input) {
        return automaton.run(input);
    }
}
