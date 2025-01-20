/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class AutomatonFieldPredicateTests extends ESTestCase {
    public void testMatching() {
        String str = randomAlphaOfLength(10);
        Automaton a = Automata.makeString(str);
        AutomatonFieldPredicate pred = new AutomatonFieldPredicate(a, new CharacterRunAutomaton(a));
        assertTrue(pred.test(str));
        assertFalse(pred.test(str + randomAlphaOfLength(1)));
    }

    public void testHash() {
        Automaton a = Automata.makeString("a");
        AutomatonFieldPredicate predA = new AutomatonFieldPredicate(a, new CharacterRunAutomaton(a));

        Automaton b = Automata.makeString("b");
        AutomatonFieldPredicate predB = new AutomatonFieldPredicate(b, new CharacterRunAutomaton(b));

        assertThat(predA.modifyHash("a"), not(equalTo(predB.modifyHash("a"))));
    }
}
