/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.expression.predicate.regex;

import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;

abstract class AbstractStringPattern implements StringPattern {

    private Automaton automaton;

    abstract Automaton createAutomaton();

    private Automaton automaton() {
        if (automaton == null) {
            automaton = createAutomaton();
        }
        return automaton;
    }

    @Override
    public boolean matchesAll() {
        return Operations.isTotal(automaton());
    }

    @Override
    public boolean isExactMatch() {
        return Operations.getCommonPrefix(automaton()).equals(asString());
    }
}
