/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression.predicate.regex;

import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;

public abstract class AbstractStringPattern implements StringPattern {

    private Automaton automaton;

    public final Automaton createAutomaton(boolean ignoreCase) {
        try {
            return doCreateAutomaton(ignoreCase);
        } catch (TooComplexToDeterminizeException e) {
            throw new IllegalArgumentException("Pattern was too complex to determinize", e);
        }
    }

    protected abstract Automaton doCreateAutomaton(boolean ignoreCase);

    private Automaton automaton() {
        if (automaton == null) {
            automaton = createAutomaton(false);
        }
        return automaton;
    }

    @Override
    public boolean matchesAll() {
        return Operations.isTotal(automaton());
    }

    @Override
    public String exactMatch() {
        Automaton a = automaton();
        if (a.getNumStates() == 0) { // workaround for https://github.com/elastic/elasticsearch/pull/128887
            return null; // Empty automaton has no matches
        }
        IntsRef singleton = Operations.getSingleton(a);
        return singleton != null ? UnicodeUtil.newString(singleton.ints, singleton.offset, singleton.length) : null;
    }
}
