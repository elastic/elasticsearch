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

public abstract class AbstractStringPattern implements StringPattern {

    private Automaton automaton;

    public abstract Automaton createAutomaton(boolean ignoreCase);

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
        IntsRef singleton = Operations.getSingleton(automaton());
        return singleton != null ? UnicodeUtil.newString(singleton.ints, singleton.offset, singleton.length) : null;
    }
}
