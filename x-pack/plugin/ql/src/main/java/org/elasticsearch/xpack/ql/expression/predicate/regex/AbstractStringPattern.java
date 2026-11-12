/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.expression.predicate.regex;

import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.core.Nullable;

public abstract class AbstractStringPattern implements StringPattern {

    private Automaton automaton;
    private boolean compiled;

    /**
     * The pattern's automaton, or {@code null} when it is not worth building here. The optimizer only reads it to simplify
     * the expression, so without one the expression stays as it is and whatever evaluates it applies its own limits.
     */
    @Nullable
    public abstract Automaton createAutomaton();

    private Automaton automaton() {
        if (compiled == false) {
            automaton = createAutomaton();
            compiled = true;
        }
        return automaton;
    }

    @Override
    public boolean matchesAll() {
        Automaton a = automaton();
        return a != null && Operations.isTotal(a);
    }

    @Override
    public String exactMatch() {
        Automaton a = automaton();
        IntsRef singleton = a == null ? null : Operations.getSingleton(a);
        return singleton != null ? UnicodeUtil.newString(singleton.ints, singleton.offset, singleton.length) : null;
    }
}
