/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.runtime;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.elasticsearch.script.StringFieldScript;
import org.elasticsearch.script.Script;

import java.util.Objects;

public class StringScriptFieldFuzzyQuery extends AbstractStringScriptFieldAutomatonQuery {
    public static StringScriptFieldFuzzyQuery build(
        Script script,
        StringFieldScript.LeafFactory leafFactory,
        String fieldName,
        String term,
        int maxEdits,
        int prefixLength,
        boolean transpositions
    ) {
        int maxExpansions = 1; // We don't actually expand anything so the value here doesn't matter
        FuzzyQuery delegate = new FuzzyQuery(new Term(fieldName, term), maxEdits, prefixLength, maxExpansions, transpositions);
        ByteRunAutomaton automaton = delegate.getAutomata().runAutomaton;
        return new StringScriptFieldFuzzyQuery(script, leafFactory, fieldName, automaton, delegate);
    }

    private final FuzzyQuery delegate;

    private StringScriptFieldFuzzyQuery(
        Script script,
        StringFieldScript.LeafFactory leafFactory,
        String fieldName,
        ByteRunAutomaton automaton,
        FuzzyQuery delegate
    ) {
        super(script, leafFactory, fieldName, automaton);
        this.delegate = delegate;
    }

    @Override
    public final String toString(String field) {
        return delegate.toString(field);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), delegate);
    }

    @Override
    public boolean equals(Object obj) {
        if (false == super.equals(obj)) {
            return false;
        }
        StringScriptFieldFuzzyQuery other = (StringScriptFieldFuzzyQuery) obj;
        return delegate.equals(other.delegate);
    }

    FuzzyQuery delegate() {
        return delegate;
    }
}
