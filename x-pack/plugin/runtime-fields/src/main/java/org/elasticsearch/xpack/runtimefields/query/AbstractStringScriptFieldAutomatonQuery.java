/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.query;

import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.elasticsearch.script.Script;
import org.elasticsearch.xpack.runtimefields.mapper.StringFieldScript;

import java.util.List;

public abstract class AbstractStringScriptFieldAutomatonQuery extends AbstractStringScriptFieldQuery {
    private final BytesRefBuilder scratch = new BytesRefBuilder();
    private final ByteRunAutomaton automaton;

    public AbstractStringScriptFieldAutomatonQuery(
        Script script,
        StringFieldScript.LeafFactory leafFactory,
        String fieldName,
        ByteRunAutomaton automaton
    ) {
        super(script, leafFactory, fieldName);
        this.automaton = automaton;
    }

    @Override
    protected final boolean matches(List<String> values) {
        for (String value : values) {
            scratch.copyChars(value);
            if (automaton.run(scratch.bytes(), 0, scratch.length())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public final void visit(QueryVisitor visitor) {
        if (visitor.acceptField(fieldName())) {
            visitor.consumeTermsMatching(this, fieldName(), () -> automaton);
        }
    }
}
