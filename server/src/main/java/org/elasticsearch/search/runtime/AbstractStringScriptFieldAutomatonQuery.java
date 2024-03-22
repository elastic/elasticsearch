/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.runtime;

import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.StringFieldScript;

import java.util.List;

public abstract class AbstractStringScriptFieldAutomatonQuery extends AbstractStringScriptFieldQuery {
    private final ThreadLocal<BytesRefBuilder> bytesRefBuilderThreadLocal = ThreadLocal.withInitial(BytesRefBuilder::new);
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
            BytesRefBuilder scratch = bytesRefBuilderThreadLocal.get();
            scratch.copyChars(value);
            System.out.println("copyChars - " + value + " - " + Thread.currentThread().getName());
            if (automaton.run(scratch.bytes(), 0, scratch.length())) {
                System.out.println("match " + Thread.currentThread().getName());
                return true;
            }
        }
        System.out.println("no match " + Thread.currentThread().getName());
        return false;
    }

    @Override
    public final void visit(QueryVisitor visitor) {
        if (visitor.acceptField(fieldName())) {
            visitor.consumeTermsMatching(this, fieldName(), () -> automaton);
        }
    }
}
