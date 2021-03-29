/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.runtime;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.elasticsearch.script.StringFieldScript;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;

public abstract class AbstractStringScriptFieldQueryTestCase<T extends AbstractStringScriptFieldQuery> extends
    AbstractScriptFieldQueryTestCase<T> {
    protected final StringFieldScript.LeafFactory leafFactory = mock(StringFieldScript.LeafFactory.class);

    /**
     * {@link Query#visit Visit} a query, collecting {@link ByteRunAutomaton automata},
     * failing if there are any terms or if there are more than one automaton.
     */
    protected final ByteRunAutomaton visitForSingleAutomata(T testQuery) {
        List<ByteRunAutomaton> automata = new ArrayList<>();
        testQuery.visit(new QueryVisitor() {
            @Override
            public void consumeTerms(Query query, Term... terms) {
                fail();
            }

            @Override
            public void consumeTermsMatching(Query query, String field, Supplier<ByteRunAutomaton> automaton) {
                assertThat(query, sameInstance(testQuery));
                assertThat(field, equalTo(testQuery.fieldName()));
                automata.add(automaton.get());
            }
        });
        assertThat(automata, hasSize(1));
        return automata.get(0);
    }
}
