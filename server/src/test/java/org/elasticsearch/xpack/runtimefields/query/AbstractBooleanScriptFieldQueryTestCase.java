/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.runtimefields.query;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.elasticsearch.xpack.runtimefields.mapper.BooleanFieldScript;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public abstract class AbstractBooleanScriptFieldQueryTestCase<T extends AbstractBooleanScriptFieldQuery> extends
    AbstractScriptFieldQueryTestCase<T> {

    protected final BooleanFieldScript.LeafFactory leafFactory = mock(BooleanFieldScript.LeafFactory.class);

    @Override
    public final void testVisit() {
        T query = createTestInstance();
        List<Query> leavesVisited = new ArrayList<>();
        query.visit(new QueryVisitor() {
            @Override
            public void consumeTerms(Query query, Term... terms) {
                fail();
            }

            @Override
            public void consumeTermsMatching(Query query, String field, Supplier<ByteRunAutomaton> automaton) {
                fail();
            }

            @Override
            public void visitLeaf(Query query) {
                leavesVisited.add(query);
            }
        });
        assertThat(leavesVisited, equalTo(List.of(query)));
    }
}
