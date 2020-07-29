/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.xpack.runtimefields.AbstractLongScriptFieldScript;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public abstract class AbstractLongScriptFieldQueryTestCase<T extends AbstractLongScriptFieldQuery> extends AbstractScriptFieldQueryTestCase<
    T> {
    protected final CheckedFunction<LeafReaderContext, AbstractLongScriptFieldScript, IOException> leafFactory = ctx -> null;

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
