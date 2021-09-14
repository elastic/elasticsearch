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
import org.elasticsearch.script.Script;
import org.elasticsearch.search.runtime.AbstractScriptFieldQuery;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public abstract class AbstractScriptFieldQueryTestCase<T extends AbstractScriptFieldQuery<?>> extends ESTestCase {
    protected abstract T createTestInstance();

    protected abstract T copy(T orig);

    protected abstract T mutate(T orig);

    protected final Script randomScript() {
        return new Script(randomAlphaOfLength(10));
    }

    public final void testEqualsAndHashCode() {
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(createTestInstance(), this::copy, this::mutate);
    }

    public abstract void testMatches() throws IOException;

    public final void testToString() {
        T query = createTestInstance();
        assertThat(query.toString(), equalTo(query.fieldName() + ":" + query.toString(query.fieldName())));
        assertToString(query);
    }

    protected abstract void assertToString(T query);

    public abstract void testVisit();

    protected final void assertEmptyVisit() {
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
