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

import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class StringScriptFieldExistsQueryTests extends AbstractStringScriptFieldQueryTestCase<StringScriptFieldExistsQuery> {
    @Override
    protected StringScriptFieldExistsQuery createTestInstance() {
        return new StringScriptFieldExistsQuery(randomScript(), leafFactory, randomAlphaOfLength(5));
    }

    @Override
    protected StringScriptFieldExistsQuery copy(StringScriptFieldExistsQuery orig) {
        return new StringScriptFieldExistsQuery(orig.script(), leafFactory, orig.fieldName());
    }

    @Override
    protected StringScriptFieldExistsQuery mutate(StringScriptFieldExistsQuery orig) {
        if (randomBoolean()) {
            new StringScriptFieldExistsQuery(randomValueOtherThan(orig.script(), this::randomScript), leafFactory, orig.fieldName());
        }
        return new StringScriptFieldExistsQuery(orig.script(), leafFactory, orig.fieldName() + "modified");
    }

    @Override
    public void testMatches() {
        assertTrue(createTestInstance().matches(List.of("test")));
        assertFalse(createTestInstance().matches(List.of()));
    }

    @Override
    protected void assertToString(StringScriptFieldExistsQuery query) {
        assertThat(query.toString(query.fieldName()), equalTo("StringScriptFieldExistsQuery"));
    }

    @Override
    public void testVisit() {
        createTestInstance().visit(new QueryVisitor() {
            @Override
            public void consumeTerms(Query query, Term... terms) {
                fail();
            }

            @Override
            public void consumeTermsMatching(Query query, String field, Supplier<ByteRunAutomaton> automaton) {
                fail();
            }
        });
    }
}
