/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.query;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.elasticsearch.script.Script;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class StringScriptFieldPrefixQueryTests extends AbstractStringScriptFieldQueryTestCase<StringScriptFieldPrefixQuery> {
    @Override
    protected StringScriptFieldPrefixQuery createTestInstance() {
        return new StringScriptFieldPrefixQuery(randomScript(), leafFactory, randomAlphaOfLength(5), randomAlphaOfLength(6));
    }

    @Override
    protected StringScriptFieldPrefixQuery copy(StringScriptFieldPrefixQuery orig) {
        return new StringScriptFieldPrefixQuery(orig.script(), leafFactory, orig.fieldName(), orig.prefix());
    }

    @Override
    protected StringScriptFieldPrefixQuery mutate(StringScriptFieldPrefixQuery orig) {
        Script script = orig.script();
        String fieldName = orig.fieldName();
        String prefix = orig.prefix();
        switch (randomInt(2)) {
            case 0:
                script = randomValueOtherThan(script, this::randomScript);
                break;
            case 1:
                fieldName += "modified";
                break;
            case 2:
                prefix += "modified";
                break;
            default:
                fail();
        }
        return new StringScriptFieldPrefixQuery(script, leafFactory, fieldName, prefix);
    }

    @Override
    public void testMatches() {
        StringScriptFieldPrefixQuery query = new StringScriptFieldPrefixQuery(randomScript(), leafFactory, "test", "foo");
        assertTrue(query.matches(List.of("foo")));
        assertTrue(query.matches(List.of("foooo")));
        assertFalse(query.matches(List.of("fo")));
        assertTrue(query.matches(List.of("fo", "foo")));
    }

    @Override
    protected void assertToString(StringScriptFieldPrefixQuery query) {
        assertThat(query.toString(query.fieldName()), equalTo(query.prefix()));
    }

    @Override
    public void testVisit() {
        StringScriptFieldPrefixQuery query = createTestInstance();
        ByteRunAutomaton automaton = visitForSingleAutomata(query);
        BytesRef term = new BytesRef(query.prefix());
        assertThat(automaton.run(term.bytes, term.offset, term.length), is(true));
        term = new BytesRef(query.prefix() + "a");
        assertThat(automaton.run(term.bytes, term.offset, term.length), is(true));
        term = new BytesRef("a" + query.prefix());
        assertThat(automaton.run(term.bytes, term.offset, term.length), is(false));
    }
}
