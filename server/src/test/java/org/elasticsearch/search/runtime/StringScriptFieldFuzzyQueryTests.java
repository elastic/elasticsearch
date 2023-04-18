/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.runtime;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.elasticsearch.script.Script;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class StringScriptFieldFuzzyQueryTests extends AbstractStringScriptFieldQueryTestCase<StringScriptFieldFuzzyQuery> {
    @Override
    protected StringScriptFieldFuzzyQuery createTestInstance() {
        String term = randomAlphaOfLength(6);
        return StringScriptFieldFuzzyQuery.build(
            randomScript(),
            leafFactory,
            randomAlphaOfLength(5),
            term,
            randomIntBetween(0, 2),
            randomIntBetween(0, term.length()),
            randomBoolean()
        );
    }

    @Override
    protected StringScriptFieldFuzzyQuery copy(StringScriptFieldFuzzyQuery orig) {
        return StringScriptFieldFuzzyQuery.build(
            orig.script(),
            leafFactory,
            orig.fieldName(),
            orig.delegate().getTerm().bytes().utf8ToString(),
            orig.delegate().getMaxEdits(),
            orig.delegate().getPrefixLength(),
            orig.delegate().getTranspositions()
        );
    }

    @Override
    protected StringScriptFieldFuzzyQuery mutate(StringScriptFieldFuzzyQuery orig) {
        Script script = orig.script();
        String fieldName = orig.fieldName();
        String term = orig.delegate().getTerm().bytes().utf8ToString();
        int maxEdits = orig.delegate().getMaxEdits();
        int prefixLength = orig.delegate().getPrefixLength();
        boolean transpositions = orig.delegate().getTranspositions();
        switch (randomInt(5)) {
            case 0 -> script = randomValueOtherThan(script, this::randomScript);
            case 1 -> fieldName += "modified";
            case 2 -> term += "modified";
            case 3 -> maxEdits = randomValueOtherThan(maxEdits, () -> randomIntBetween(0, 2));
            case 4 -> prefixLength += 1;
            case 5 -> transpositions = transpositions == false;
            default -> fail();
        }
        return StringScriptFieldFuzzyQuery.build(script, leafFactory, fieldName, term, maxEdits, prefixLength, transpositions);
    }

    @Override
    public void testMatches() {
        StringScriptFieldFuzzyQuery query = StringScriptFieldFuzzyQuery.build(randomScript(), leafFactory, "test", "foo", 1, 0, false);
        assertTrue(query.matches(List.of("foo")));
        assertTrue(query.matches(List.of("foa")));
        assertTrue(query.matches(List.of("foo", "bar")));
        assertFalse(query.matches(List.of("bar")));
        query = StringScriptFieldFuzzyQuery.build(randomScript(), leafFactory, "test", "foo", 0, 0, false);
        assertTrue(query.matches(List.of("foo")));
        assertFalse(query.matches(List.of("foa")));
        query = StringScriptFieldFuzzyQuery.build(randomScript(), leafFactory, "test", "foo", 2, 0, false);
        assertTrue(query.matches(List.of("foo")));
        assertTrue(query.matches(List.of("foa")));
        assertTrue(query.matches(List.of("faa")));
        assertFalse(query.matches(List.of("faaa")));
    }

    @Override
    protected void assertToString(StringScriptFieldFuzzyQuery query) {
        assertThat(
            query.toString(query.fieldName()),
            equalTo(query.delegate().getTerm().bytes().utf8ToString() + "~" + query.delegate().getMaxEdits())
        );
    }

    @Override
    public void testVisit() {
        StringScriptFieldFuzzyQuery query = createTestInstance();
        ByteRunAutomaton automaton = visitForSingleAutomata(query);
        BytesRef term = query.delegate().getTerm().bytes();
        assertThat(automaton.run(term.bytes, term.offset, term.length), is(true));
        term = new BytesRef(query.delegate().getTerm().bytes().utf8ToString() + "a");
        assertThat(
            automaton.run(term.bytes, term.offset, term.length),
            is(query.delegate().getMaxEdits() > 0 && query.delegate().getPrefixLength() < term.length)
        );
        term = new BytesRef(query.delegate().getTerm().bytes().utf8ToString() + "abba");
        assertThat(automaton.run(term.bytes, term.offset, term.length), is(false));
    }
}
