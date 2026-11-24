/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.runtime;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.script.Script;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class StringScriptFieldAutomatonQueryTests extends AbstractStringScriptFieldQueryTestCase<StringScriptFieldAutomatonQuery> {

    @Override
    protected StringScriptFieldAutomatonQuery createTestInstance() {
        String value = randomAlphaOfLength(6);
        return new StringScriptFieldAutomatonQuery(randomScript(), leafFactory, randomAlphaOfLength(5), prefix(value), value + ".*");
    }

    @Override
    protected StringScriptFieldAutomatonQuery copy(StringScriptFieldAutomatonQuery orig) {
        String value = orig.description().substring(0, orig.description().length() - 2);
        return new StringScriptFieldAutomatonQuery(orig.script(), leafFactory, orig.fieldName(), prefix(value), orig.description());
    }

    @Override
    protected StringScriptFieldAutomatonQuery mutate(StringScriptFieldAutomatonQuery orig) {
        Script script = orig.script();
        String fieldName = orig.fieldName();
        String value = orig.description().substring(0, orig.description().length() - 2);
        String description = orig.description();
        switch (randomInt(3)) {
            case 0 -> script = randomValueOtherThan(script, this::randomScript);
            case 1 -> fieldName += "modified";
            // the automaton alone, as two callers may describe different automata alike
            case 2 -> value += "modified";
            case 3 -> description += "modified";
            default -> fail();
        }
        return new StringScriptFieldAutomatonQuery(script, leafFactory, fieldName, prefix(value), description);
    }

    @Override
    public void testMatches() {
        StringScriptFieldAutomatonQuery query = new StringScriptFieldAutomatonQuery(
            randomScript(),
            leafFactory,
            "test",
            Operations.determinize(Operations.union(List.of(Automata.makeString("cat"), Automata.makeString("été"))), 10_000),
            "cat|été"
        );
        BytesRefBuilder scratch = new BytesRefBuilder();
        assertTrue(query.matches(List.of("cat"), scratch));
        assertTrue(query.matches(List.of("été"), scratch));
        assertFalse(query.matches(List.of("cats"), scratch));
        assertFalse(query.matches(List.of("ete"), scratch));
        assertTrue(query.matches(List.of("dog", "cat"), scratch));
    }

    @Override
    protected void assertToString(StringScriptFieldAutomatonQuery query) {
        assertThat(query.toString(query.fieldName()), equalTo(query.description()));
        assertThat(query.toString("other"), equalTo(query.fieldName() + ":" + query.description()));
    }

    @Override
    public void testVisit() {
        StringScriptFieldAutomatonQuery query = new StringScriptFieldAutomatonQuery(
            randomScript(),
            leafFactory,
            "test",
            prefix("a"),
            "a.*"
        );
        ByteRunAutomaton automaton = visitForSingleAutomata(query);
        BytesRef term = new BytesRef("astuff");
        assertThat(automaton.run(term.bytes, term.offset, term.length), is(true));
        term = new BytesRef("stuff");
        assertThat(automaton.run(term.bytes, term.offset, term.length), is(false));
    }

    private static Automaton prefix(String value) {
        return Operations.determinize(Operations.concatenate(List.of(Automata.makeString(value), Automata.makeAnyString())), 10_000);
    }
}
