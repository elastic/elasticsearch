/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.query;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.script.Script;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class StringScriptFieldRegexpQueryTests extends AbstractStringScriptFieldQueryTestCase<StringScriptFieldRegexpQuery> {
    @Override
    protected StringScriptFieldRegexpQuery createTestInstance() {
        return new StringScriptFieldRegexpQuery(
            randomScript(),
            leafFactory,
            randomAlphaOfLength(5),
            randomAlphaOfLength(6),
            randomInt(RegExp.ALL),
            Operations.DEFAULT_MAX_DETERMINIZED_STATES
        );
    }

    @Override
    protected StringScriptFieldRegexpQuery copy(StringScriptFieldRegexpQuery orig) {
        return new StringScriptFieldRegexpQuery(
            orig.script(),
            leafFactory,
            orig.fieldName(),
            orig.pattern(),
            orig.flags(),
            Operations.DEFAULT_MAX_DETERMINIZED_STATES
        );
    }

    @Override
    protected StringScriptFieldRegexpQuery mutate(StringScriptFieldRegexpQuery orig) {
        Script script = orig.script();
        String fieldName = orig.fieldName();
        String pattern = orig.pattern();
        int flags = orig.flags();
        switch (randomInt(3)) {
            case 0:
                script = randomValueOtherThan(script, this::randomScript);
                break;
            case 1:
                fieldName += "modified";
                break;
            case 2:
                pattern += "modified";
                break;
            case 3:
                flags = randomValueOtherThan(flags, () -> randomInt(0xFFFF));
                break;
            default:
                fail();
        }
        return new StringScriptFieldRegexpQuery(script, leafFactory, fieldName, pattern, flags, Operations.DEFAULT_MAX_DETERMINIZED_STATES);
    }

    @Override
    public void testMatches() {
        StringScriptFieldRegexpQuery query = new StringScriptFieldRegexpQuery(
            randomScript(),
            leafFactory,
            "test",
            "a.+b",
            0,
            Operations.DEFAULT_MAX_DETERMINIZED_STATES
        );
        assertTrue(query.matches(List.of("astuffb")));
        assertFalse(query.matches(List.of("fffff")));
        assertFalse(query.matches(List.of("ab")));
        assertFalse(query.matches(List.of("aasdf")));
        assertFalse(query.matches(List.of("dsfb")));
        assertTrue(query.matches(List.of("astuffb", "fffff")));
    }

    @Override
    protected void assertToString(StringScriptFieldRegexpQuery query) {
        assertThat(query.toString(query.fieldName()), equalTo("/" + query.pattern() + "/"));
    }

    @Override
    public void testVisit() {
        StringScriptFieldRegexpQuery query = new StringScriptFieldRegexpQuery(
            randomScript(),
            leafFactory,
            "test",
            "a.+b",
            0,
            Operations.DEFAULT_MAX_DETERMINIZED_STATES
        );
        ByteRunAutomaton automaton = visitForSingleAutomata(query);
        BytesRef term = new BytesRef("astuffb");
        assertThat(automaton.run(term.bytes, term.offset, term.length), is(true));
    }
}
