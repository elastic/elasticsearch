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
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.script.Script;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class StringScriptFieldRegexpQueryTests extends AbstractStringScriptFieldQueryTestCase<StringScriptFieldRegexpQuery> {
    @Override
    protected StringScriptFieldRegexpQuery createTestInstance() {
        int matchFlags = randomBoolean() ? 0 : RegExp.ASCII_CASE_INSENSITIVE;
        return new StringScriptFieldRegexpQuery(
            randomScript(),
            leafFactory,
            randomAlphaOfLength(5),
            randomAlphaOfLength(6),
            randomInt(RegExp.ALL),
            matchFlags,
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
            orig.syntaxFlags(),
            orig.matchFlags(),
            Operations.DEFAULT_MAX_DETERMINIZED_STATES
        );
    }

    @Override
    protected StringScriptFieldRegexpQuery mutate(StringScriptFieldRegexpQuery orig) {
        Script script = orig.script();
        String fieldName = orig.fieldName();
        String pattern = orig.pattern();
        int syntaxFlags = orig.syntaxFlags();
        int matchFlags = orig.matchFlags();
        switch (randomInt(4)) {
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
                syntaxFlags = randomValueOtherThan(syntaxFlags, () -> randomInt(RegExp.ALL));
                break;
            case 4:
                matchFlags = (matchFlags & RegExp.ASCII_CASE_INSENSITIVE) != 0 ? 0 : RegExp.ASCII_CASE_INSENSITIVE;
                break;
            default:
                fail();
        }
        return new StringScriptFieldRegexpQuery(
            script,
            leafFactory,
            fieldName,
            pattern,
            syntaxFlags,
            matchFlags,
            Operations.DEFAULT_MAX_DETERMINIZED_STATES
        );
    }

    @Override
    public void testMatches() {
        StringScriptFieldRegexpQuery query = new StringScriptFieldRegexpQuery(
            randomScript(),
            leafFactory,
            "test",
            "a.+b",
            0,
            0,
            Operations.DEFAULT_MAX_DETERMINIZED_STATES
        );
        assertTrue(query.matches(List.of("astuffb")));
        assertFalse(query.matches(List.of("astuffB")));
        assertFalse(query.matches(List.of("fffff")));
        assertFalse(query.matches(List.of("ab")));
        assertFalse(query.matches(List.of("aasdf")));
        assertFalse(query.matches(List.of("dsfb")));
        assertTrue(query.matches(List.of("astuffb", "fffff")));

        StringScriptFieldRegexpQuery ciQuery = new StringScriptFieldRegexpQuery(
            randomScript(),
            leafFactory,
            "test",
            "a.+b",
            0,
            RegExp.ASCII_CASE_INSENSITIVE,
            Operations.DEFAULT_MAX_DETERMINIZED_STATES
        );
        assertTrue(ciQuery.matches(List.of("astuffB")));
        assertTrue(ciQuery.matches(List.of("Astuffb", "fffff")));

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
            0,
            Operations.DEFAULT_MAX_DETERMINIZED_STATES
        );
        ByteRunAutomaton automaton = visitForSingleAutomata(query);
        BytesRef term = new BytesRef("astuffb");
        assertThat(automaton.run(term.bytes, term.offset, term.length), is(true));
    }
}
