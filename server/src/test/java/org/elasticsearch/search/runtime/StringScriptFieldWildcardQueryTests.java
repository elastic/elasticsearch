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

public class StringScriptFieldWildcardQueryTests extends AbstractStringScriptFieldQueryTestCase<StringScriptFieldWildcardQuery> {
    @Override
    protected StringScriptFieldWildcardQuery createTestInstance() {
        return new StringScriptFieldWildcardQuery(
            randomScript(),
            leafFactory,
            randomAlphaOfLength(5),
            randomAlphaOfLength(6),
            randomBoolean()
        );
    }

    @Override
    protected StringScriptFieldWildcardQuery copy(StringScriptFieldWildcardQuery orig) {
        return new StringScriptFieldWildcardQuery(orig.script(), leafFactory, orig.fieldName(), orig.pattern(), orig.caseInsensitive());
    }

    @Override
    protected StringScriptFieldWildcardQuery mutate(StringScriptFieldWildcardQuery orig) {
        Script script = orig.script();
        String fieldName = orig.fieldName();
        String pattern = orig.pattern();
        boolean caseInsensitive = orig.caseInsensitive();
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
                caseInsensitive = caseInsensitive == false;
                break;
            default:
                fail();
        }
        return new StringScriptFieldWildcardQuery(script, leafFactory, fieldName, pattern, caseInsensitive);
    }

    @Override
    public void testMatches() {
        StringScriptFieldWildcardQuery query = new StringScriptFieldWildcardQuery(randomScript(), leafFactory, "test", "a*b", false);
        assertTrue(query.matches(List.of("astuffb")));
        assertFalse(query.matches(List.of("Astuffb")));
        assertFalse(query.matches(List.of("fffff")));
        assertFalse(query.matches(List.of("a")));
        assertFalse(query.matches(List.of("b")));
        assertFalse(query.matches(List.of("aasdf")));
        assertFalse(query.matches(List.of("dsfb")));
        assertTrue(query.matches(List.of("astuffb", "fffff")));

        StringScriptFieldWildcardQuery ciQuery = new StringScriptFieldWildcardQuery(randomScript(), leafFactory, "test", "a*b", true);
        assertTrue(ciQuery.matches(List.of("Astuffb")));
        assertTrue(ciQuery.matches(List.of("astuffB", "fffff")));

    }

    @Override
    protected void assertToString(StringScriptFieldWildcardQuery query) {
        assertThat(query.toString(query.fieldName()), equalTo(query.pattern()));
    }

    @Override
    public void testVisit() {
        StringScriptFieldWildcardQuery query = new StringScriptFieldWildcardQuery(randomScript(), leafFactory, "test", "a*b", false);
        ByteRunAutomaton automaton = visitForSingleAutomata(query);
        BytesRef term = new BytesRef("astuffb");
        assertTrue(automaton.run(term.bytes, term.offset, term.length));
    }
}
