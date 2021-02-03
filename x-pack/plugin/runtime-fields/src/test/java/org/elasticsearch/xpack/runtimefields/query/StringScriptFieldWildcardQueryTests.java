/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.runtimefields.query;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.elasticsearch.script.Script;

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
                caseInsensitive = !caseInsensitive;
                break;
            default:
                fail();
        }
        return new StringScriptFieldWildcardQuery(script, leafFactory, fieldName, pattern, caseInsensitive);
    }

    @Override
    public void testMatches() {
        StringScriptFieldWildcardQuery query = new StringScriptFieldWildcardQuery(randomScript(), leafFactory, "test", "a*b", false);
        assertTrue(query.matches(org.elasticsearch.common.collect.List.of("astuffb")));
        assertFalse(query.matches(org.elasticsearch.common.collect.List.of("Astuffb")));
        assertFalse(query.matches(org.elasticsearch.common.collect.List.of("fffff")));
        assertFalse(query.matches(org.elasticsearch.common.collect.List.of("a")));
        assertFalse(query.matches(org.elasticsearch.common.collect.List.of("b")));
        assertFalse(query.matches(org.elasticsearch.common.collect.List.of("aasdf")));
        assertFalse(query.matches(org.elasticsearch.common.collect.List.of("dsfb")));
        assertTrue(query.matches(org.elasticsearch.common.collect.List.of("astuffb", "fffff")));

        StringScriptFieldWildcardQuery ciQuery = new StringScriptFieldWildcardQuery(randomScript(), leafFactory, "test", "a*b", true);
        assertTrue(ciQuery.matches(org.elasticsearch.common.collect.List.of("Astuffb")));
        assertTrue(ciQuery.matches(org.elasticsearch.common.collect.List.of("astuffB", "fffff")));

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
