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
            case 0 -> script = randomValueOtherThan(script, this::randomScript);
            case 1 -> fieldName += "modified";
            case 2 -> pattern += "modified";
            case 3 -> caseInsensitive = caseInsensitive == false;
            default -> fail();
        }
        return new StringScriptFieldWildcardQuery(script, leafFactory, fieldName, pattern, caseInsensitive);
    }

    @Override
    public void testMatches() {
        StringScriptFieldWildcardQuery query = new StringScriptFieldWildcardQuery(randomScript(), leafFactory, "test", "a*b", false);
        BytesRefBuilder scratch = new BytesRefBuilder();
        assertTrue(query.matches(List.of("astuffb"), scratch));
        assertFalse(query.matches(List.of("Astuffb"), scratch));
        assertFalse(query.matches(List.of("fffff"), scratch));
        assertFalse(query.matches(List.of("a"), scratch));
        assertFalse(query.matches(List.of("b"), scratch));
        assertFalse(query.matches(List.of("aasdf"), scratch));
        assertFalse(query.matches(List.of("dsfb"), scratch));
        assertTrue(query.matches(List.of("astuffb", "fffff"), scratch));

        StringScriptFieldWildcardQuery ciQuery = new StringScriptFieldWildcardQuery(randomScript(), leafFactory, "test", "a*b", true);
        assertTrue(ciQuery.matches(List.of("Astuffb"), scratch));
        assertTrue(ciQuery.matches(List.of("astuffB", "fffff"), scratch));

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
