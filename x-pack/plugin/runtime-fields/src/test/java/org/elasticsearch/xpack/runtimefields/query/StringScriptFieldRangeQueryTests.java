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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class StringScriptFieldRangeQueryTests extends AbstractStringScriptFieldQueryTestCase<StringScriptFieldRangeQuery> {
    @Override
    protected StringScriptFieldRangeQuery createTestInstance() {
        String lower = randomAlphaOfLength(3);
        String upper = randomValueOtherThan(lower, () -> randomAlphaOfLength(3));
        if (lower.compareTo(upper) > 0) {
            String tmp = lower;
            lower = upper;
            upper = tmp;
        }
        return new StringScriptFieldRangeQuery(
            randomScript(),
            leafFactory,
            randomAlphaOfLength(5),
            lower,
            upper,
            randomBoolean(),
            randomBoolean()
        );
    }

    @Override
    protected StringScriptFieldRangeQuery copy(StringScriptFieldRangeQuery orig) {
        return new StringScriptFieldRangeQuery(
            orig.script(),
            leafFactory,
            orig.fieldName(),
            orig.lowerValue(),
            orig.upperValue(),
            orig.includeLower(),
            orig.includeUpper()
        );
    }

    @Override
    protected StringScriptFieldRangeQuery mutate(StringScriptFieldRangeQuery orig) {
        Script script = orig.script();
        String fieldName = orig.fieldName();
        String lower = orig.lowerValue();
        String upper = orig.upperValue();
        boolean includeLower = orig.includeLower();
        boolean includeUpper = orig.includeUpper();
        switch (randomInt(5)) {
            case 0:
                script = randomValueOtherThan(script, this::randomScript);
                break;
            case 1:
                fieldName += "modified";
                break;
            case 2:
                lower += "modified";
                break;
            case 3:
                upper += "modified";
                break;
            case 4:
                includeLower = !includeLower;
                break;
            case 5:
                includeUpper = !includeUpper;
                break;
            default:
                fail();
        }
        return new StringScriptFieldRangeQuery(script, leafFactory, fieldName, lower, upper, includeLower, includeUpper);
    }

    @Override
    public void testMatches() {
        StringScriptFieldRangeQuery query = new StringScriptFieldRangeQuery(randomScript(), leafFactory, "test", "a", "b", true, true);
        assertTrue(query.matches(List.of("a")));
        assertTrue(query.matches(List.of("ab")));
        assertTrue(query.matches(List.of("b")));
        assertFalse(query.matches(List.of("ba")));
        assertTrue(query.matches(List.of("a", "c")));
        query = new StringScriptFieldRangeQuery(randomScript(), leafFactory, "test", "a", "b", false, false);
        assertFalse(query.matches(List.of("a")));
        assertTrue(query.matches(List.of("ab")));
        assertFalse(query.matches(List.of("b")));
        assertFalse(query.matches(List.of("ba")));
    }

    @Override
    protected void assertToString(StringScriptFieldRangeQuery query) {
        assertThat(query.toString(query.fieldName()), containsString(query.includeLower() ? "[" : "{"));
        assertThat(query.toString(query.fieldName()), containsString(query.includeUpper() ? "]" : "}"));
        assertThat(query.toString(query.fieldName()), containsString(query.lowerValue() + " TO " + query.upperValue()));
    }

    @Override
    public void testVisit() {
        StringScriptFieldRangeQuery query = createTestInstance();
        ByteRunAutomaton automaton = visitForSingleAutomata(query);
        BytesRef term = new BytesRef(query.lowerValue() + "a");
        assertThat(automaton.run(term.bytes, term.offset, term.length), is(true));
        term = new BytesRef(query.lowerValue());
        assertThat(automaton.run(term.bytes, term.offset, term.length), is(query.includeLower()));
        term = new BytesRef(query.upperValue() + "a");
        assertThat(automaton.run(term.bytes, term.offset, term.length), is(false));
        term = new BytesRef(query.upperValue());
        assertThat(automaton.run(term.bytes, term.offset, term.length), is(query.includeUpper()));
    }
}
