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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class StringScriptFieldRangeQueryTests extends AbstractStringScriptFieldQueryTestCase<StringScriptFieldRangeQuery> {
    @Override
    protected StringScriptFieldRangeQuery createTestInstance() {
        String lower = randomBoolean() ? null : randomAlphaOfLength(3);
        String upper = randomBoolean() ? null : randomAlphaOfLength(3);
        return new StringScriptFieldRangeQuery(
            randomScript(),
            leafFactory,
            randomAlphaOfLength(5),
            lower,
            upper,
            lower == null || randomBoolean(),
            upper == null || randomBoolean()
        );
    }

    public void testValidate() {
        expectThrows(
            IllegalArgumentException.class,
            () -> new StringScriptFieldRangeQuery(
                randomScript(),
                leafFactory,
                randomAlphaOfLength(5),
                null,
                randomAlphaOfLength(3),
                false,
                randomBoolean()
            )
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new StringScriptFieldRangeQuery(
                randomScript(),
                leafFactory,
                randomAlphaOfLength(5),
                randomAlphaOfLength(3),
                null,
                randomBoolean(),
                false
            )
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
            case 0 -> script = randomValueOtherThan(script, this::randomScript);
            case 1 -> fieldName += "modified";
            case 2 -> {
                lower = mutate(lower);
                if (lower == null) {
                    includeLower = true;
                }
            }
            case 3 -> {
                upper = mutate(upper);
                if (upper == null) {
                    includeUpper = true;
                }
            }
            case 4 -> {
                if (lower == null) {
                    lower = mutate(lower);
                }
                includeLower = includeLower == false;
            }
            case 5 -> {
                if (upper == null) {
                    upper = mutate(upper);
                }
                includeUpper = includeUpper == false;
            }
            default -> fail();
        }
        return new StringScriptFieldRangeQuery(script, leafFactory, fieldName, lower, upper, includeLower, includeUpper);
    }

    private static String mutate(String term) {
        if (term == null) {
            return randomAlphaOfLength(3);
        }
        if (randomBoolean()) {
            return null;
        }
        return term + "modified";
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
        query = new StringScriptFieldRangeQuery(randomScript(), leafFactory, "test", "b", "a", randomBoolean(), randomBoolean());
        assertFalse(query.matches(List.of("a")));
        assertFalse(query.matches(List.of("ab")));
        assertFalse(query.matches(List.of("b")));
        assertFalse(query.matches(List.of("ba")));
        query = new StringScriptFieldRangeQuery(randomScript(), leafFactory, "test", null, "b", true, false);
        assertTrue(query.matches(List.of("a")));
        assertTrue(query.matches(List.of("ab")));
        assertFalse(query.matches(List.of("b")));
        assertFalse(query.matches(List.of("ba")));
        query = new StringScriptFieldRangeQuery(randomScript(), leafFactory, "test", "a", null, false, true);
        assertFalse(query.matches(List.of("a")));
        assertTrue(query.matches(List.of("ab")));
        assertTrue(query.matches(List.of("b")));
        assertTrue(query.matches(List.of("ba")));
        assertTrue(query.matches(List.of("z")));
        query = new StringScriptFieldRangeQuery(randomScript(), leafFactory, "test", null, null, true, true);
        assertTrue(query.matches(List.of("a")));
        assertTrue(query.matches(List.of("ab")));
        assertTrue(query.matches(List.of("b")));
        assertTrue(query.matches(List.of("ba")));
        assertTrue(query.matches(List.of("z")));
    }

    @Override
    protected void assertToString(StringScriptFieldRangeQuery query) {
        assertThat(query.toString(query.fieldName()), containsString(query.includeLower() ? "[" : "{"));
        assertThat(query.toString(query.fieldName()), containsString(query.includeUpper() ? "]" : "}"));
        String lowerValue = query.lowerValue() == null ? "*" : query.lowerValue();
        String upperValue = query.upperValue() == null ? "*" : query.upperValue();
        assertThat(query.toString(query.fieldName()), containsString(lowerValue + " TO " + upperValue));
    }

    @Override
    public void testVisit() {
        StringScriptFieldRangeQuery query = createTestInstance();
        ByteRunAutomaton automaton = visitForSingleAutomata(query);
        boolean validRange = true;
        if (query.lowerValue() != null && query.upperValue() != null) {
            validRange = query.lowerValue().compareTo(query.upperValue()) < 0;
        }
        String termString;
        if (query.lowerValue() == null) {
            if (query.upperValue() == null) {
                termString = randomAlphaOfLength(1);
            } else {
                termString = randomValueOtherThanMany(value -> {
                    int cmp = value.compareTo(query.upperValue());
                    return query.includeUpper() ? cmp > 0 : cmp >= 0;
                }, () -> randomAlphaOfLength(1));
            }
        } else {
            termString = query.lowerValue() + "a";
        }
        BytesRef term = new BytesRef(termString);
        assertThat(automaton.run(term.bytes, term.offset, term.length), is(validRange));

        if (query.lowerValue() != null) {
            term = new BytesRef(query.lowerValue());
            assertThat(automaton.run(term.bytes, term.offset, term.length), is(query.includeLower() && validRange));
        }
        if (query.upperValue() != null) {
            term = new BytesRef(query.upperValue() + "a");
            assertThat(automaton.run(term.bytes, term.offset, term.length), is(false));
            term = new BytesRef(query.upperValue());
            assertThat(automaton.run(term.bytes, term.offset, term.length), is(query.includeUpper() && validRange));
        }
    }
}
