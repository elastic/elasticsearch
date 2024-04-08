/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.runtime;

import org.elasticsearch.script.Script;

import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class LongScriptFieldTermsQueryTests extends AbstractLongScriptFieldQueryTestCase<LongScriptFieldTermsQuery> {
    @Override
    protected LongScriptFieldTermsQuery createTestInstance() {
        Set<Long> terms = new HashSet<>();
        int count = between(1, 100);
        while (terms.size() < count) {
            terms.add(randomLong());
        }
        return new LongScriptFieldTermsQuery(randomScript(), leafFactory, randomAlphaOfLength(5), terms);
    }

    @Override
    protected LongScriptFieldTermsQuery copy(LongScriptFieldTermsQuery orig) {
        return new LongScriptFieldTermsQuery(orig.script(), leafFactory, orig.fieldName(), orig.terms());
    }

    @Override
    protected LongScriptFieldTermsQuery mutate(LongScriptFieldTermsQuery orig) {
        Script script = orig.script();
        String fieldName = orig.fieldName();
        Set<Long> terms = orig.terms();
        switch (randomInt(2)) {
            case 0 -> script = randomValueOtherThan(script, this::randomScript);
            case 1 -> fieldName += "modified";
            case 2 -> {
                terms = new HashSet<>(terms);
                while (false == terms.add(randomLong())) {
                    // Random long was already in the set
                }
            }
            default -> fail();
        }
        return new LongScriptFieldTermsQuery(script, leafFactory, fieldName, terms);
    }

    @Override
    public void testMatches() {
        LongScriptFieldTermsQuery query = new LongScriptFieldTermsQuery(randomScript(), leafFactory, "test", Set.of(1L, 2L, 3L));
        assertTrue(query.matches(new long[] { 1 }, 1));
        assertTrue(query.matches(new long[] { 2 }, 1));
        assertTrue(query.matches(new long[] { 3 }, 1));
        assertTrue(query.matches(new long[] { 1, 0 }, 2));
        assertTrue(query.matches(new long[] { 0, 1 }, 2));
        assertFalse(query.matches(new long[] { 0 }, 1));
        assertFalse(query.matches(new long[] { 0, 1 }, 1));
    }

    @Override
    protected void assertToString(LongScriptFieldTermsQuery query) {
        assertThat(query.toString(query.fieldName()), equalTo(query.terms().toString()));
    }
}
