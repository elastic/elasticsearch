/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.runtime;

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;
import org.elasticsearch.script.Script;

import static org.hamcrest.Matchers.equalTo;

public class LongScriptFieldTermsQueryTests extends AbstractLongScriptFieldQueryTestCase<LongScriptFieldTermsQuery> {
    @Override
    protected LongScriptFieldTermsQuery createTestInstance() {
        LongSet terms = new LongHashSet();
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
        LongSet terms = orig.terms();
        switch (randomInt(2)) {
            case 0:
                script = randomValueOtherThan(script, this::randomScript);
                break;
            case 1:
                fieldName += "modified";
                break;
            case 2:
                terms = new LongHashSet(terms);
                while (false == terms.add(randomLong())) {
                    // Random long was already in the set
                }
                break;
            default:
                fail();
        }
        return new LongScriptFieldTermsQuery(script, leafFactory, fieldName, terms);
    }

    @Override
    public void testMatches() {
        LongScriptFieldTermsQuery query = new LongScriptFieldTermsQuery(randomScript(), leafFactory, "test", LongHashSet.from(1, 2, 3));
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
