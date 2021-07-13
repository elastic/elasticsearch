/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.runtime;

import static org.hamcrest.Matchers.equalTo;

public class LongScriptFieldExistsQueryTests extends AbstractLongScriptFieldQueryTestCase<LongScriptFieldExistsQuery> {
    @Override
    protected LongScriptFieldExistsQuery createTestInstance() {
        return new LongScriptFieldExistsQuery(randomScript(), leafFactory, randomAlphaOfLength(5));
    }

    @Override
    protected LongScriptFieldExistsQuery copy(LongScriptFieldExistsQuery orig) {
        return new LongScriptFieldExistsQuery(orig.script(), leafFactory, orig.fieldName());
    }

    @Override
    protected LongScriptFieldExistsQuery mutate(LongScriptFieldExistsQuery orig) {
        if (randomBoolean()) {
            new LongScriptFieldExistsQuery(randomValueOtherThan(orig.script(), this::randomScript), leafFactory, orig.fieldName());
        }
        return new LongScriptFieldExistsQuery(orig.script(), leafFactory, orig.fieldName() + "modified");
    }

    @Override
    public void testMatches() {
        assertTrue(createTestInstance().matches(new long[0], randomIntBetween(1, Integer.MAX_VALUE)));
        assertFalse(createTestInstance().matches(new long[0], 0));
        assertFalse(createTestInstance().matches(new long[] { 1, 2, 3 }, 0));
    }

    @Override
    protected void assertToString(LongScriptFieldExistsQuery query) {
        assertThat(query.toString(query.fieldName()), equalTo("LongScriptFieldExistsQuery"));
    }
}
