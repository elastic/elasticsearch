/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.runtime;

import static org.hamcrest.Matchers.equalTo;

public class BooleanScriptFieldExistsQueryTests extends AbstractBooleanScriptFieldQueryTestCase<BooleanScriptFieldExistsQuery> {
    @Override
    protected BooleanScriptFieldExistsQuery createTestInstance() {
        return new BooleanScriptFieldExistsQuery(randomScript(), leafFactory, randomAlphaOfLength(5));
    }

    @Override
    protected BooleanScriptFieldExistsQuery copy(BooleanScriptFieldExistsQuery orig) {
        return new BooleanScriptFieldExistsQuery(orig.script(), leafFactory, orig.fieldName());
    }

    @Override
    protected BooleanScriptFieldExistsQuery mutate(BooleanScriptFieldExistsQuery orig) {
        if (randomBoolean()) {
            new BooleanScriptFieldExistsQuery(randomValueOtherThan(orig.script(), this::randomScript), leafFactory, orig.fieldName());
        }
        return new BooleanScriptFieldExistsQuery(orig.script(), leafFactory, orig.fieldName() + "modified");
    }

    @Override
    public void testMatches() {
        assertTrue(createTestInstance().matches(between(1, Integer.MAX_VALUE), 0));
        assertTrue(createTestInstance().matches(0, between(1, Integer.MAX_VALUE)));
        assertTrue(createTestInstance().matches(between(1, Integer.MAX_VALUE), between(1, Integer.MAX_VALUE)));
        assertFalse(createTestInstance().matches(0, 0));
    }

    @Override
    protected void assertToString(BooleanScriptFieldExistsQuery query) {
        assertThat(query.toString(query.fieldName()), equalTo("BooleanScriptFieldExistsQuery"));
    }
}
