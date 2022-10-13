/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.runtime;

import static org.hamcrest.Matchers.equalTo;

public class DoubleScriptFieldExistsQueryTests extends AbstractDoubleScriptFieldQueryTestCase<DoubleScriptFieldExistsQuery> {
    @Override
    protected DoubleScriptFieldExistsQuery createTestInstance() {
        return new DoubleScriptFieldExistsQuery(randomScript(), leafFactory, randomAlphaOfLength(5));
    }

    @Override
    protected DoubleScriptFieldExistsQuery copy(DoubleScriptFieldExistsQuery orig) {
        return new DoubleScriptFieldExistsQuery(orig.script(), leafFactory, orig.fieldName());
    }

    @Override
    protected DoubleScriptFieldExistsQuery mutate(DoubleScriptFieldExistsQuery orig) {
        if (randomBoolean()) {
            new DoubleScriptFieldExistsQuery(randomValueOtherThan(orig.script(), this::randomScript), leafFactory, orig.fieldName());
        }
        return new DoubleScriptFieldExistsQuery(orig.script(), leafFactory, orig.fieldName() + "modified");
    }

    @Override
    public void testMatches() {
        assertTrue(createTestInstance().matches(new double[0], randomIntBetween(1, Integer.MAX_VALUE)));
        assertFalse(createTestInstance().matches(new double[0], 0));
        assertFalse(createTestInstance().matches(new double[] { 1, 2, 3 }, 0));
    }

    @Override
    protected void assertToString(DoubleScriptFieldExistsQuery query) {
        assertThat(query.toString(query.fieldName()), equalTo("DoubleScriptFieldExistsQuery"));
    }
}
