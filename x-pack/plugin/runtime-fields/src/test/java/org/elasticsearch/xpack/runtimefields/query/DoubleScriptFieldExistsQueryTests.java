/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.query;

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
        assertTrue(createTestInstance().matches(new double[] { 1 }));
        assertFalse(createTestInstance().matches(new double[0]));
        double[] big = new double[between(1, 10000)];
        for (int i = 0; i < big.length; i++) {
            big[i] = 1.0;
        }
        assertTrue(createTestInstance().matches(big));
    }

    @Override
    protected void assertToString(DoubleScriptFieldExistsQuery query) {
        assertThat(query.toString(query.fieldName()), equalTo("DoubleScriptFieldExistsQuery"));
    }
}
