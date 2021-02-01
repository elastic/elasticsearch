/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.query;

import static org.hamcrest.Matchers.equalTo;

public class GeoPointScriptFieldExistsQueryTests extends AbstractGeoPointScriptFieldQueryTestCase<GeoPointScriptFieldExistsQuery> {
    @Override
    protected GeoPointScriptFieldExistsQuery createTestInstance() {
        return new GeoPointScriptFieldExistsQuery(randomScript(), leafFactory, randomAlphaOfLength(5));
    }

    @Override
    protected GeoPointScriptFieldExistsQuery copy(GeoPointScriptFieldExistsQuery orig) {
        return new GeoPointScriptFieldExistsQuery(orig.script(), leafFactory, orig.fieldName());
    }

    @Override
    protected GeoPointScriptFieldExistsQuery mutate(GeoPointScriptFieldExistsQuery orig) {
        if (randomBoolean()) {
            new GeoPointScriptFieldExistsQuery(randomValueOtherThan(orig.script(), this::randomScript), leafFactory, orig.fieldName());
        }
        return new GeoPointScriptFieldExistsQuery(orig.script(), leafFactory, orig.fieldName() + "modified");
    }

    @Override
    public void testMatches() {
        assertTrue(createTestInstance().matches(new long[] { 1L }, randomIntBetween(1, Integer.MAX_VALUE)));
        assertFalse(createTestInstance().matches(new long[0], 0));
        assertFalse(createTestInstance().matches(new long[1], 0));
    }

    @Override
    protected void assertToString(GeoPointScriptFieldExistsQuery query) {
        assertThat(query.toString(query.fieldName()), equalTo("GeoPointScriptFieldExistsQuery"));
    }
}
