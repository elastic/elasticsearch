/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.runtime;

import org.elasticsearch.geo.GeometryTestUtils;

import static org.hamcrest.Matchers.equalTo;

public class GeoShapeScriptFieldExistsQueryTests extends AbstractGeoShapeScriptFieldQueryTestCase<GeoShapeScriptFieldExistsQuery> {
    @Override
    protected GeoShapeScriptFieldExistsQuery createTestInstance() {
        return new GeoShapeScriptFieldExistsQuery(randomScript(), leafFactory, randomAlphaOfLength(5));
    }

    @Override
    protected GeoShapeScriptFieldExistsQuery copy(GeoShapeScriptFieldExistsQuery orig) {
        return new GeoShapeScriptFieldExistsQuery(orig.script(), leafFactory, orig.fieldName());
    }

    @Override
    protected GeoShapeScriptFieldExistsQuery mutate(GeoShapeScriptFieldExistsQuery orig) {
        if (randomBoolean()) {
            new GeoShapeScriptFieldExistsQuery(randomValueOtherThan(orig.script(), this::randomScript), leafFactory, orig.fieldName());
        }
        return new GeoShapeScriptFieldExistsQuery(orig.script(), leafFactory, orig.fieldName() + "modified");
    }

    @Override
    public void testMatches() {
        assertTrue(createTestInstance().matches(GeometryTestUtils.randomGeometry(random().nextBoolean())));
        assertFalse(createTestInstance().matches(null));
    }

    @Override
    protected void assertToString(GeoShapeScriptFieldExistsQuery query) {
        assertThat(query.toString(query.fieldName()), equalTo("GeoShapeScriptFieldExistsQuery"));
    }
}
