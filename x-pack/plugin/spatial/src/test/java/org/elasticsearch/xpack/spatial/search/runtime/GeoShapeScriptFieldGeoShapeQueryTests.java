/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.runtime;

import org.apache.lucene.geo.Polygon;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.geometry.Point;

import static org.hamcrest.Matchers.equalTo;

public class GeoShapeScriptFieldGeoShapeQueryTests extends AbstractGeoShapeScriptFieldQueryTestCase<GeoShapeScriptFieldGeoShapeQuery> {

    private static final Polygon polygon1 = new Polygon(new double[] { -10, -10, 10, 10, -10 }, new double[] { -10, 10, 10, -10, -10 });
    private static final Polygon polygon2 = new Polygon(new double[] { -11, -10, 10, 10, -11 }, new double[] { -10, 10, 10, -10, -10 });

    @Override
    protected GeoShapeScriptFieldGeoShapeQuery createTestInstance() {
        return new GeoShapeScriptFieldGeoShapeQuery(
            randomScript(),
            leafFactory,
            randomAlphaOfLength(5),
            ShapeRelation.INTERSECTS,
            polygon1
        );
    }

    @Override
    protected GeoShapeScriptFieldGeoShapeQuery copy(GeoShapeScriptFieldGeoShapeQuery orig) {
        return new GeoShapeScriptFieldGeoShapeQuery(orig.script(), leafFactory, orig.fieldName(), ShapeRelation.INTERSECTS, polygon1);
    }

    @Override
    protected GeoShapeScriptFieldGeoShapeQuery mutate(GeoShapeScriptFieldGeoShapeQuery orig) {
        return switch (randomInt(2)) {
            case 0 -> new GeoShapeScriptFieldGeoShapeQuery(
                randomValueOtherThan(orig.script(), this::randomScript),
                leafFactory,
                orig.fieldName(),
                ShapeRelation.INTERSECTS,
                polygon2
            );
            case 1 -> new GeoShapeScriptFieldGeoShapeQuery(orig.script(), leafFactory, orig.fieldName(), ShapeRelation.DISJOINT, polygon1);
            default -> new GeoShapeScriptFieldGeoShapeQuery(
                orig.script(),
                leafFactory,
                orig.fieldName() + "modified",
                ShapeRelation.INTERSECTS,
                polygon1
            );
        };
    }

    @Override
    public void testMatches() {
        assertTrue(createTestInstance().matches(new Point(1, 2, randomIntBetween(1, Integer.MAX_VALUE))));
        assertFalse(createTestInstance().matches(new Point(20, 0, randomIntBetween(1, Integer.MAX_VALUE))));
    }

    @Override
    protected void assertToString(GeoShapeScriptFieldGeoShapeQuery query) {
        assertThat(query.toString(query.fieldName()), equalTo("GeoShapeScriptFieldGeoShapeQuery"));
    }
}
