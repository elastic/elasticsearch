/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.runtime;

import org.apache.lucene.geo.Polygon;
import org.elasticsearch.common.geo.ShapeRelation;

import static org.hamcrest.Matchers.equalTo;

public class GeoPointScriptFieldGeoShapeQueryTests extends AbstractGeoPointScriptFieldQueryTestCase<GeoPointScriptFieldGeoShapeQuery> {

    private static final Polygon polygon1 = new Polygon(new double[] { -10, -10, 10, 10, -10 }, new double[] { -10, 10, 10, -10, -10 });
    private static final Polygon polygon2 = new Polygon(new double[] { -11, -10, 10, 10, -11 }, new double[] { -10, 10, 10, -10, -10 });

    @Override
    protected GeoPointScriptFieldGeoShapeQuery createTestInstance() {
        return new GeoPointScriptFieldGeoShapeQuery(
            randomScript(),
            leafFactory,
            randomAlphaOfLength(5),
            ShapeRelation.INTERSECTS,
            polygon1
        );
    }

    @Override
    protected GeoPointScriptFieldGeoShapeQuery copy(GeoPointScriptFieldGeoShapeQuery orig) {
        return new GeoPointScriptFieldGeoShapeQuery(orig.script(), leafFactory, orig.fieldName(), ShapeRelation.INTERSECTS, polygon1);
    }

    @Override
    protected GeoPointScriptFieldGeoShapeQuery mutate(GeoPointScriptFieldGeoShapeQuery orig) {
        return switch (randomInt(2)) {
            case 0 -> new GeoPointScriptFieldGeoShapeQuery(
                randomValueOtherThan(orig.script(), this::randomScript),
                leafFactory,
                orig.fieldName(),
                ShapeRelation.INTERSECTS,
                polygon2
            );
            case 1 -> new GeoPointScriptFieldGeoShapeQuery(orig.script(), leafFactory, orig.fieldName(), ShapeRelation.DISJOINT, polygon1);
            default -> new GeoPointScriptFieldGeoShapeQuery(
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
        assertTrue(createTestInstance().matches(new long[] { 1L }, randomIntBetween(1, Integer.MAX_VALUE)));
        assertFalse(createTestInstance().matches(new long[0], 0));
        assertFalse(createTestInstance().matches(new long[1], 0));
    }

    @Override
    protected void assertToString(GeoPointScriptFieldGeoShapeQuery query) {
        assertThat(query.toString(query.fieldName()), equalTo("GeoPointScriptFieldGeoShapeQuery"));
    }
}
