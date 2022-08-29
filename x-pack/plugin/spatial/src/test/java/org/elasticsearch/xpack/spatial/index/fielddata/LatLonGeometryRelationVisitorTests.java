/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.apache.lucene.document.ShapeField;
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.tests.geo.GeoTestUtil;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.spatial.util.GeoTestUtils;

import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class LatLonGeometryRelationVisitorTests extends ESTestCase {

    public void testPoint() throws Exception {
        doTestShapes(GeoTestUtil::nextPoint);
    }

    public void testLine() throws Exception {
        doTestShapes(GeoTestUtil::nextLine);
    }

    public void testPolygon() throws Exception {
        doTestShapes(GeoTestUtil::nextPolygon);
    }

    private void doTestShapes(Supplier<LatLonGeometry> supplier) throws Exception {
        Geometry geometry = GeometryTestUtils.randomGeometryWithoutCircle(0, false);
        GeoShapeValues.GeoShapeValue geoShapeValue = GeoTestUtils.geoShapeValue(geometry);
        GeometryDocValueReader reader = GeoTestUtils.geometryDocValueReader(geometry, CoordinateEncoder.GEO);
        for (int i = 0; i < 1000; i++) {
            LatLonGeometry latLonGeometry = supplier.get();
            GeoRelation relation = geoShapeValue.relate(latLonGeometry);
            Component2D component2D = LatLonGeometry.create(latLonGeometry);
            Component2DVisitor contains = Component2DVisitor.getVisitor(
                component2D,
                ShapeField.QueryRelation.CONTAINS,
                CoordinateEncoder.GEO
            );
            reader.visit(contains);
            Component2DVisitor intersects = Component2DVisitor.getVisitor(
                component2D,
                ShapeField.QueryRelation.INTERSECTS,
                CoordinateEncoder.GEO
            );
            reader.visit(intersects);
            Component2DVisitor disjoint = Component2DVisitor.getVisitor(
                component2D,
                ShapeField.QueryRelation.DISJOINT,
                CoordinateEncoder.GEO
            );
            reader.visit(disjoint);
            if (relation == GeoRelation.QUERY_INSIDE) {
                assertThat(contains.matches(), equalTo(true));
                assertThat(intersects.matches(), equalTo(true));
                assertThat(disjoint.matches(), equalTo(false));
            } else if (relation == GeoRelation.QUERY_CROSSES) {
                assertThat(contains.matches(), equalTo(false));
                assertThat(intersects.matches(), equalTo(true));
                assertThat(disjoint.matches(), equalTo(false));
            } else {
                assertThat(contains.matches(), equalTo(false));
                assertThat(intersects.matches(), equalTo(false));
                assertThat(disjoint.matches(), equalTo(true));
            }
        }
    }
}
