/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.spatial;

import org.apache.lucene.document.XYShape;
import org.apache.lucene.geo.XYEncodingUtils;
import org.elasticsearch.common.geo.LuceneGeometriesUtils;
import org.elasticsearch.geo.ShapeTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Polygon;

public class SpatialPushDownCartesianShapeIT extends SpatialPushDownShapeTestCase {

    @Override
    protected String fieldType() {
        return "shape";
    }

    @Override
    protected Geometry getIndexGeometry() {
        return randomValueOtherThanMany(
            SpatialPushDownCartesianShapeIT::isInvalid,
            () -> ShapeTestUtils.randomGeometryWithoutCircle(false)
        );
    }

    static boolean isInvalid(Geometry geometry) {
        return switch (geometry) {
            case Polygon p -> isInvalid(p);
            case MultiPolygon m -> isInvalid(m);
            default -> false;
        };
    }

    static boolean isInvalid(Polygon polygon) {
        try {
            XYShape.createIndexableFields("test", LuceneGeometriesUtils.toXYPolygon(polygon), true);
            return false;
        } catch (Exception e) {
            return true;
        }
    }

    static boolean isInvalid(MultiPolygon mulitPolygon) {
        boolean inValid = false;
        for (Polygon p : mulitPolygon) {
            inValid |= isInvalid(p);
        }
        return inValid;
    }

    @Override
    protected Geometry getQueryGeometry() {
        return ShapeTestUtils.randomGeometry(false);
    }

    @Override
    protected String castingFunction() {
        return "TO_CARTESIANSHAPE";
    }

    @Override
    protected double quantizeX(double x) {
        return XYEncodingUtils.decode(XYEncodingUtils.encode((float) x));
    }

    @Override
    protected double quantizeY(double y) {
        return XYEncodingUtils.decode(XYEncodingUtils.encode((float) y));
    }
}
