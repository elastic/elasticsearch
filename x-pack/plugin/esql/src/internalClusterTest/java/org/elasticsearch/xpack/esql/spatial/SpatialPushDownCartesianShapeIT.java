/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.spatial;

import org.apache.lucene.geo.XYEncodingUtils;
import org.elasticsearch.geo.ShapeTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Point;

public class SpatialPushDownCartesianShapeIT extends SpatialPushDownShapeTestCase {

    @Override
    protected String fieldType() {
        return "shape";
    }

    @Override
    protected Geometry getIndexGeometry() {
        return ShapeTestUtils.randomGeometryWithoutCircle(false);
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
    protected Point quantizePoint(Point point) {
        double x = XYEncodingUtils.decode(XYEncodingUtils.encode((float) point.getX()));
        double y = XYEncodingUtils.decode(XYEncodingUtils.encode((float) point.getY()));
        return new Point(x, y);
    }
}
