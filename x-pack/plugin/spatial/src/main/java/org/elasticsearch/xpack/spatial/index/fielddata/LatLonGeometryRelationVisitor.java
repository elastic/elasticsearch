/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.index.PointValues;

/**
 * A reusable tree reader visitor for a previous serialized {@link org.elasticsearch.geometry.Geometry} using
 * {@link TriangleTreeWriter}.
 *
 * This class supports checking {@link LatLonGeometry} relations against a serialized triangle tree.
 * It does not support bounding boxes crossing the dateline.
 *
 */
class LatLonGeometryRelationVisitor extends TriangleTreeReader.DecodedVisitor {

    private GeoRelation relation;
    private Component2D component2D;

    LatLonGeometryRelationVisitor(CoordinateEncoder encoder) {
        super(encoder);
    }

    public void reset(LatLonGeometry latLonGeometry) {
        component2D = LatLonGeometry.create(latLonGeometry);
        relation = GeoRelation.QUERY_DISJOINT;
    }

    /**
     * return the computed relation.
     */
    public GeoRelation relation() {
        return relation;
    }

    @Override
    void visitDecodedPoint(double x, double y) {
        if (component2D.contains(x, y)) {
            if (component2D.withinPoint(x, y) == Component2D.WithinRelation.CANDIDATE) {
                relation = GeoRelation.QUERY_INSIDE;
            } else {
                relation = GeoRelation.QUERY_CROSSES;
            }
        }
    }

    @Override
    void visitDecodedLine(double aX, double aY, double bX, double bY, byte metadata) {
        if (component2D.intersectsLine(aX, aY, bX, bY)) {
            final boolean ab = (metadata & 1 << 4) == 1 << 4;
            if (component2D.withinLine(aX, aY, ab, bX, bY) == Component2D.WithinRelation.CANDIDATE) {
                relation = GeoRelation.QUERY_INSIDE;
            } else {
                relation = GeoRelation.QUERY_CROSSES;
            }
        }
    }

    @Override
    void visitDecodedTriangle(double aX, double aY, double bX, double bY, double cX, double cY, byte metadata) {
        if (component2D.intersectsTriangle(aX, aY, bX, bY, cX, cY)) {
            boolean ab = (metadata & 1 << 4) == 1 << 4;
            boolean bc = (metadata & 1 << 5) == 1 << 5;
            boolean ca = (metadata & 1 << 6) == 1 << 6;
            if (component2D.withinTriangle(aX, aY, ab, bX, bY, bc, cX, cY, ca) == Component2D.WithinRelation.CANDIDATE) {
                relation = GeoRelation.QUERY_INSIDE;
            } else {
                relation = GeoRelation.QUERY_CROSSES;
            }
        }
    }

    @Override
    public boolean push() {
        return relation != GeoRelation.QUERY_CROSSES;
    }

    @Override
    public boolean pushDecodedX(double minX) {
        return component2D.getMaxX() >= minX;
    }

    @Override
    public boolean pushDecodedY(double minY) {
        return component2D.getMaxY() >= minY;
    }

    @Override
    public boolean pushDecoded(double maxX, double maxY) {
        return component2D.getMinY() <= maxY && component2D.getMinX() <= maxX;
    }

    @Override
    @SuppressWarnings("HiddenField")
    public boolean pushDecoded(double minX, double minY, double maxX, double maxY) {
        PointValues.Relation rel = component2D.relate(minX, maxX, minY, maxY);
        if (rel == PointValues.Relation.CELL_OUTSIDE_QUERY) {
            // shapes are disjoint
            relation = GeoRelation.QUERY_DISJOINT;
            return false;
        }
        if (rel == PointValues.Relation.CELL_INSIDE_QUERY) {
            // the rectangle fully contains the shape
            relation = GeoRelation.QUERY_CROSSES;
            return false;
        }
        return true;
    }
}
