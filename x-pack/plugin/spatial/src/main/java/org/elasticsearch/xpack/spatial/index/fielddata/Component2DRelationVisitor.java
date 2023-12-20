/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.apache.lucene.geo.Component2D;
import org.apache.lucene.index.PointValues;

import static org.elasticsearch.xpack.spatial.index.fielddata.TriangleTreeVisitor.TriangleTreeDecodedVisitor;
import static org.elasticsearch.xpack.spatial.index.fielddata.TriangleTreeVisitor.abFromTriangle;
import static org.elasticsearch.xpack.spatial.index.fielddata.TriangleTreeVisitor.bcFromTriangle;
import static org.elasticsearch.xpack.spatial.index.fielddata.TriangleTreeVisitor.caFromTriangle;

/**
 * A reusable tree reader visitor for a previous serialized {@link org.elasticsearch.geometry.Geometry} using
 * {@link TriangleTreeWriter}.
 *
 * This class supports checking {@link Component2D} relations against a serialized triangle tree.
 * It does not support bounding boxes crossing the dateline.
 */
class Component2DRelationVisitor extends TriangleTreeDecodedVisitor {

    private GeoRelation relation;
    private Component2D component2D;

    Component2DRelationVisitor(CoordinateEncoder encoder) {
        super(encoder);
    }

    public void reset(Component2D component2D) {
        this.component2D = component2D;
        relation = null;
    }

    /**
     * return the computed relation.
     */
    public GeoRelation relation() {
        return relation == null ? GeoRelation.QUERY_DISJOINT : relation;
    }

    @Override
    protected void visitDecodedPoint(double x, double y) {
        if (component2D.contains(x, y)) {
            if (canBeInside() && component2D.withinPoint(x, y) == Component2D.WithinRelation.CANDIDATE) {
                relation = GeoRelation.QUERY_INSIDE;
            } else if (canBeContained()) {
                relation = GeoRelation.QUERY_CONTAINS;
            } else {
                relation = GeoRelation.QUERY_CROSSES;
            }
        } else {
            adjustRelationForNotIntersectingComponent();
        }
    }

    @Override
    protected void visitDecodedLine(double aX, double aY, double bX, double bY, byte metadata) {
        if (component2D.intersectsLine(aX, aY, bX, bY)) {
            final boolean ab = abFromTriangle(metadata);
            if (canBeInside() && component2D.withinLine(aX, aY, ab, bX, bY) == Component2D.WithinRelation.CANDIDATE) {
                relation = GeoRelation.QUERY_INSIDE;
            } else if (canBeContained() && component2D.containsLine(aX, aY, bX, bY)) {
                relation = GeoRelation.QUERY_CONTAINS;
            } else {
                relation = GeoRelation.QUERY_CROSSES;
            }
        } else {
            adjustRelationForNotIntersectingComponent();
        }
    }

    @Override
    protected void visitDecodedTriangle(double aX, double aY, double bX, double bY, double cX, double cY, byte metadata) {
        if (component2D.intersectsTriangle(aX, aY, bX, bY, cX, cY)) {
            final boolean ab = abFromTriangle(metadata);
            final boolean bc = bcFromTriangle(metadata);
            final boolean ca = caFromTriangle(metadata);
            if (canBeInside() && component2D.withinTriangle(aX, aY, ab, bX, bY, bc, cX, cY, ca) == Component2D.WithinRelation.CANDIDATE) {
                relation = GeoRelation.QUERY_INSIDE;
            } else if (canBeContained() && component2D.containsTriangle(aX, aY, bX, bY, cX, cY)) {
                relation = GeoRelation.QUERY_CONTAINS;
            } else {
                relation = GeoRelation.QUERY_CROSSES;
            }
        } else {
            adjustRelationForNotIntersectingComponent();
        }

    }

    @Override
    public boolean push() {
        return relation != GeoRelation.QUERY_CROSSES;
    }

    @Override
    public boolean pushDecodedX(double minX) {
        if (component2D.getMaxX() >= minX) {
            return true;
        }
        adjustRelationForNotIntersectingComponent();
        return false;
    }

    @Override
    public boolean pushDecodedY(double minY) {
        if (component2D.getMaxY() >= minY) {
            return true;
        }
        adjustRelationForNotIntersectingComponent();
        return false;
    }

    @Override
    public boolean pushDecoded(double maxX, double maxY) {
        if (component2D.getMinY() <= maxY && component2D.getMinX() <= maxX) {
            return true;
        }
        adjustRelationForNotIntersectingComponent();
        return false;
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
        return true;
    }

    private void adjustRelationForNotIntersectingComponent() {
        if (relation == null) {
            relation = GeoRelation.QUERY_DISJOINT;
        } else if (relation == GeoRelation.QUERY_CONTAINS) {
            relation = GeoRelation.QUERY_CROSSES;
        }
    }

    private boolean canBeContained() {
        return relation == null || relation == GeoRelation.QUERY_CONTAINS;
    }

    private boolean canBeInside() {
        return relation != GeoRelation.QUERY_CONTAINS;
    }

}
