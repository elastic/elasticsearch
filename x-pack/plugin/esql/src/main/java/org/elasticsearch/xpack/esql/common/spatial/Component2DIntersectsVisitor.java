/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.common.spatial;

import org.apache.lucene.geo.Component2D;
import org.apache.lucene.index.PointValues;
import org.elasticsearch.lucene.spatial.CoordinateEncoder;

/**
 * A {@link TriangleTreeVisitor.TriangleTreeDecodedVisitor} that tests whether a triangle tree intersects a
 * {@link Component2D}, stopping as soon as one intersecting component is found.
 * <p>
 * This is the {@code INTERSECTS} case of {@code org.elasticsearch.lucene.spatial.Component2DVisitor}, copied
 * onto ES|QL's own visitor hierarchy; only intersection is needed by the geo-grid functions. See
 * {@link TriangleTreeVisitor} for why ES|QL deliberately keeps its own copies rather than sharing the server's.
 */
final class Component2DIntersectsVisitor extends TriangleTreeVisitor.TriangleTreeDecodedVisitor {

    private final Component2D component2D;
    private boolean intersects;

    Component2DIntersectsVisitor(Component2D component2D, CoordinateEncoder encoder) {
        super(encoder);
        this.component2D = component2D;
    }

    /** Whether an intersecting component was found. */
    boolean matches() {
        return intersects;
    }

    /** Resets the visitor to the initial, disjoint state so it can be reused for another tree. */
    void reset() {
        intersects = false;
    }

    @Override
    protected void visitDecodedPoint(double x, double y) {
        intersects = component2D.contains(x, y);
    }

    @Override
    protected void visitDecodedLine(double aX, double aY, double bX, double bY, byte metadata) {
        intersects = component2D.intersectsLine(aX, aY, bX, bY);
    }

    @Override
    protected void visitDecodedTriangle(double aX, double aY, double bX, double bY, double cX, double cY, byte metadata) {
        intersects = component2D.intersectsTriangle(aX, aY, bX, bY, cX, cY);
    }

    @Override
    public boolean push() {
        // as far as shapes don't intersect, keep traversing the tree
        return intersects == false;
    }

    @Override
    protected boolean pushDecodedX(double minX) {
        return component2D.getMaxX() >= minX;
    }

    @Override
    protected boolean pushDecodedY(double minY) {
        return component2D.getMaxY() >= minY;
    }

    @Override
    protected boolean pushDecoded(double maxX, double maxY) {
        return component2D.getMinX() <= maxX && component2D.getMinY() <= maxY;
    }

    @Override
    protected boolean pushDecoded(double minX, double minY, double maxX, double maxY) {
        PointValues.Relation relation = component2D.relate(minX, maxX, minY, maxY);
        if (relation == PointValues.Relation.CELL_OUTSIDE_QUERY) {
            // shapes are disjoint, stop traversing the tree.
            return false;
        }
        if (relation == PointValues.Relation.CELL_INSIDE_QUERY) {
            // shapes intersect, stop traversing the tree.
            intersects = true;
            return false;
        }
        // traverse the tree.
        return true;
    }
}
