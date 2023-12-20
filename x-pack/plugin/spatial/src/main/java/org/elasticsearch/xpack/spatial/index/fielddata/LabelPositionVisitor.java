/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.elasticsearch.common.geo.SpatialPoint;

import java.util.function.BiFunction;

/**
 * Get the first node of the tree and provide a point in that geometry (point, line or triangle)
 * as a suggested label position likely to be somewhere in the middle of the entire geometry.
 *
 * TODO: We could instead choose the point closer to the centroid which improves unbalanced trees
 */
class LabelPositionVisitor extends TriangleTreeVisitor.TriangleTreeDecodedVisitor {

    private SpatialPoint labelPosition;
    private final BiFunction<Double, Double, SpatialPoint> pointMaker;

    LabelPositionVisitor(CoordinateEncoder encoder, BiFunction<Double, Double, SpatialPoint> pointMaker) {
        super(encoder);
        this.pointMaker = pointMaker;
    }

    @Override
    protected void visitDecodedPoint(double x, double y) {
        assert labelPosition == null;
        labelPosition = pointMaker.apply(x, y);
    }

    @Override
    protected void visitDecodedLine(double aX, double aY, double bX, double bY, byte metadata) {
        assert labelPosition == null;
        labelPosition = pointMaker.apply((aX + bX) / 2.0, (aY + bY) / 2.0);
    }

    @Override
    protected void visitDecodedTriangle(double aX, double aY, double bX, double bY, double cX, double cY, byte metadata) {
        assert labelPosition == null;
        labelPosition = pointMaker.apply((aX + bX + cX) / 3.0, (aY + bY + cY) / 3.0);
    }

    @Override
    protected boolean pushDecodedX(double minX) {
        return labelPosition == null;
    }

    @Override
    protected boolean pushDecodedY(double minX) {
        return labelPosition == null;
    }

    @Override
    public boolean push() {
        // Don't traverse deeper once we found a result
        return labelPosition == null;
    }

    @Override
    protected boolean pushDecoded(double maxX, double maxY) {
        return labelPosition == null;
    }

    @Override
    protected boolean pushDecoded(double minX, double minY, double maxX, double maxY) {
        return labelPosition == null;
    }

    public SpatialPoint labelPosition() {
        return labelPosition;
    }
}
