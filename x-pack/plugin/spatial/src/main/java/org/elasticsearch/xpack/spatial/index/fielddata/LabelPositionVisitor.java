/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import java.util.function.BiFunction;

/**
 * Get the first node of the tree and provide a point in that geometry (point, line or triangle)
 * as a suggested label position likely to be somewhere in the middle of the entire geometry.
 *
 * TODO: We could instead choose the point closer to the centroid which improves unbalanced trees
 */
public class LabelPositionVisitor<T> implements TriangleTreeReader.Visitor {

    private T labelPosition;
    private final CoordinateEncoder encoder;
    private final BiFunction<Double, Double, T> pointMaker;

    public LabelPositionVisitor(CoordinateEncoder encoder, BiFunction<Double, Double, T> pointMaker) {
        this.encoder = encoder;
        this.pointMaker = pointMaker;
    }

    @Override
    public void visitPoint(int xi, int yi) {
        double x = encoder.decodeX(xi);
        double y = encoder.decodeY(yi);
        // System.out.println("Got point: (" + x + "," + y + ")");
        assert labelPosition == null;
        labelPosition = pointMaker.apply(x, y);
    }

    @Override
    public void visitLine(int aXi, int aYi, int bXi, int bYi, byte metadata) {
        double aX = encoder.decodeX(aXi);
        double aY = encoder.decodeY(aYi);
        double bX = encoder.decodeX(bXi);
        double bY = encoder.decodeY(bYi);
        // System.out.println("Got line: (" + aX + "," + aY + ")-(" + bX + "," + bY + ")");
        assert labelPosition == null;
        labelPosition = pointMaker.apply((aX + bX) / 2.0, (aY + bY) / 2.0);
    }

    @Override
    public void visitTriangle(int aXi, int aYi, int bXi, int bYi, int cXi, int cYi, byte metadata) {
        double aX = encoder.decodeX(aXi);
        double aY = encoder.decodeY(aYi);
        double bX = encoder.decodeX(bXi);
        double bY = encoder.decodeY(bYi);
        double cX = encoder.decodeX(cXi);
        double cY = encoder.decodeY(cYi);
        // System.out.println("Got triangle: (" + aX + "," + aY + ")-(" + bX + "," + bY + ")-(" + cX + "," + cY + ")");
        assert labelPosition == null;
        labelPosition = pointMaker.apply((aX + bX + cX) / 3.0, (aY + bY + cY) / 3.0);
    }

    @Override
    public boolean push() {
        // Don't traverse deeper once we found a result
        return labelPosition == null;
    }

    @Override
    public boolean pushX(int minX) {
        // Don't traverse deeper once we found a result
        return labelPosition == null;
    }

    @Override
    public boolean pushY(int minY) {
        // Don't traverse deeper once we found a result
        return labelPosition == null;
    }

    @Override
    public boolean push(int maxX, int maxY) {
        // Don't traverse deeper once we found a result
        return labelPosition == null;
    }

    @Override
    public boolean push(int minX, int minY, int maxX, int maxY) {
        // Always start the traversal
        return true;
    }

    public T labelPosition() {
        return labelPosition;
    }
}
