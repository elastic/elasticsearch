/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.elasticsearch.common.geo.GeoPoint;

/**
 * Get the first node of the tree and provide a point in that gemetry (point, line or triangle)
 * as a suggested label position likely to be somewhere in the middle of the entire geometry.
 *
 * TODO: We could instead choose the point closer to the centroid which improves unbalanced trees
 */
public class LabelPositionVisitor implements TriangleTreeReader.Visitor {

    private GeoPoint labelPosition;
    private final CoordinateEncoder encoder;

    public LabelPositionVisitor(CoordinateEncoder encoder) {
        this.encoder = encoder;
    }

    @Override
    public void visitPoint(int x, int y) {
        double lon = encoder.decodeX(x);
        double lat = encoder.decodeY(y);
        // System.out.println("Got point: (" + lon + "," + lat + ")");
        assert labelPosition == null;
        labelPosition = new GeoPoint(lat, lon);
    }

    @Override
    public void visitLine(int aX, int aY, int bX, int bY, byte metadata) {
        double aLon = encoder.decodeX(aX);
        double aLat = encoder.decodeY(aY);
        double bLon = encoder.decodeX(bX);
        double bLat = encoder.decodeY(bY);
        // System.out.println("Got line: (" + aLon + "," + aLat + ")-(" + bLon + "," + bLat + ")");
        assert labelPosition == null;
        labelPosition = new GeoPoint((aLat + bLat) / 2.0, (aLon + bLon) / 2.0);
    }

    @Override
    public void visitTriangle(int aX, int aY, int bX, int bY, int cX, int cY, byte metadata) {
        double aLon = encoder.decodeX(aX);
        double aLat = encoder.decodeY(aY);
        double bLon = encoder.decodeX(bX);
        double bLat = encoder.decodeY(bY);
        double cLon = encoder.decodeX(cX);
        double cLat = encoder.decodeY(cY);
        // System.out.println("Got triangle: (" + aLon + "," + aLat + ")-(" + bLon + "," + bLat + ")-(" + cLon + "," + cLat + ")");
        assert labelPosition == null;
        labelPosition = new GeoPoint((aLat + bLat + cLat) / 3.0, (aLon + bLon + cLon) / 3.0);
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

    public GeoPoint labelPosition() {
        return labelPosition;
    }
}
