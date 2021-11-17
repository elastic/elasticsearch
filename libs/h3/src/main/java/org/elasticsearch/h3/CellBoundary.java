/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.h3;

/**
 * cell boundary points as {@link LatLng}
 */
public final class CellBoundary {

    /** Maximum number of cell boundary vertices; worst case is pentagon:
     *  5 original verts + 5 edge crossings
     */
    private static final int MAX_CELL_BNDRY_VERTS = 10;
    /** How many points it holds */
    private int numVertext;
    /** The actual points */
    private final LatLng[] points = new LatLng[MAX_CELL_BNDRY_VERTS];

    CellBoundary() {}

    void add(LatLng point) {
        points[numVertext++] = point;
    }

    /** Number of points in this boundary */
    public int numPoints() {
        return numVertext;
    }

    /** Return the point at the given position*/
    public LatLng getLatLon(int i) {
        if (i >= numVertext) {
            throw new IndexOutOfBoundsException();
        }
        return points[i];
    }
}
