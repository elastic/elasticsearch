/*
 * Based on the h3 project by Uber (@uber)
 * https://github.com/uber/h3
 * Licensed to Elasticsearch B.V under the Apache 2.0 License.
 * Elasticsearch B.V licenses this file, including any modifications, to you under the Apache 2.0 License.
 * See the LICENSE file in the project root for more information.
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
