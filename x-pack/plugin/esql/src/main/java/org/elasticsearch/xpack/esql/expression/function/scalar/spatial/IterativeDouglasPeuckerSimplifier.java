/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateArrays;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineSegment;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.util.GeometryTransformer;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;

/**
 * Equivalent to JTS {@link org.locationtech.jts.simplify.DouglasPeuckerSimplifier}, but iterates with
 * an explicit {@link Deque} instead of recursion so a geometry with many thousands of vertices cannot
 * overflow the call stack.
 */
public class IterativeDouglasPeuckerSimplifier {

    /**
     * Simplifies {@code geometry} with the given distance tolerance, matching the contract of
     * {@link org.locationtech.jts.simplify.DouglasPeuckerSimplifier#simplify(Geometry, double)}.
     */
    public static Geometry simplify(Geometry geometry, double distanceTolerance) {
        if (distanceTolerance < 0.0) {
            throw new IllegalArgumentException("Tolerance must be non-negative");
        }
        // The transformer returns null for empty polygons, so copy empty input directly, as JTS does.
        if (geometry.isEmpty()) {
            return geometry.copy();
        }
        return new DPTransformer(distanceTolerance).transform(geometry);
    }

    private static class DPTransformer extends GeometryTransformer {
        private final double distanceTolerance;

        DPTransformer(double distanceTolerance) {
            this.distanceTolerance = distanceTolerance;
        }

        @Override
        protected CoordinateSequence transformCoordinates(CoordinateSequence coords, Geometry parent) {
            Coordinate[] inputPts = coords.toCoordinateArray();
            if (inputPts.length == 0) {
                return factory.getCoordinateSequenceFactory().create(new Coordinate[0]);
            }
            Coordinate[] simplified = simplifyCoordinates(inputPts, distanceTolerance);
            // A ring's shared endpoint is not exempt from simplification, unlike a line's endpoints.
            if (parent instanceof LinearRing && CoordinateArrays.isRing(inputPts)) {
                simplified = simplifyRingEndpoint(simplified, distanceTolerance);
            }
            return factory.getCoordinateSequenceFactory().create(simplified);
        }

        @Override
        protected Geometry transformLinearRing(LinearRing ring, Geometry parent) {
            // A ring that collapsed below a valid ring becomes null so its polygon collapses too.
            Geometry result = super.transformLinearRing(ring, parent);
            if (parent instanceof Polygon && result instanceof LinearRing == false) {
                return null;
            }
            return result;
        }

        @Override
        protected Geometry transformPolygon(Polygon polygon, Geometry parent) {
            if (polygon.isEmpty()) {
                return null;
            }
            Geometry rawGeom = super.transformPolygon(polygon, parent);
            // Inside a MultiPolygon, validity repair happens at the multipolygon level, so skip it here.
            if (parent instanceof MultiPolygon) {
                return rawGeom;
            }
            return createValidArea(rawGeom);
        }

        @Override
        protected Geometry transformMultiPolygon(MultiPolygon multiPolygon, Geometry parent) {
            Geometry roughGeom = super.transformMultiPolygon(multiPolygon, parent);
            return createValidArea(roughGeom);
        }

        /** Repairs area topology broken by simplification via {@code buffer(0.0)}, unless it is already a valid 2-D area. */
        private static Geometry createValidArea(Geometry rawAreaGeom) {
            if (rawAreaGeom == null) {
                return null;
            }
            boolean isAlreadyValidArea = rawAreaGeom.getDimension() == 2 && rawAreaGeom.isValid();
            if (isAlreadyValidArea == false) {
                return rawAreaGeom.buffer(0.0);
            }
            return rawAreaGeom;
        }
    }

    /**
     * Iterative Douglas-Peucker: an explicit stack of {@code (i, j)} index pairs replaces JTS's
     * recursive {@code simplifySection}, keeping the work-list on the heap rather than the call stack.
     */
    static Coordinate[] simplifyCoordinates(Coordinate[] pts, double distanceTolerance) {
        int n = pts.length;
        boolean[] usePt = new boolean[n];
        Arrays.fill(usePt, true);

        // Reused across iterations; this method is single-threaded.
        LineSegment seg = new LineSegment();
        Deque<int[]> stack = new ArrayDeque<>();
        stack.push(new int[] { 0, n - 1 });

        while (stack.isEmpty() == false) {
            int[] range = stack.pop();
            int i = range[0];
            int j = range[1];
            if (i + 1 == j) {
                continue;
            }

            seg.p0 = pts[i];
            seg.p1 = pts[j];
            double maxDist = -1.0;
            int maxIdx = i;
            for (int k = i + 1; k < j; k++) {
                // Distance to the segment (clamped to its endpoints), not to the infinite line
                // (distancePerpendicular); JTS uses segment distance, so the kept points match.
                double dist = seg.distance(pts[k]);
                if (dist > maxDist) {
                    maxDist = dist;
                    maxIdx = k;
                }
            }

            if (maxDist <= distanceTolerance) {
                for (int k = i + 1; k < j; k++) {
                    usePt[k] = false;
                }
            } else {
                stack.push(new int[] { i, maxIdx });
                stack.push(new int[] { maxIdx, j });
            }
        }

        int count = 0;
        for (boolean b : usePt) {
            if (b) count++;
        }
        Coordinate[] result = new Coordinate[count];
        int idx = 0;
        for (int i = 0; i < n; i++) {
            if (usePt[i]) result[idx++] = pts[i];
        }
        return result;
    }

    /**
     * Drops a ring's shared endpoint when it lies within tolerance of the segment between its
     * neighbours, re-closing the ring around the new first vertex. Triangles are left intact.
     */
    private static Coordinate[] simplifyRingEndpoint(Coordinate[] pts, double distanceTolerance) {
        if (pts.length < 4) {
            return pts;
        }
        LineSegment seg = new LineSegment();
        seg.p0 = pts[1];
        seg.p1 = pts[pts.length - 2];
        if (seg.distance(pts[0]) > distanceTolerance) {
            return pts;
        }
        // Keep pts[1..n-2] and re-close the ring around the new first vertex.
        Coordinate[] open = Arrays.copyOfRange(pts, 1, pts.length - 1);
        if (open.length > 0 && open[0].equals2D(open[open.length - 1]) == false) {
            Coordinate[] closed = Arrays.copyOf(open, open.length + 1);
            closed[open.length] = open[0];
            return closed;
        }
        return open;
    }

}
