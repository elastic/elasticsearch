/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.locationtech.jts.geom.Coordinate;
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
 * Functionally equivalent to JTS {@link org.locationtech.jts.simplify.DouglasPeuckerSimplifier}
 * but uses an explicit {@link Deque}-based iteration instead of JVM call-stack recursion.
 * <p>
 * JTS's {@code DouglasPeuckerLineSimplifier.simplifySection(int, int)} recurses proportionally
 * to vertex count in the worst case, causing {@link StackOverflowError} for geometries with many
 * thousands of vertices. This class eliminates that risk by maintaining the same work-list on the
 * heap rather than the JVM call stack.
 */
public class IterativeDouglasPeuckerSimplifier {

    /**
     * Simplifies {@code geometry} using the iterative Douglas-Peucker algorithm with the given
     * distance tolerance. Matches the contract of
     * {@link org.locationtech.jts.simplify.DouglasPeuckerSimplifier#simplify(Geometry, double)}.
     */
    public static Geometry simplify(Geometry geometry, double distanceTolerance) {
        if (distanceTolerance < 0.0) {
            throw new IllegalArgumentException("Tolerance must be non-negative");
        }
        // Match JTS DouglasPeuckerSimplifier.getResultGeometry(): empty geometry → return a copy
        // without running the transformer (which would return null for empty polygons).
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
            return factory.getCoordinateSequenceFactory().create(simplified);
        }

        @Override
        protected Geometry transformLinearRing(LinearRing ring, Geometry parent) {
            // The base-class transformLinearRing calls this.transformCoordinates (our iterative
            // override), then returns a LinearRing if enough points remain or downgrades to a
            // LineString for degenerate results (fewer than LinearRing.MINIMUM_VALID_SIZE points).
            // We mirror JTS DPTransformer exactly: signal polygon degeneration via null when the
            // ring has collapsed, so transformPolygon can collapse the whole polygon.
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
            // When called as part of a MultiPolygon traversal, validity repair happens at the
            // MultiPolygon level; skip it here (matches JTS DPTransformer.transformPolygon).
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

        /**
         * Repairs an area geometry that may have become invalid after simplification.
         * Matches JTS {@code DPTransformer.createValidArea} with {@code isEnsureValidTopology=true}:
         * calls {@code buffer(0.0)} unless the geometry is already a valid 2-D area. This handles
         * the case where a polygon collapses completely to a {@code GEOMETRYCOLLECTION EMPTY}
         * (dimension -1), which {@code buffer(0.0)} normalizes to {@code POLYGON EMPTY}.
         */
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
     * Iterative Douglas-Peucker coordinate simplification.
     * <p>
     * This replaces the recursive {@code simplifySection(int i, int j)} in JTS's
     * {@code DouglasPeuckerLineSimplifier} with an explicit stack of {@code (i, j)} index pairs,
     * so heap memory is used instead of the JVM call stack.
     */
    static Coordinate[] simplifyCoordinates(Coordinate[] pts, double distanceTolerance) {
        int n = pts.length;
        boolean[] usePt = new boolean[n];
        Arrays.fill(usePt, true);

        // Reused across iterations to avoid per-iteration allocation; this method is single-threaded.
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
                // Use LineSegment.distancePerpendicular to match JTS's own DP distance calculation
                // exactly, ensuring bit-for-bit identical output to DouglasPeuckerSimplifier.
                double dist = seg.distancePerpendicular(pts[k]);
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

}
