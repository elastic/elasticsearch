/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.elasticsearch.test.ESTestCase;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Asserts {@link IterativeDouglasPeuckerSimplifier} produces the same output as JTS
 * {@link org.locationtech.jts.simplify.DouglasPeuckerSimplifier}.
 */
public class IterativeDouglasPeuckerEquivalenceTests extends ESTestCase {

    private static final GeometryFactory GF = new GeometryFactory();

    private record Witness(String wkt, double tolerance, String jts, String iterative) {}

    private static Geometry jts(Geometry g, double t) {
        return org.locationtech.jts.simplify.DouglasPeuckerSimplifier.simplify(g, t);
    }

    private static Geometry iter(Geometry g, double t) {
        return IterativeDouglasPeuckerSimplifier.simplify(g, t);
    }

    private static boolean sameOutput(Geometry a, Geometry b) {
        if (a == null || b == null) {
            return a == b;
        }
        if (a.getNumPoints() != b.getNumPoints()) {
            return false;
        }
        // Normalize handles ordering/orientation differences; equalsExact then compares coordinates.
        Geometry na = a.copy();
        Geometry nb = b.copy();
        na.normalize();
        nb.normalize();
        return na.equalsExact(nb, 0.0);
    }

    private void check(List<Witness> witnesses, Geometry g, double t) {
        Geometry jtsOut;
        Geometry iterOut;
        try {
            jtsOut = jts(g, t);
        } catch (RuntimeException e) {
            jtsOut = null;
        }
        try {
            iterOut = iter(g, t);
        } catch (RuntimeException e) {
            iterOut = null;
        }
        if (sameOutput(jtsOut, iterOut) == false) {
            witnesses.add(
                new Witness(
                    g.toText(),
                    t,
                    jtsOut == null ? "<exception/null>" : jtsOut.toText(),
                    iterOut == null ? "<exception/null>" : iterOut.toText()
                )
            );
        }
    }

    private static double[] tolerances() {
        return new double[] { 0.0, 1e-9, 0.001, 0.05, 0.5, 1.0, 5.0, 1000.0 };
    }

    /** Regular convex n-gons — exercises the ring-endpoint simplification (start vertex within tolerance). */
    public void testConvexPolygons() {
        List<Witness> w = new ArrayList<>();
        for (int n = 3; n <= 64; n++) {
            Coordinate[] ring = new Coordinate[n + 1];
            for (int i = 0; i < n; i++) {
                double a = 2 * Math.PI * i / n;
                ring[i] = new Coordinate(Math.cos(a), Math.sin(a));
            }
            ring[n] = ring[0];
            Polygon p = GF.createPolygon(ring);
            for (double t : tolerances()) {
                check(w, p, t);
            }
        }
        report("convex n-gons", w);
    }

    /**
     * Backtracking linestrings: an interior vertex projects OUTSIDE the [i,j] chord, so segment
     * distance (JTS) differs from perpendicular distance. Exercises the split-loop distance function.
     */
    public void testBacktrackingLineStrings() {
        List<Witness> w = new ArrayList<>();
        checkLine(w, new double[][] { { 0, 0 }, { 100, 0 }, { 1, 1 } });
        checkLine(w, new double[][] { { 0, 0 }, { 100, 0.5 }, { 1, 1 } });
        checkLine(w, new double[][] { { 0, 0 }, { 10, 0 }, { 20, 0 }, { -5, 1 } });
        checkLine(w, new double[][] { { 0, 0 }, { 5, 3 }, { 10, 0 }, { -3, 0.2 } });
        checkLine(w, new double[][] { { 0, 0 }, { 50, 0 }, { 50, 0.4 }, { 0.5, 0.5 } });
        report("hand-crafted backtracking linestrings", w);
    }

    private void checkLine(List<Witness> w, double[][] pts) {
        Coordinate[] cs = new Coordinate[pts.length];
        for (int i = 0; i < pts.length; i++) {
            cs[i] = new Coordinate(pts[i][0], pts[i][1]);
        }
        Geometry g = GF.createLineString(cs);
        for (double t : tolerances()) {
            check(w, g, t);
        }
    }

    /** Random backtracking geometries: random walks with sharp reversals, thousands of iterations. */
    public void testRandomBacktrackingGeometries() {
        List<Witness> w = new ArrayList<>();
        Random rnd = new Random(42);
        int iterations = 5000;
        for (int it = 0; it < iterations; it++) {
            int n = 3 + rnd.nextInt(12);
            Coordinate[] cs = new Coordinate[n];
            double x = 0, y = 0;
            for (int i = 0; i < n; i++) {
                // Bias toward backtracking: allow large negative steps so projections fall off-chord.
                x += (rnd.nextDouble() - 0.5) * 20;
                y += (rnd.nextDouble() - 0.5) * 4;
                cs[i] = new Coordinate(x, y);
            }
            Geometry line = GF.createLineString(cs.clone());
            for (double t : tolerances()) {
                check(w, line, t);
            }
            // Also make a polygon ring out of these points when possible.
            Coordinate[] ring = new Coordinate[n + 1];
            System.arraycopy(cs, 0, ring, 0, n);
            ring[n] = new Coordinate(cs[0]);
            try {
                LinearRing lr = GF.createLinearRing(ring);
                Polygon poly = GF.createPolygon(lr);
                for (double t : tolerances()) {
                    check(w, poly, t);
                }
            } catch (IllegalArgumentException ignored) {
                // Not a valid ring (e.g. too few distinct points); skip.
            }
        }
        report("random backtracking geometries (" + iterations + " iters)", w);
    }

    /** Polygons with holes, multipolygons, points, empty geometries. */
    public void testMixedGeometryTypes() {
        List<Witness> w = new ArrayList<>();
        check(w, GF.createPoint(new Coordinate(1, 2)), 0.5);
        check(w, GF.createPolygon((LinearRing) null), 0.5);
        Coordinate[] shell = {
            new Coordinate(0, 0),
            new Coordinate(10, 0),
            new Coordinate(10, 10),
            new Coordinate(0, 10),
            new Coordinate(0, 0) };
        Coordinate[] hole = {
            new Coordinate(3, 3),
            new Coordinate(3, 6),
            new Coordinate(6, 6),
            new Coordinate(6, 3),
            new Coordinate(3, 3) };
        Polygon withHole = GF.createPolygon(GF.createLinearRing(shell), new LinearRing[] { GF.createLinearRing(hole) });
        for (double t : tolerances()) {
            check(w, withHole, t);
        }
        Coordinate[] shell2 = {
            new Coordinate(20, 20),
            new Coordinate(25, 20),
            new Coordinate(25, 25),
            new Coordinate(20, 25),
            new Coordinate(20, 20) };
        Polygon mp = GF.createPolygon(shell2);
        for (double t : tolerances()) {
            check(w, GF.createMultiPolygon(new Polygon[] { withHole, mp }), t);
        }
        report("mixed geometry types", w);
    }

    /**
     * The exact polygon from the documented csv-spec example {@code stSimplifyLiteralPolygon}
     * (spatial-jts.csv-spec, tolerance 0.7), whose golden was generated by JTS and drops the ring's
     * start/end vertex via ring-endpoint simplification. Guards that the iterative simplifier matches.
     */
    public void testCsvSpecGoldenPolygon() throws Exception {
        String wkt = "POLYGON ((7.998 53.827, 9.470 53.068, 15.754 53.801, 16.523 57.160, "
            + "11.162 57.868, 8.064 57.445, 6.219 55.317, 7.998 53.827))";
        Geometry g = new org.locationtech.jts.io.WKTReader(GF).read(wkt);
        List<Witness> w = new ArrayList<>();
        check(w, g, 0.7);
        report("csv-spec stSimplifyLiteralPolygon", w);
    }

    private void report(String label, List<Witness> witnesses) {
        if (witnesses.isEmpty()) {
            return;
        }
        StringBuilder sb = new StringBuilder();
        sb.append(label).append(": ").append(witnesses.size()).append(" divergences. First witnesses:\n");
        int show = Math.min(witnesses.size(), 8);
        for (int i = 0; i < show; i++) {
            Witness ww = witnesses.get(i);
            sb.append("  tol=").append(ww.tolerance).append(" input=").append(ww.wkt).append("\n");
            sb.append("      JTS      =").append(ww.jts).append("\n");
            sb.append("      ITERATIVE=").append(ww.iterative).append("\n");
        }
        fail(sb.toString());
    }
}
