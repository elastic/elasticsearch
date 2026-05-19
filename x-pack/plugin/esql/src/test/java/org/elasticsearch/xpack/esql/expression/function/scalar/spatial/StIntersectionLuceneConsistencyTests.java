/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.apache.lucene.document.ShapeField;
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.lucene.spatial.Component2DVisitor;
import org.elasticsearch.lucene.spatial.CoordinateEncoder;
import org.elasticsearch.lucene.spatial.GeometryDocValueReader;
import org.elasticsearch.lucene.spatial.TriangleTreeVisitor;
import org.elasticsearch.test.ESTestCase;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.UNSPECIFIED;
import static org.elasticsearch.xpack.esql.expression.function.scalar.spatial.BinarySpatialFunction.SpatialCrsType;
import static org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialRelatesUtils.asGeometryDocValueReader;
import static org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialRelatesUtils.asLuceneComponent2D;

/**
 * Tests that verify consistency between Lucene's triangle-tree INTERSECTS predicate (used by
 * {@code ST_INTERSECTS}) and JTS intersection geometry computation (used by {@code ST_INTERSECTION}).
 *
 * <p>The key invariants are:
 * <ul>
 *   <li>If the triangle-tree predicate says the geometries intersect, the JTS intersection must be
 *       non-empty.</li>
 *   <li>If the triangle-tree predicate says the geometries are disjoint, the JTS intersection must
 *       be empty.</li>
 * </ul>
 *
 * <p>Lucene's predicate operates on geometry A after it has been quantized onto the integer grid
 * used by the triangle tree (via {@code GeoShapeIndexer}), while geometry B is converted to a
 * {@code Component2D} at its original precision. JTS operates on both geometries at their source
 * precision. Pre-quantizing both coordinates to the same Lucene grid before testing should bring the
 * two approaches into agreement for boundary cases.
 *
 * <p>An optional triangle-tree decomposition path computes the actual intersection geometry by
 * visiting all triangles in the encoded tree, intersecting each with the query geometry via JTS,
 * and unioning the results. The outcome should agree with direct JTS intersection.
 */
public class StIntersectionLuceneConsistencyTests extends ESTestCase {

    static final GeoShapeIndexer GEO_INDEXER = new GeoShapeIndexer(Orientation.CCW, "test-field");
    static final GeometryFactory JTS_FACTORY = new GeometryFactory();

    // -------------------------------------------------------------------------
    // Known geometry pair consistency tests
    // -------------------------------------------------------------------------

    public void testOverlappingPolygonsConsistency() throws IOException {
        assertConsistency("overlapping squares", rect(0, 3, 0, 3), rect(1, 4, 1, 4));
    }

    public void testDisjointPolygonsConsistency() throws IOException {
        assertConsistency("disjoint squares", rect(0, 1, 0, 1), rect(2, 3, 2, 3));
    }

    public void testContainedPolygonConsistency() throws IOException {
        assertConsistency("inner polygon contained by outer", rect(0, 10, 0, 10), rect(2, 5, 2, 5));
    }

    public void testPolygonsTouchingAtEdgeConsistency() throws IOException {
        // Polygons that share exactly one edge; both tools should agree whether this counts as an intersection.
        assertConsistency("polygons sharing an edge", rect(0, 1, 0, 1), rect(1, 2, 0, 1));
    }

    public void testPolygonsTouchingAtSinglePointConsistency() throws IOException {
        // Polygons that share exactly one corner point.
        assertConsistency("polygons sharing a corner", rect(0, 1, 0, 1), rect(1, 2, 1, 2));
    }

    public void testLineCrossesPolygonConsistency() throws IOException {
        // A line that clearly crosses through a polygon.
        assertConsistency("line crossing polygon", rect(1, 3, 1, 3), new Line(new double[] { 0, 4 }, new double[] { 2, 2 }));
    }

    public void testLineOutsidePolygonConsistency() throws IOException {
        assertConsistency("line outside polygon", rect(0, 1, 0, 1), new Line(new double[] { 2, 4 }, new double[] { 2, 2 }));
    }

    public void testPointInsidePolygonConsistency() throws IOException {
        assertConsistency("point inside polygon", rect(0, 4, 0, 4), new Point(2, 2));
    }

    public void testPointOutsidePolygonConsistency() throws IOException {
        assertConsistency("point outside polygon", rect(0, 1, 0, 1), new Point(5, 5));
    }

    public void testPointOnPolygonEdgeConsistency() throws IOException {
        assertConsistency("point on polygon edge", rect(0, 2, 0, 2), new Point(1, 0));
    }

    // -------------------------------------------------------------------------
    // Random rectangle-pair consistency
    // -------------------------------------------------------------------------

    /**
     * Verify consistency with random rectangles at full source precision.
     * This is the case for literal values or source-loaded fields: JTS uses full double precision
     * while the triangle-tree predicate quantizes geometry A to the integer grid.
     */
    public void testRandomRectanglePairsConsistency() throws IOException {
        for (int i = 0; i < 100; i++) {
            Polygon a = randomRect();
            Polygon b = randomRect();
            assertConsistency("random pair #" + i, a, b);
        }
    }

    /**
     * Verify consistency after both geometries are pre-quantized to Lucene's GEO integer grid,
     * simulating the path taken by coordinates that were stored in an Elasticsearch index.
     * Quantization should eliminate boundary disagreements that arise when the two approaches
     * operate at different precisions.
     */
    public void testQuantizedRandomRectanglePairsConsistency() throws IOException {
        for (int i = 0; i < 100; i++) {
            Polygon a = quantize(randomRect());
            Polygon b = quantize(randomRect());
            assertConsistency("quantized random pair #" + i, a, b);
        }
    }

    // -------------------------------------------------------------------------
    // Triangle-tree decomposition intersection (nice-to-have)
    // -------------------------------------------------------------------------

    /**
     * Verify that the intersection geometry computed by decomposing geometry A into its triangle-tree
     * components and intersecting each with B using JTS agrees with a direct JTS intersection.
     * <p>
     * The decomposition path encodes A as triangle-tree doc-values, visits every stored component
     * (triangles, line segments, and points), intersects each with B via JTS, and unions the
     * partial results. The direct path passes A and B straight to {@code Geometry.intersection()}.
     * Both should produce geometrically equivalent results (possibly with different representations).
     */
    public void testTriangleDecompositionMatchesDirectJts() throws IOException {
        List<Polygon[]> pairs = List.of(
            new Polygon[] { rect(0, 3, 0, 3), rect(1, 4, 1, 4) },
            new Polygon[] { rect(0, 10, 0, 10), rect(2, 5, 2, 5) }
        );
        for (Polygon[] pair : pairs) {
            Polygon a = pair[0];
            Polygon b = pair[1];
            org.locationtech.jts.geom.Geometry direct = jtsGeoIntersection(a, b);
            org.locationtech.jts.geom.Geometry decomposed = triangleDecompositionIntersection(a, b);
            // Both should agree on emptiness.
            assertEquals(
                "Direct JTS and triangle-decomposition intersection should agree on emptiness",
                direct.isEmpty(),
                decomposed.isEmpty()
            );
            if (direct.isEmpty() == false) {
                // Areas should be approximately equal (decomposition may differ slightly in topology
                // because triangulation approximates the polygon boundary).
                assertEquals(
                    "Areas of direct and decomposed intersections should be close",
                    direct.getArea(),
                    decomposed.getArea(),
                    direct.getArea() * 1e-6
                );
            }
        }
    }

    // -------------------------------------------------------------------------
    // Area calculation consistency tests
    // -------------------------------------------------------------------------

    public void testOverlappingPolygonsAreaConsistency() throws IOException {
        assertAreaConsistency("overlapping squares", rect(0, 3, 0, 3), rect(1, 4, 1, 4));
    }

    public void testDisjointPolygonsAreaConsistency() throws IOException {
        assertAreaConsistency("disjoint squares", rect(0, 1, 0, 1), rect(2, 3, 2, 3));
    }

    public void testContainedPolygonAreaConsistency() throws IOException {
        assertAreaConsistency("inner polygon contained by outer", rect(0, 10, 0, 10), rect(2, 5, 2, 5));
    }

    public void testPolygonsTouchingAtEdgeAreaConsistency() throws IOException {
        assertAreaConsistency("polygons sharing an edge", rect(0, 1, 0, 1), rect(1, 2, 0, 1));
    }

    public void testLineCrossesPolygonAreaConsistency() throws IOException {
        assertAreaConsistency("line crossing polygon", rect(1, 3, 1, 3), new Line(new double[] { 0, 4 }, new double[] { 2, 2 }));
    }

    /**
     * Verify area consistency with random quantized rectangle pairs. Quantizing both geometries to
     * Lucene's GEO integer grid ensures that direct JTS and triangle-decomposition operate on
     * identical coordinate values, eliminating precision differences at polygon boundaries.
     */
    public void testQuantizedRandomRectanglePairsAreaConsistency() throws IOException {
        for (int i = 0; i < 100; i++) {
            Polygon a = quantize(randomRect());
            Polygon b = quantize(randomRect());
            assertAreaConsistency("quantized random pair #" + i, a, b);
        }
    }

    // -------------------------------------------------------------------------
    // Core helpers
    // -------------------------------------------------------------------------

    /**
     * Assert that Lucene's triangle-tree INTERSECTS predicate and JTS intersection agree for
     * the given geometry pair, testing both orderings (A,B) and (B,A).
     */
    static void assertConsistency(String label, Geometry a, Geometry b) throws IOException {
        boolean luceneAB = luceneGeoIntersects(a, b);
        boolean luceneBA = luceneGeoIntersects(b, a);

        org.locationtech.jts.geom.Geometry jtsResult;
        try {
            jtsResult = jtsGeoIntersection(a, b);
        } catch (Exception e) {
            // JTS topology exceptions can be thrown for degenerate geometries; skip those cases.
            assumeNoException("JTS topology exception for " + label, e);
            return;
        }
        boolean jtsIntersects = jtsResult.isEmpty() == false;

        assertEquals(
            "Lucene triangle-tree predicate should be symmetric for " + label + ": intersects(A,B) != intersects(B,A)",
            luceneAB,
            luceneBA
        );
        assertEquals(
            "Lucene predicate and JTS intersection disagree for " + label + ": lucene=" + luceneAB + ", jts-empty=" + jtsResult.isEmpty(),
            luceneAB,
            jtsIntersects
        );
    }

    /**
     * Assert that the intersection area computed by summing per-triangle JTS intersections
     * (the triangle-decomposition path) agrees with the area of the direct JTS intersection.
     * Both orderings (A,B) and (B,A) are tested since intersection is symmetric.
     * <p>
     * The tolerance is {@code max(jtsArea * 1e-6, 1e-7)}. The absolute floor of {@code 1e-7}
     * accommodates quantization artifacts that arise when geometry A is encoded through
     * {@link GeoShapeIndexer}: boundary triangles can bleed by up to one Lucene GEO grid step
     * (~{@code 2^-24} ≈ 5.96e-8 degrees) past the original polygon edge, producing a tiny
     * non-zero area where JTS sees a zero-area line or point intersection. Using pre-quantized
     * geometries (via {@link #quantize}) in the random-pair tests eliminates this source of
     * disagreement.
     */
    static void assertAreaConsistency(String label, Geometry a, Geometry b) throws IOException {
        double jtsAreaAB;
        double jtsAreaBA;
        try {
            jtsAreaAB = jtsGeoIntersection(a, b).getArea();
            jtsAreaBA = jtsGeoIntersection(b, a).getArea();
        } catch (Exception e) {
            assumeNoException("JTS topology exception for " + label, e);
            return;
        }
        double triangleAreaAB = triangleDecompositionArea(a, b);
        double triangleAreaBA = triangleDecompositionArea(b, a);

        assertEquals(
            "Triangle decomposition area (A,B) should match JTS intersection area for " + label,
            jtsAreaAB,
            triangleAreaAB,
            Math.max(jtsAreaAB * 1e-6, 1e-7)
        );
        assertEquals(
            "Triangle decomposition area (B,A) should match JTS intersection area for " + label,
            jtsAreaBA,
            triangleAreaBA,
            Math.max(jtsAreaBA * 1e-6, 1e-7)
        );
    }

    /**
     * Evaluate Lucene's triangle-tree INTERSECTS predicate for GEO coordinates.
     * Geometry {@code a} is encoded as triangle-tree doc-values (quantizing its coordinates);
     * geometry {@code b} is converted to a {@link Component2D} at its original precision.
     */
    static boolean luceneGeoIntersects(Geometry a, Geometry b) throws IOException {
        GeometryDocValueReader reader = asGeometryDocValueReader(CoordinateEncoder.GEO, GEO_INDEXER, a);
        Component2D component2D = asLuceneComponent2D(SpatialCrsType.GEO, b);
        Component2DVisitor visitor = Component2DVisitor.getVisitor(component2D, ShapeField.QueryRelation.INTERSECTS, CoordinateEncoder.GEO);
        reader.visit(visitor);
        return visitor.matches();
    }

    /**
     * Compute the JTS intersection of two GEO geometries (both at full source precision).
     */
    static org.locationtech.jts.geom.Geometry jtsGeoIntersection(Geometry a, Geometry b) {
        try {
            org.locationtech.jts.geom.Geometry jtsA = UNSPECIFIED.wkbToJtsGeometry(GEO.asWkb(a));
            org.locationtech.jts.geom.Geometry jtsB = UNSPECIFIED.wkbToJtsGeometry(GEO.asWkb(b));
            return jtsA.intersection(jtsB);
        } catch (org.locationtech.jts.io.ParseException e) {
            throw new RuntimeException("Failed to convert geometry to JTS", e);
        }
    }

    /**
     * Compute an intersection geometry by decomposing {@code a} into its triangle-tree components
     * (triangles, line segments, and points), intersecting each component with {@code b} using JTS,
     * and unioning the partial results.
     * <p>
     * This exercises the same quantization path that Lucene uses for indexed data, providing a
     * triangle-tree-based counterpart to the direct JTS computation.
     */
    static org.locationtech.jts.geom.Geometry triangleDecompositionIntersection(Geometry a, Geometry b) throws IOException {
        GeometryDocValueReader reader = asGeometryDocValueReader(CoordinateEncoder.GEO, GEO_INDEXER, a);
        org.locationtech.jts.geom.Geometry jtsB;
        try {
            jtsB = UNSPECIFIED.wkbToJtsGeometry(GEO.asWkb(b));
        } catch (org.locationtech.jts.io.ParseException e) {
            throw new RuntimeException("Failed to convert geometry B to JTS", e);
        }

        GeometryCollectingVisitor collector = new GeometryCollectingVisitor(CoordinateEncoder.GEO);
        reader.visit(collector);

        org.locationtech.jts.geom.Geometry result = JTS_FACTORY.createEmpty(2);
        for (org.locationtech.jts.geom.Geometry component : collector.components()) {
            org.locationtech.jts.geom.Geometry partial = component.intersection(jtsB);
            if (partial.isEmpty() == false) {
                result = result.union(partial);
            }
        }
        return result;
    }

    /**
     * Compute the intersection area between {@code a} and {@code b} by decomposing {@code a} into
     * its triangle-tree components and summing the per-triangle intersection areas. This is valid
     * because the triangle decomposition produces non-overlapping triangles, so areas can be
     * accumulated without a union step.
     */
    static double triangleDecompositionArea(Geometry a, Geometry b) throws IOException {
        GeometryDocValueReader reader = asGeometryDocValueReader(CoordinateEncoder.GEO, GEO_INDEXER, a);
        org.locationtech.jts.geom.Geometry jtsB;
        try {
            jtsB = UNSPECIFIED.wkbToJtsGeometry(GEO.asWkb(b));
        } catch (org.locationtech.jts.io.ParseException e) {
            throw new RuntimeException("Failed to convert geometry B to JTS", e);
        }
        IntersectionAreaVisitor visitor = new IntersectionAreaVisitor(CoordinateEncoder.GEO, jtsB);
        reader.visit(visitor);
        return visitor.getArea();
    }

    // -------------------------------------------------------------------------
    // Geometry-collecting tree visitor
    // -------------------------------------------------------------------------

    /**
     * A {@link TriangleTreeVisitor.TriangleTreeDecodedVisitor} that collects every component
     * (triangles, line segments, and points) stored in a triangle tree as JTS geometries.
     * All push methods return {@code true} so the entire tree is visited.
     */
    static class GeometryCollectingVisitor extends TriangleTreeVisitor.TriangleTreeDecodedVisitor {

        private final List<org.locationtech.jts.geom.Geometry> collected = new ArrayList<>();

        GeometryCollectingVisitor(CoordinateEncoder encoder) {
            super(encoder);
        }

        List<org.locationtech.jts.geom.Geometry> components() {
            return collected;
        }

        @Override
        protected void visitDecodedTriangle(double aX, double aY, double bX, double bY, double cX, double cY, byte metadata) {
            Coordinate[] coords = new Coordinate[] {
                new Coordinate(aX, aY),
                new Coordinate(bX, bY),
                new Coordinate(cX, cY),
                new Coordinate(aX, aY)  // close the ring
            };
            collected.add(JTS_FACTORY.createPolygon(coords));
        }

        @Override
        protected void visitDecodedLine(double aX, double aY, double bX, double bY, byte metadata) {
            collected.add(JTS_FACTORY.createLineString(new Coordinate[] { new Coordinate(aX, aY), new Coordinate(bX, bY) }));
        }

        @Override
        protected void visitDecodedPoint(double x, double y) {
            collected.add(JTS_FACTORY.createPoint(new Coordinate(x, y)));
        }

        @Override
        public boolean push() {
            return true;
        }

        @Override
        protected boolean pushDecodedX(double minX) {
            return true;
        }

        @Override
        protected boolean pushDecodedY(double minY) {
            return true;
        }

        @Override
        protected boolean pushDecoded(double maxX, double maxY) {
            return true;
        }

        @Override
        protected boolean pushDecoded(double minX, double minY, double maxX, double maxY) {
            return true;
        }
    }

    // -------------------------------------------------------------------------
    // Intersection-area tree visitor
    // -------------------------------------------------------------------------

    /**
     * A {@link TriangleTreeVisitor.TriangleTreeDecodedVisitor} that accumulates the intersection
     * area between each decoded triangle and a fixed query geometry. Because the triangle
     * decomposition of a polygon produces non-overlapping triangles, the per-triangle areas can be
     * summed directly without a union step.
     */
    static class IntersectionAreaVisitor extends TriangleTreeVisitor.TriangleTreeDecodedVisitor {

        private final org.locationtech.jts.geom.Geometry queryGeom;
        private double area = 0.0;

        IntersectionAreaVisitor(CoordinateEncoder encoder, org.locationtech.jts.geom.Geometry queryGeom) {
            super(encoder);
            this.queryGeom = queryGeom;
        }

        double getArea() {
            return area;
        }

        @Override
        protected void visitDecodedTriangle(double aX, double aY, double bX, double bY, double cX, double cY, byte metadata) {
            Coordinate[] coords = new Coordinate[] {
                new Coordinate(aX, aY),
                new Coordinate(bX, bY),
                new Coordinate(cX, cY),
                new Coordinate(aX, aY) };
            org.locationtech.jts.geom.Geometry triangle = JTS_FACTORY.createPolygon(coords);
            area += triangle.intersection(queryGeom).getArea();
        }

        @Override
        protected void visitDecodedLine(double aX, double aY, double bX, double bY, byte metadata) {}

        @Override
        protected void visitDecodedPoint(double x, double y) {}

        @Override
        public boolean push() {
            return true;
        }

        @Override
        protected boolean pushDecodedX(double minX) {
            return true;
        }

        @Override
        protected boolean pushDecodedY(double minY) {
            return true;
        }

        @Override
        protected boolean pushDecoded(double maxX, double maxY) {
            return true;
        }

        @Override
        protected boolean pushDecoded(double minX, double minY, double maxX, double maxY) {
            return true;
        }
    }

    // -------------------------------------------------------------------------
    // Geometry construction helpers
    // -------------------------------------------------------------------------

    /** Create a simple axis-aligned rectangle polygon (lon/lat order). */
    static Polygon rect(double minLon, double maxLon, double minLat, double maxLat) {
        return new Polygon(
            new LinearRing(new double[] { minLon, maxLon, maxLon, minLon, minLon }, new double[] { minLat, minLat, maxLat, maxLat, minLat })
        );
    }

    /** Generate a random axis-aligned rectangle within valid GEO coordinate ranges. */
    private Polygon randomRect() {
        double minLon = randomDoubleBetween(-179.0, 178.0, true);
        double maxLon = randomDoubleBetween(minLon + 0.01, Math.min(minLon + 5.0, 180.0), true);
        double minLat = randomDoubleBetween(-89.0, 88.0, true);
        double maxLat = randomDoubleBetween(minLat + 0.01, Math.min(minLat + 5.0, 90.0), true);
        return rect(minLon, maxLon, minLat, maxLat);
    }

    /**
     * Quantize polygon coordinates to Lucene's GEO integer grid by encoding each coordinate to the
     * 32-bit integer representation and decoding it back. This simulates what happens to coordinates
     * when a geometry is stored in an Elasticsearch index.
     */
    static Polygon quantize(Polygon polygon) {
        LinearRing shell = polygon.getPolygon();
        double[] lons = shell.getLons().clone();
        double[] lats = shell.getLats().clone();
        for (int i = 0; i < lons.length; i++) {
            lons[i] = GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(lons[i]));
            lats[i] = GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(lats[i]));
        }
        return new Polygon(new LinearRing(lons, lats));
    }
}
