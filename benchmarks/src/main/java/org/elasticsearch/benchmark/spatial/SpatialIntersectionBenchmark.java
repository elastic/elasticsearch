/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.spatial;

import org.apache.lucene.document.ShapeField;
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.common.geo.LuceneGeometriesUtils;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.lucene.spatial.CentroidCalculator;
import org.elasticsearch.lucene.spatial.Component2DVisitor;
import org.elasticsearch.lucene.spatial.CoordinateEncoder;
import org.elasticsearch.lucene.spatial.GeometryDocValueReader;
import org.elasticsearch.lucene.spatial.GeometryDocValueWriter;
import org.elasticsearch.lucene.spatial.TriangleTreeVisitor;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.UNSPECIFIED;

/**
 * Benchmarks comparing two approaches for evaluating whether two geometries intersect and for
 * computing their intersection geometry:
 *
 * <ol>
 *   <li><b>Lucene triangle-tree predicate</b> — encodes geometry A as triangle-tree doc-values,
 *       converts geometry B to a {@link Component2D}, and evaluates the
 *       {@link ShapeField.QueryRelation#INTERSECTS} predicate. This is the path used by
 *       {@code ST_INTERSECTS}.</li>
 *   <li><b>JTS intersection geometry</b> — decodes both geometries to JTS, calls
 *       {@code Geometry.intersection()}, and re-encodes the result to WKB. This is the path used
 *       by {@code ST_INTERSECTION}.</li>
 * </ol>
 *
 * <p>The {@code scenario} parameter controls whether the two geometries overlap, are disjoint, or
 * one contains the other. The {@code vertexCount} parameter controls the polygon complexity
 * (number of vertices per polygon).
 *
 * <p>Run with:
 * <pre>
 *   ./gradlew :benchmarks:run --args='SpatialIntersectionBenchmark'
 * </pre>
 */
@Fork(1)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
public class SpatialIntersectionBenchmark {

    /** Controls spatial relationship between the two polygons. */
    @Param({ "overlapping", "disjoint", "contained" })
    public String scenario;

    /**
     * Number of vertices per polygon edge side; total vertex count is approximately
     * {@code 4 * vertexCount + 1}. Larger values create more complex polygons with finer edges.
     */
    @Param({ "1", "4", "16", "64", "4096" })
    public int vertexCount;

    private static final GeoShapeIndexer GEO_SHAPE_INDEXER = new GeoShapeIndexer(Orientation.CCW, "benchmark-field");
    private static final GeometryFactory JTS_FACTORY = new GeometryFactory();

    /** Pre-built WKB representations of the two geometries. */
    private BytesRef wkbA;
    private BytesRef wkbB;

    /**
     * Pre-built triangle-tree doc-values for geometry A (avoids measuring encoding overhead in the
     * predicate benchmark).
     */
    private BytesRef docValuesA;

    /** Pre-built Lucene {@link Component2D} for geometry B (avoids measuring conversion overhead). */
    private Component2D component2dB;

    /** Pre-built JTS geometry for B, used by the triangle-decomposition area benchmark. */
    private org.locationtech.jts.geom.Geometry jtsBPrebuilt;

    @Setup
    public void setup() throws IOException {
        Utils.configureBenchmarkLogging();
        Polygon a;
        Polygon b;
        switch (scenario) {
            case "overlapping" -> {
                a = grid(-5, 5, -5, 5, vertexCount);
                b = grid(0, 10, 0, 10, vertexCount);
            }
            case "disjoint" -> {
                a = grid(-10, -1, -10, -1, vertexCount);
                b = grid(1, 10, 1, 10, vertexCount);
            }
            case "contained" -> {
                a = grid(-10, 10, -10, 10, vertexCount);
                b = grid(-1, 1, -1, 1, vertexCount);
            }
            default -> throw new IllegalArgumentException("Unknown scenario: " + scenario);
        }

        wkbA = GEO.asWkb(a);
        wkbB = GEO.asWkb(b);

        CentroidCalculator centroid = new CentroidCalculator();
        centroid.add(a);
        docValuesA = GeometryDocValueWriter.write(GEO_SHAPE_INDEXER.indexShape(a), CoordinateEncoder.GEO, centroid);

        LatLonGeometry[] luceneGeoms = LuceneGeometriesUtils.toLatLonGeometry(b, true, t -> {});
        component2dB = LatLonGeometry.create(luceneGeoms);

        try {
            jtsBPrebuilt = UNSPECIFIED.wkbToJtsGeometry(wkbB);
        } catch (org.locationtech.jts.io.ParseException e) {
            throw new IOException("Failed to parse WKB for geometry B", e);
        }
    }

    /**
     * Benchmark the full Lucene triangle-tree INTERSECTS predicate from source WKB.
     * This path mirrors what {@code ST_INTERSECTS} does when neither argument is from doc-values:
     * geometry A is encoded to triangle-tree doc-values (quantizing its coordinates), and geometry B
     * is converted to a {@link Component2D}.
     */
    @Benchmark
    public boolean lucenePredicateFromSource(Blackhole bh) throws IOException {
        Geometry geomA = UNSPECIFIED.wkbToGeometry(wkbA);
        CentroidCalculator centroid = new CentroidCalculator();
        centroid.add(geomA);
        BytesRef docValues = GeometryDocValueWriter.write(GEO_SHAPE_INDEXER.indexShape(geomA), CoordinateEncoder.GEO, centroid);

        Geometry geomB = UNSPECIFIED.wkbToGeometry(wkbB);
        LatLonGeometry[] luceneGeoms = LuceneGeometriesUtils.toLatLonGeometry(geomB, true, t -> {});
        Component2D c2d = LatLonGeometry.create(luceneGeoms);

        GeometryDocValueReader reader = new GeometryDocValueReader();
        reader.reset(docValues);
        Component2DVisitor visitor = Component2DVisitor.getVisitor(c2d, ShapeField.QueryRelation.INTERSECTS, CoordinateEncoder.GEO);
        reader.visit(visitor);
        return visitor.matches();
    }

    /**
     * Benchmark the Lucene predicate where geometry A comes from doc-values (triangle-tree already
     * encoded on disk) and geometry B is a pre-compiled {@link Component2D}. This is the hot path
     * for indexed {@code geo_shape} fields.
     */
    @Benchmark
    public boolean lucenePredicateFromDocValues(Blackhole bh) throws IOException {
        GeometryDocValueReader reader = new GeometryDocValueReader();
        reader.reset(docValuesA);
        Component2DVisitor visitor = Component2DVisitor.getVisitor(
            component2dB,
            ShapeField.QueryRelation.INTERSECTS,
            CoordinateEncoder.GEO
        );
        reader.visit(visitor);
        return visitor.matches();
    }

    /**
     * Benchmark the full JTS intersection computation from source WKB, returning the intersection
     * area. This is the apples-to-apples counterpart to {@link #luceneTriangleAreaFromDocValues}.
     */
    @Benchmark
    public double jtsIntersectionAreaFromSource(Blackhole bh) throws IOException {
        org.locationtech.jts.geom.Geometry jtsA;
        org.locationtech.jts.geom.Geometry jtsB;
        try {
            jtsA = UNSPECIFIED.wkbToJtsGeometry(wkbA);
            jtsB = UNSPECIFIED.wkbToJtsGeometry(wkbB);
        } catch (org.locationtech.jts.io.ParseException e) {
            throw new IOException("Failed to parse WKB", e);
        }
        return jtsA.intersection(jtsB).getArea();
    }

    /**
     * Benchmark intersection-area computation via Lucene triangle-tree decomposition.
     * Geometry A is read from pre-built doc-values; the triangle tree is visited and each triangle
     * is intersected with geometry B (pre-built JTS) to accumulate the total intersection area.
     * Because the triangle decomposition produces non-overlapping triangles, areas can be summed
     * directly without a union step.
     * <p>
     * This is the apples-to-apples counterpart to {@link #jtsIntersectionAreaFromSource}: both
     * return the area of the intersection, but via different algorithms.
     */
    @Benchmark
    public double luceneTriangleAreaFromDocValues(Blackhole bh) throws IOException {
        GeometryDocValueReader reader = new GeometryDocValueReader();
        reader.reset(docValuesA);
        IntersectionAreaVisitor visitor = new IntersectionAreaVisitor(CoordinateEncoder.GEO, jtsBPrebuilt);
        reader.visit(visitor);
        return visitor.getArea();
    }

    // -------------------------------------------------------------------------
    // Triangle-tree intersection-area visitor
    // -------------------------------------------------------------------------

    /**
     * A {@link TriangleTreeVisitor.TriangleTreeDecodedVisitor} that accumulates the intersection
     * area between each decoded triangle and a fixed query geometry. Because the triangle
     * decomposition of a polygon produces non-overlapping triangles, the per-triangle areas can be
     * summed without a union step.
     */
    private static class IntersectionAreaVisitor extends TriangleTreeVisitor.TriangleTreeDecodedVisitor {

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
    // Polygon construction helpers
    // -------------------------------------------------------------------------

    /**
     * Build a polygon approximating a rectangle by placing {@code pointsPerSide} evenly-spaced
     * vertices along each of the four sides. This produces a polygon with
     * {@code 4 * pointsPerSide + 1} vertices (the closing vertex is a repeat of the first).
     * A {@code pointsPerSide} value of 1 produces a simple 5-vertex rectangle.
     */
    private static Polygon grid(double minLon, double maxLon, double minLat, double maxLat, int pointsPerSide) {
        int n = 4 * pointsPerSide;
        double[] lons = new double[n + 1];
        double[] lats = new double[n + 1];
        int i = 0;
        // bottom edge: left → right
        for (int k = 0; k < pointsPerSide; k++) {
            lons[i] = minLon + (maxLon - minLon) * k / pointsPerSide;
            lats[i++] = minLat;
        }
        // right edge: bottom → top
        for (int k = 0; k < pointsPerSide; k++) {
            lons[i] = maxLon;
            lats[i++] = minLat + (maxLat - minLat) * k / pointsPerSide;
        }
        // top edge: right → left
        for (int k = 0; k < pointsPerSide; k++) {
            lons[i] = maxLon - (maxLon - minLon) * k / pointsPerSide;
            lats[i++] = maxLat;
        }
        // left edge: top → bottom
        for (int k = 0; k < pointsPerSide; k++) {
            lons[i] = minLon;
            lats[i++] = maxLat - (maxLat - minLat) * k / pointsPerSide;
        }
        // close the ring
        lons[n] = lons[0];
        lats[n] = lats[0];
        return new Polygon(new LinearRing(lons, lats));
    }
}
