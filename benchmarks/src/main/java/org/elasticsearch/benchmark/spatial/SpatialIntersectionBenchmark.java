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
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.BinarySpatialFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.GeometryOperator;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.HybridGeometryOperator;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.JtsGeometryOperator;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.TriangleDecompositionOperator;
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

    /** Operator instances used by the operator benchmarks. */
    private GeometryOperator jtsOperator;
    private GeometryOperator hybridOperator;
    private GeometryOperator triangleOperator;

    /** Pre-decoded JTS geometries, used by the prebuilt-operator benchmark as a raw-JTS baseline. */
    private org.locationtech.jts.geom.Geometry prebuiltJtsA;
    private org.locationtech.jts.geom.Geometry prebuiltJtsB;

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

        jtsOperator = JtsGeometryOperator.INTERSECTION;
        hybridOperator = HybridGeometryOperator.intersection(BinarySpatialFunction.SpatialCrsType.GEO);
        triangleOperator = TriangleDecompositionOperator.intersection(BinarySpatialFunction.SpatialCrsType.GEO);

        try {
            prebuiltJtsA = UNSPECIFIED.wkbToJtsGeometry(wkbA);
            prebuiltJtsB = UNSPECIFIED.wkbToJtsGeometry(wkbB);
        } catch (org.locationtech.jts.io.ParseException e) {
            throw new IOException("Failed to parse WKB for geometries", e);
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
     * Benchmark the full JTS intersection computation from source WKB (decode + operate + encode).
     * The operator internally decodes WKB to JTS, applies the operation, and re-encodes to WKB.
     */
    @Benchmark
    public BytesRef jtsOperatorFromSource() throws IOException {
        return jtsOperator.apply(wkbA, wkbB);
    }

    /**
     * Benchmark the hybrid (Lucene pre-check + JTS fallback) intersection from source WKB
     * (decode + operate + encode).
     */
    @Benchmark
    public BytesRef hybridOperatorFromSource() throws IOException {
        return hybridOperator.apply(wkbA, wkbB);
    }

    /**
     * Benchmark the triangle-decomposition intersection from source WKB
     * (decode + operate + encode).
     */
    @Benchmark
    public BytesRef triangleOperatorFromSource() throws IOException {
        return triangleOperator.apply(wkbA, wkbB);
    }

    /**
     * Benchmark the JTS intersection from pre-built JTS geometries (pure JTS cost, no
     * WKB decode overhead). This serves as a baseline for measuring the decode/encode
     * overhead in the {@code FromSource} variants.
     */
    @Benchmark
    public BytesRef jtsOperatorFromPrebuilt() {
        return UNSPECIFIED.jtsGeometryToWkb(prebuiltJtsA.intersection(prebuiltJtsB));
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
