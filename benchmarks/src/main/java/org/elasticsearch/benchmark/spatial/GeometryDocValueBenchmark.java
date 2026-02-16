/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.spatial;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.lucene.spatial.CentroidCalculator;
import org.elasticsearch.lucene.spatial.CoordinateEncoder;
import org.elasticsearch.lucene.spatial.GeometryDocValueReader;
import org.elasticsearch.lucene.spatial.GeometryDocValueWriter;
import org.elasticsearch.lucene.spatial.TriangleTreeVisitor;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks for geometry doc-value writing and reading in both legacy and V2 formats.
 *
 * <p>Measures:
 * <ul>
 *   <li>Write throughput for legacy vs V2 format</li>
 *   <li>Read throughput: centroid, extent, tree visitation, geometry reconstruction</li>
 *   <li>Storage size comparison (reported via setup output)</li>
 * </ul>
 *
 * <p>Run with: {@code ./gradlew :benchmarks:run --args 'GeometryDocValueBenchmark'}
 */
@Fork(1)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
public class GeometryDocValueBenchmark {

    @Param({ "point", "simplePoly", "complexPoly", "multiPoint" })
    public String geometryType;

    private Geometry geometry;
    private Geometry normalizedGeometry;
    private List<IndexableField> tessellatedFields;
    private CentroidCalculator centroidCalculator;

    private BytesRef legacyBytes;
    private BytesRef v2Bytes;

    private final GeometryDocValueReader reader = new GeometryDocValueReader();

    private boolean sizeReported;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        geometry = createGeometry(geometryType);

        GeoShapeIndexer indexer = new GeoShapeIndexer(Orientation.CCW, "benchmark");
        normalizedGeometry = indexer.normalize(geometry);
        tessellatedFields = indexer.getIndexableFields(normalizedGeometry);

        centroidCalculator = new CentroidCalculator();
        centroidCalculator.add(geometry);

        legacyBytes = GeometryDocValueWriter.writeLegacy(tessellatedFields, CoordinateEncoder.GEO, centroidCalculator);
        v2Bytes = GeometryDocValueWriter.write(tessellatedFields, CoordinateEncoder.GEO, centroidCalculator, List.of(normalizedGeometry));

        if (sizeReported == false) {
            sizeReported = true;
            System.out.println("=== Storage size for " + geometryType + " ===");
            System.out.println("  Triangles:    " + tessellatedFields.size());
            System.out.println("  Legacy bytes: " + legacyBytes.length);
            System.out.println("  V2 bytes:     " + v2Bytes.length);
            System.out.println(
                "  Delta:        "
                    + (v2Bytes.length - legacyBytes.length)
                    + " bytes ("
                    + String.format("%.1f%%", 100.0 * (v2Bytes.length - legacyBytes.length) / legacyBytes.length)
                    + ")"
            );
        }
    }

    // ---- Write benchmarks ----

    @Benchmark
    public BytesRef writeLegacy() throws IOException {
        return GeometryDocValueWriter.writeLegacy(tessellatedFields, CoordinateEncoder.GEO, centroidCalculator);
    }

    @Benchmark
    public BytesRef writeV2() throws IOException {
        return GeometryDocValueWriter.write(tessellatedFields, CoordinateEncoder.GEO, centroidCalculator, List.of(normalizedGeometry));
    }

    // ---- Read centroid benchmarks ----

    @Benchmark
    public void readCentroidLegacy(Blackhole bh) throws IOException {
        reader.reset(legacyBytes);
        bh.consume(reader.getCentroidX());
        bh.consume(reader.getCentroidY());
        bh.consume(reader.getDimensionalShapeType());
        bh.consume(reader.getSumCentroidWeight());
    }

    @Benchmark
    public void readCentroidV2(Blackhole bh) throws IOException {
        reader.reset(v2Bytes);
        bh.consume(reader.getCentroidX());
        bh.consume(reader.getCentroidY());
        bh.consume(reader.getDimensionalShapeType());
        bh.consume(reader.getSumCentroidWeight());
    }

    // ---- Read extent benchmarks ----

    @Benchmark
    public void readExtentLegacy(Blackhole bh) throws IOException {
        reader.reset(legacyBytes);
        bh.consume(reader.getExtent());
    }

    @Benchmark
    public void readExtentV2(Blackhole bh) throws IOException {
        reader.reset(v2Bytes);
        bh.consume(reader.getExtent());
    }

    // ---- Tree visit benchmarks ----

    @Benchmark
    public void visitTreeLegacy(Blackhole bh) throws IOException {
        reader.reset(legacyBytes);
        CountingVisitor visitor = new CountingVisitor();
        reader.visit(visitor);
        bh.consume(visitor.count);
    }

    @Benchmark
    public void visitTreeV2(Blackhole bh) throws IOException {
        reader.reset(v2Bytes);
        CountingVisitor visitor = new CountingVisitor();
        reader.visit(visitor);
        bh.consume(visitor.count);
    }

    // ---- Geometry reconstruction benchmark (V2 only) ----

    @Benchmark
    public void reconstructGeometryV2(Blackhole bh) throws IOException {
        reader.reset(v2Bytes);
        bh.consume(reader.getGeometry(CoordinateEncoder.GEO));
    }

    // ---- Helper methods ----

    private static Geometry createGeometry(String type) {
        return switch (type) {
            case "point" -> new Point(5.0, 10.0);
            case "simplePoly" -> new Polygon(new LinearRing(new double[] { 0, 10, 10, 0, 0 }, new double[] { 0, 0, 10, 10, 0 }));
            case "complexPoly" -> createStarPolygon(500);
            case "multiPoint" -> createMultiPoint(100);
            default -> throw new IllegalArgumentException("Unknown geometry type: " + type);
        };
    }

    /**
     * Creates a star-shaped polygon with many vertices that tessellates into many triangles.
     * The polygon alternates between an outer and inner radius, creating a gear/star shape
     * with {@code numPoints} teeth.
     */
    private static Polygon createStarPolygon(int numPoints) {
        int totalVertices = numPoints * 2;
        double[] lons = new double[totalVertices + 1];
        double[] lats = new double[totalVertices + 1];
        double centerLon = 0.0;
        double centerLat = 0.0;
        double outerRadius = 10.0;
        double innerRadius = 5.0;

        for (int i = 0; i < totalVertices; i++) {
            double angle = 2.0 * Math.PI * i / totalVertices;
            double radius = (i % 2 == 0) ? outerRadius : innerRadius;
            lons[i] = centerLon + radius * Math.cos(angle);
            lats[i] = centerLat + radius * Math.sin(angle);
        }
        lons[totalVertices] = lons[0];
        lats[totalVertices] = lats[0];
        return new Polygon(new LinearRing(lons, lats));
    }

    private static MultiPoint createMultiPoint(int count) {
        List<Point> points = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            double lon = -180.0 + (360.0 * i / count);
            double lat = -90.0 + (180.0 * i / count);
            points.add(new Point(lon, lat));
        }
        return new MultiPoint(points);
    }

    private static class CountingVisitor implements TriangleTreeVisitor {
        int count;

        @Override
        public void visitPoint(int x, int y) {
            count++;
        }

        @Override
        public void visitLine(int aX, int aY, int bX, int bY, byte metadata) {
            count++;
        }

        @Override
        public void visitTriangle(int aX, int aY, int bX, int bY, int cX, int cY, byte metadata) {
            count++;
        }

        @Override
        public boolean push() {
            return true;
        }

        @Override
        public boolean pushX(int minX) {
            return true;
        }

        @Override
        public boolean pushY(int minY) {
            return true;
        }

        @Override
        public boolean push(int maxX, int maxY) {
            return true;
        }

        @Override
        public boolean push(int minX, int minY, int maxX, int maxY) {
            return true;
        }
    }
}
