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
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.lucene.spatial.CentroidCalculator;
import org.elasticsearch.lucene.spatial.CoordinateEncoder;
import org.elasticsearch.lucene.spatial.Extent;
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
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Compares doc-value layouts for points and multipoints: the legacy triangle tree, V2 with a triangle tree,
 * connectivity and vertex table, and the points-only V2 layout that {@link GeometryDocValueWriter#write} selects.
 * Unlike {@link GeometryDocValueBenchmark}, it adds a box query that can skip parts of the value.
 */
@Fork(1)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
public class PointsDocValueBenchmark {

    static {
        LogConfigurator.configureESLogging();
    }

    @Param({ "point", "fewRandomMultiPoint", "multiPoint", "localMultiPoint", "randomMultiPoint", "largeRandomMultiPoint" })
    public String geometryType;

    @Param({ "legacy", "v2Tree", "v2Points" })
    public String layout;

    private List<IndexableField> fields;
    private CentroidCalculator centroidCalculator;
    private Geometry normalizedGeometry;
    private BytesRef bytes;
    private final GeometryDocValueReader reader = new GeometryDocValueReader();
    private int queryMinX, queryMinY, queryMaxX, queryMaxY;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        Geometry geometry = createGeometry(geometryType);
        GeoShapeIndexer indexer = new GeoShapeIndexer(Orientation.CCW, "benchmark");
        normalizedGeometry = indexer.normalize(geometry);
        fields = indexer.getIndexableFields(normalizedGeometry);
        centroidCalculator = new CentroidCalculator();
        centroidCalculator.add(geometry);
        bytes = write();

        reader.reset(bytes);
        Extent extent = reader.getExtent();
        // A query box over the middle tenth of the extent in each dimension, so subtrees and blocks can be pruned.
        long width = (long) extent.maxX() - extent.minX();
        long height = (long) extent.maxY() - extent.minY();
        queryMinX = (int) (extent.minX() + width * 9 / 20);
        queryMaxX = (int) (extent.minX() + width * 11 / 20);
        queryMinY = (int) (extent.minY() + height * 9 / 20);
        queryMaxY = (int) (extent.minY() + height * 11 / 20);

        Geometry reconstructed = reader.getGeometry(CoordinateEncoder.GEO);
        CountingVisitor all = new CountingVisitor();
        reader.reset(bytes);
        reader.visit(all);
        BoxVisitor box = new BoxVisitor(queryMinX, queryMinY, queryMaxX, queryMaxY);
        reader.reset(bytes);
        reader.visit(box);
        System.out.println(
            "=== "
                + geometryType
                + " / "
                + layout
                + ": bytes="
                + bytes.length
                + " visited="
                + all.count
                + " boxHits="
                + box.hits
                + " boxVisited="
                + box.visited
                + " reconstructed="
                + numPoints(reconstructed)
                + " ==="
        );
    }

    private BytesRef write() throws IOException {
        return switch (layout) {
            case "legacy" -> GeometryDocValueWriter.writeLegacy(fields, CoordinateEncoder.GEO, centroidCalculator);
            case "v2Tree" -> GeometryDocValueWriter.writeV2(fields, CoordinateEncoder.GEO, centroidCalculator, List.of(normalizedGeometry));
            case "v2Points" -> GeometryDocValueWriter.write(fields, CoordinateEncoder.GEO, centroidCalculator, List.of(normalizedGeometry));
            default -> throw new IllegalArgumentException("Unknown layout: " + layout);
        };
    }

    private static int numPoints(Geometry geometry) {
        return switch (geometry) {
            case Point p -> 1;
            case MultiPoint mp -> mp.size();
            default -> throw new IllegalStateException("unexpected " + geometry);
        };
    }

    @Benchmark
    public BytesRef writeDocValue() throws IOException {
        return write();
    }

    @Benchmark
    public void visitTree(Blackhole bh) throws IOException {
        reader.reset(bytes);
        CountingVisitor visitor = new CountingVisitor();
        reader.visit(visitor);
        bh.consume(visitor.count);
    }

    @Benchmark
    public void visitBoxQuery(Blackhole bh) throws IOException {
        reader.reset(bytes);
        BoxVisitor visitor = new BoxVisitor(queryMinX, queryMinY, queryMaxX, queryMaxY);
        reader.visit(visitor);
        bh.consume(visitor.hits);
    }

    @Benchmark
    public void reconstruct(Blackhole bh) throws IOException {
        reader.reset(bytes);
        bh.consume(reader.getGeometry(CoordinateEncoder.GEO));
    }

    private static Geometry createGeometry(String type) {
        return switch (type) {
            case "point" -> new Point(5.0, 10.0);
            case "fewRandomMultiPoint" -> randomMultiPoint(4, 0, 0, 180);
            case "multiPoint" -> diagonalMultiPoint(100);
            case "localMultiPoint" -> randomMultiPoint(100, 2.35, 48.85, 0.01);
            case "randomMultiPoint" -> randomMultiPoint(100, 0, 0, 180);
            case "largeRandomMultiPoint" -> randomMultiPoint(1000, 0, 0, 180);
            default -> throw new IllegalArgumentException("Unknown geometry type: " + type);
        };
    }

    /** The same points as {@code GeometryDocValueBenchmark}'s {@code multiPoint}: a diagonal across the world. */
    private static MultiPoint diagonalMultiPoint(int count) {
        List<Point> points = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            points.add(new Point(-180.0 + (360.0 * i / count), -90.0 + (180.0 * i / count)));
        }
        return new MultiPoint(points);
    }

    /** Random points in random order within {@code span} degrees of a center, clamped to valid coordinates. */
    private static MultiPoint randomMultiPoint(int count, double lon, double lat, double span) {
        Random random = new Random(42);
        List<Point> points = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            double x = Math.max(-180, Math.min(180, lon + (random.nextDouble() * 2 - 1) * span));
            double y = Math.max(-90, Math.min(90, lat + (random.nextDouble() * 2 - 1) * span / 2));
            points.add(new Point(x, y));
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
        public void visitLine(int aX, int aY, int bX, int bY, byte metadata) {}

        @Override
        public void visitTriangle(int aX, int aY, int bX, int bY, int cX, int cY, byte metadata) {}

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

    /** Counts points inside a box, pruning subtrees and blocks that cannot intersect it, like a relation query. */
    private static class BoxVisitor implements TriangleTreeVisitor {
        final int minX, minY, maxX, maxY;
        int hits;
        int visited;

        BoxVisitor(int minX, int minY, int maxX, int maxY) {
            this.minX = minX;
            this.minY = minY;
            this.maxX = maxX;
            this.maxY = maxY;
        }

        @Override
        public void visitPoint(int x, int y) {
            visited++;
            if (x >= minX && x <= maxX && y >= minY && y <= maxY) {
                hits++;
            }
        }

        @Override
        public void visitLine(int aX, int aY, int bX, int bY, byte metadata) {}

        @Override
        public void visitTriangle(int aX, int aY, int bX, int bY, int cX, int cY, byte metadata) {}

        @Override
        public boolean push() {
            return true;
        }

        @Override
        public boolean pushX(int minX) {
            return minX <= maxX;
        }

        @Override
        public boolean pushY(int minY) {
            return minY <= maxY;
        }

        @Override
        public boolean push(int maxX, int maxY) {
            return maxX >= minX && maxY >= minY;
        }

        @Override
        public boolean push(int minX, int minY, int maxX, int maxY) {
            return minX <= this.maxX && maxX >= this.minX && minY <= this.maxY && maxY >= this.minY;
        }
    }
}
