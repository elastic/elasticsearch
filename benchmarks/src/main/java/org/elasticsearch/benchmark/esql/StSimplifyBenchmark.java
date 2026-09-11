/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.esql;

import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.IterativeDouglasPeuckerSimplifier;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.simplify.DouglasPeuckerSimplifier;
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

import java.util.concurrent.TimeUnit;

/**
 * Compares {@link DouglasPeuckerSimplifier} (JTS recursive) against
 * {@link IterativeDouglasPeuckerSimplifier} (heap-stack iterative) across a range of polygon sizes
 * and tolerance values.
 *
 * <p>The iterative implementation was introduced to eliminate {@link StackOverflowError} on
 * geometries with many thousands of vertices. This benchmark confirms that the replacement carries
 * no throughput penalty, i.e. the heap-stack approach is at least as fast as the JVM call-stack
 * approach.
 *
 * <p>Geometry: a regular polygon (circle approximation) with {@code vertices} equally-spaced
 * points on a unit circle. This is worst-case for the recursive implementation because the
 * divide-and-conquer split always lands near the midpoint, giving maximum recursion depth
 * (~log₂(vertices)).
 *
 * <p>Tolerance dimension:
 * <ul>
 *   <li>{@code 0.0} — nothing is simplified; every vertex is retained. Measures pure traversal
 *       cost with no simplification work.</li>
 *   <li>{@code 0.01} — moderate simplification; interior vertices within 1% of the unit circle
 *       radius are removed.</li>
 *   <li>{@code 2.0} — aggressive simplification; the polygon collapses entirely to a degenerate
 *       result. Measures the fast-exit path.</li>
 * </ul>
 *
 * <p>A setup self-test asserts that both implementations produce geometrically equivalent output
 * (same vertex count) for the chosen tolerance before any timed measurement begins; a silent
 * disagreement would make any speedup meaningless.
 */
@Warmup(iterations = 4, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Fork(1)
public class StSimplifyBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    /** Number of vertices in the input polygon. */
    @Param({ "100", "1000", "10000", "50000" })
    public int vertices;

    /**
     * Simplification tolerance. See class Javadoc for the interpretation of each value.
     * 0.0 = no-op, 0.01 = moderate, 2.0 = full collapse.
     */
    @Param({ "0.0", "0.01", "2.0" })
    public double tolerance;

    private Geometry polygon;

    @Setup
    public void setup() {
        polygon = regularPolygon(vertices);

        // Self-test: both implementations must agree on the output vertex count.
        int recursiveCount = DouglasPeuckerSimplifier.simplify(polygon, tolerance).getNumPoints();
        int iterativeCount = IterativeDouglasPeuckerSimplifier.simplify(polygon, tolerance).getNumPoints();
        if (recursiveCount != iterativeCount) {
            throw new AssertionError(
                "Output mismatch for vertices="
                    + vertices
                    + " tolerance="
                    + tolerance
                    + ": recursive="
                    + recursiveCount
                    + " iterative="
                    + iterativeCount
            );
        }
    }

    @Benchmark
    public Geometry recursive() {
        return DouglasPeuckerSimplifier.simplify(polygon, tolerance);
    }

    @Benchmark
    public Geometry iterative() {
        return IterativeDouglasPeuckerSimplifier.simplify(polygon, tolerance);
    }

    /**
     * Builds a closed regular polygon with {@code n} vertices evenly distributed on the unit
     * circle. The first and last coordinates are identical (closed ring), giving {@code n + 1}
     * coordinates total as required by JTS LinearRing.
     */
    private static Geometry regularPolygon(int n) {
        GeometryFactory factory = new GeometryFactory();
        Coordinate[] coords = new Coordinate[n + 1];
        for (int i = 0; i < n; i++) {
            double angle = 2.0 * Math.PI * i / n;
            coords[i] = new Coordinate(Math.cos(angle), Math.sin(angle));
        }
        coords[n] = coords[0];
        return factory.createPolygon(coords);
    }
}
