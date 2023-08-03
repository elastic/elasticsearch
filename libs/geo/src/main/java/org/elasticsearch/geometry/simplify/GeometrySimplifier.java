/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.geometry.simplify;

import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The geometry simplifier can simplify any geometry, and does so by internally making use of the StreamingGeometrySimplifier,
 * to minimize memory usages. When simplifying a complex geometry, for example a Polygon with holes, with the simplification threshold
 * <code>k=1000</code>, the memory usage should be:
 * <ul>
 *     <li>Original complete geometry (in this example, one shell and several holes)</li>
 *     <li>Internal memory for simplifying the shell is <code>O(k)</code>,
 *     so an array of 1000 objects containing x, y and an estimated error.</li>
 *     <li>Internal memory for simplifying each hole, which is a scaled down <code>k</code> based on the relative size of the hole.
 *     For example, if the shell is 100k points, and a hole is 1k points, we will scale the hole k to just 10 points.</li>
 *     <li>The finally simplified geometry, which occupies a similar amount of memory to the simplification algorithm,
 *     in other words <code>O(k)</code></li>
 * </ul>
 * This approach of limiting the algorithms to some deterministic linear function of the simplification threshold k is used for all
 * complex geometries, even <code>GeometryCollection</code>.
 * <p>
 * Note, however, that no attempt is made to prevent invalid geometries from forming. For example, when simplifying a polygon,
 * the removal of points can cause lines to cross, creating invalid polygons. Any usage of this algorithm needs to consider that
 * possibility. If the simplification is only required for visualization, and the visualization code is tolerant of line crossings,
 * this is not an issue. But if you need valid simplified geometries, do additional validation on the final results before using them.
 * <p>
 * If the incoming data is a stream of points, or x and y coordinates, it is even more memory efficient to directly use the
 * <code>StreamingGeometrySimplifier</code> which will only maintain the single array of length <code>k</code>, and produce a final
 * geometry of <code>O(k)</code> at the end, so the total size of the original geometry is not a factor.
 */
public abstract class GeometrySimplifier<T extends Geometry> {
    protected final int maxPoints;
    protected final SimplificationErrorCalculator calculator;
    protected final StreamingGeometrySimplifier.Monitor monitor;
    protected String description;
    protected final StreamingGeometrySimplifier<T> innerSimplifier;

    protected GeometrySimplifier(
        String description,
        int maxPoints,
        SimplificationErrorCalculator calculator,
        StreamingGeometrySimplifier.Monitor monitor,
        StreamingGeometrySimplifier<T> innerSimplifier
    ) {
        this.description = description;
        this.maxPoints = maxPoints;
        this.calculator = calculator;
        this.monitor = monitor;
        this.innerSimplifier = innerSimplifier;
    }

    /**
     * Simplify an entire geometry in a non-streaming fashion
     */
    public abstract T simplify(T geometry);

    /**
     * When re-using a simplifier instance, call <code>reset()</code> to clear internal memory
     */
    public void reset() {
        if (innerSimplifier != null) {
            this.innerSimplifier.reset();
        }
    }

    protected void notifyMonitorSimplificationStart() {
        if (monitor != null) {
            monitor.startSimplification(description, maxPoints);
        }
    }

    protected void notifyMonitorSimplificationEnd() {
        if (monitor != null) {
            monitor.endSimplification(description, getCurrentPoints());
        }
    }

    protected List<SimplificationErrorCalculator.PointLike> getCurrentPoints() {
        ArrayList<SimplificationErrorCalculator.PointLike> simplification = new ArrayList<>();
        if (innerSimplifier != null) {
            simplification.addAll(Arrays.asList(innerSimplifier.points).subList(0, innerSimplifier.length));
        }
        return simplification;
    }

    /**
     * Simplifies a Line geometry to the specified maximum number of points.
     */
    public static class LineSimplifier extends GeometrySimplifier<Line> {
        public LineSimplifier(int maxPoints, SimplificationErrorCalculator calculator) {
            this(maxPoints, calculator, null);
        }

        public LineSimplifier(int maxPoints, SimplificationErrorCalculator calculator, StreamingGeometrySimplifier.Monitor monitor) {
            super("Line", maxPoints, calculator, monitor, new StreamingGeometrySimplifier.LineSimplifier(maxPoints, calculator, monitor));
        }

        @Override
        public Line simplify(Line line) {
            reset();
            notifyMonitorSimplificationStart();
            try {
                if (line.length() <= maxPoints) {
                    return line;
                }
                for (int i = 0; i < line.length(); i++) {
                    innerSimplifier.consume(line.getX(i), line.getY(i));
                }
                return innerSimplifier.produce();
            } finally {
                notifyMonitorSimplificationEnd();
            }
        }
    }

    /**
     * This behaves the same as the Line simplifier except that it assumes the first and last point are the same point.
     * The minimum acceptable polygon size is therefor 4 points.
     */
    public static class LinearRingSimplifier extends GeometrySimplifier<LinearRing> {
        public LinearRingSimplifier(int maxPoints, SimplificationErrorCalculator calculator) {
            this(maxPoints, calculator, null);
        }

        public LinearRingSimplifier(int maxPoints, SimplificationErrorCalculator calculator, StreamingGeometrySimplifier.Monitor monitor) {
            super(
                "LinearRing",
                maxPoints,
                calculator,
                monitor,
                new StreamingGeometrySimplifier.LinearRingSimplifier(maxPoints, calculator, monitor)
            );
            assert maxPoints >= 4;
        }

        @Override
        public LinearRing simplify(LinearRing ring) {
            reset();
            notifyMonitorSimplificationStart();
            try {
                if (ring.length() <= maxPoints) {
                    return ring;
                }
                for (int i = 0; i < ring.length(); i++) {
                    innerSimplifier.consume(ring.getX(i), ring.getY(i));
                }
                return innerSimplifier.produce();
            } finally {
                notifyMonitorSimplificationEnd();
            }
        }
    }

    /**
     * This class wraps a collection of LinearRing simplifiers for polygon holes.
     * It also uses its own simplifier capabilities for the outer ring simplification.
     * The outer ring is simplified to the specified maxPoints, while the holes are simplified
     * to a maxPoints value that is a fraction of the holes size compared to the outer ring size.
     * <p>
     * Note that while the polygon simplifier can work in both streaming and non-streaming modes,
     * the streaming mode will assume all points consumed belong to the outer shell. If you want
     * to simplify polygons with holes, use the <code>simplify(polygon)</code> method instead.
     */
    public static class PolygonSimplifier extends GeometrySimplifier<Polygon> {
        public PolygonSimplifier(int maxPoints, SimplificationErrorCalculator calculator) {
            this(maxPoints, calculator, null);
        }

        public PolygonSimplifier(int maxPoints, SimplificationErrorCalculator calculator, StreamingGeometrySimplifier.Monitor monitor) {
            super(
                "Polygon",
                maxPoints,
                calculator,
                monitor,
                new StreamingGeometrySimplifier.PolygonSimplifier(maxPoints, calculator, monitor)
            );
        }

        @Override
        public Polygon simplify(Polygon geometry) {
            reset();
            notifyMonitorSimplificationStart();
            try {
                LinearRing ring = geometry.getPolygon();
                if (ring.length() <= maxPoints) {
                    return geometry;
                }
                for (int i = 0; i < ring.length(); i++) {
                    innerSimplifier.consume(ring.getX(i), ring.getY(i));
                }
                ArrayList<LinearRing> holes = new ArrayList<>(geometry.getNumberOfHoles());
                for (int i = 0; i < geometry.getNumberOfHoles(); i++) {
                    LinearRing hole = geometry.getHole(i);
                    double simplificationFactor = (double) maxPoints / ring.length();
                    int maxHolePoints = Math.max(4, (int) (simplificationFactor * hole.length()));
                    var holeSimplifier = new GeometrySimplifier.LinearRingSimplifier(maxHolePoints, calculator, this.monitor);
                    holeSimplifier.description = "Polygon.Hole";
                    holes.add(holeSimplifier.simplify(hole));
                }
                return new Polygon(StreamingGeometrySimplifier.produceLinearRing(innerSimplifier), holes);
            } finally {
                notifyMonitorSimplificationEnd();
            }
        }
    }

    /**
     * This class wraps a collection of Polygon simplifiers.
     * It does not make use of its own simplifier capabilities.
     * The largest inner polygon is simplified to the specified maxPoints, while the rest are simplified
     * to a maxPoints value that is a fraction of their size compared to the largest size.
     * <p>
     * Note that this simplifier cannot work in streaming mode.
     * Since a MultiPolygon can contain more than one polygon,
     * the <code>consume(Point)</code> method would not know which polygon to add to.
     * If you need to use the streaming mode, separate the multi-polygon into individual polygons and use
     * the <code>Polygon</code> simplifier on each individually.
     */
    public static class MultiPolygonSimplifier extends GeometrySimplifier<MultiPolygon> {
        ArrayList<Integer> indexes = new ArrayList<>();

        public MultiPolygonSimplifier(int maxPoints, SimplificationErrorCalculator calculator) {
            this(maxPoints, calculator, null);
        }

        public MultiPolygonSimplifier(
            int maxPoints,
            SimplificationErrorCalculator calculator,
            StreamingGeometrySimplifier.Monitor monitor
        ) {
            super("MultiPolygon", maxPoints, calculator, monitor, null);
        }

        @Override
        public void reset() {
            super.reset();
            indexes.clear();
        }

        @Override
        public MultiPolygon simplify(MultiPolygon geometry) {
            ArrayList<Polygon> polygons = new ArrayList<>(geometry.size());
            int maxPolyLength = GeometrySimplifier.maxLengthOf(geometry);
            notifyMonitorSimplificationStart();
            for (int i = 0; i < geometry.size(); i++) {
                Polygon polygon = geometry.get(i);
                double simplificationFactor = (double) maxPoints / maxPolyLength;
                int maxPolyPoints = Math.max(4, (int) (simplificationFactor * polygon.getPolygon().length()));
                PolygonSimplifier simplifier = new PolygonSimplifier(maxPolyPoints, calculator, monitor);
                simplifier.description = "MultiPolygon.Polygon[" + i + "]";
                Polygon simplified = simplifier.simplify(polygon);
                if (simplified.getPolygon().length() > 0) {
                    // Invalid polygons (all points co-located) will not be simplified
                    polygons.add(simplified);
                    indexes.add(i);
                }
            }
            notifyMonitorSimplificationEnd();
            return new MultiPolygon(polygons);
        }

        /**
         * Provide the index of the original un-simplified polygon given the index of the simplified polygon.
         * This is only useful in the case that some incoming polygons were invalid, and excluded from the final geometry.
         */
        public int indexOf(int simplified) {
            return indexes.get(simplified);
        }
    }

    /**
     * This class wraps a collection of other simplifiers.
     * It does not make use of its own simplifier capabilities.
     * The largest inner geometry is simplified to the specified maxPoints, while the rest are simplified
     * to a maxPoints value that is a fraction of their size compared to the largest size.
     * <p>
     * Note that this simplifier cannot work in streaming mode, since it would not know what to add the points to.
     * If you need to use the streaming mode, separate the geometry collection into individual geometries and use
     * the <code>Polygon</code> or <code>LineString</code> simplifier on each individually.
     */
    public static class GeometryCollections extends GeometrySimplifier<GeometryCollection<?>> {
        public GeometryCollections(int maxPoints, SimplificationErrorCalculator calculator) {
            this(maxPoints, calculator, null);
        }

        public GeometryCollections(int maxPoints, SimplificationErrorCalculator calculator, StreamingGeometrySimplifier.Monitor monitor) {
            super("GeometryCollection", maxPoints, calculator, monitor, null);
        }

        @Override
        public GeometryCollection<?> simplify(GeometryCollection<?> collection) {
            ArrayList<Geometry> geometries = new ArrayList<>(collection.size());
            int maxGeometryLength = maxLengthOf(collection);
            notifyMonitorSimplificationStart();
            for (int i = 0; i < collection.size(); i++) {
                Geometry geometry = collection.get(i);
                double simplificationFactor = (double) maxPoints / maxGeometryLength;
                int maxLength = lengthOf(geometry);
                int maxPolyPoints = Math.max(4, (int) (simplificationFactor * maxLength));
                if (geometry instanceof Point point) {
                    var pointSimplifier = new Identity<Point>(maxPolyPoints, calculator, monitor);
                    pointSimplifier.description = "GeometryCollection.Point[" + i + "]";
                    geometries.add(pointSimplifier.simplify(point));
                } else if (geometry instanceof Line line) {
                    var lineSimplifier = new LineSimplifier(maxPolyPoints, calculator, monitor);
                    lineSimplifier.description = "GeometryCollection.Line[" + i + "]";
                    geometries.add(lineSimplifier.simplify(line));
                } else if (geometry instanceof Polygon polygon) {
                    var polygonSimplifier = new PolygonSimplifier(maxPolyPoints, calculator, monitor);
                    polygonSimplifier.description = "GeometryCollection.Polygon[" + i + "]";
                    geometries.add(polygonSimplifier.simplify(polygon));
                } else if (geometry instanceof MultiPolygon multiPolygon) {
                    var multiPolygonSimplifier = new MultiPolygonSimplifier(maxPolyPoints, calculator, monitor);
                    multiPolygonSimplifier.description = "GeometryCollection.MultiPolygon[" + i + "]";
                    geometries.add(multiPolygonSimplifier.simplify(multiPolygon));
                } else if (geometry instanceof GeometryCollection<?> g) {
                    var collectionSimplifier = new GeometryCollections(maxPolyPoints, calculator, monitor);
                    collectionSimplifier.description = "GeometryCollection.GeometryCollection[" + i + "]";
                    geometries.add(collectionSimplifier.simplify(g));
                } else {
                    throw new IllegalArgumentException("Unsupported geometry type: " + geometry.type());
                }
            }
            notifyMonitorSimplificationEnd();
            return new GeometryCollection<>(geometries);
        }
    }

    public static <G extends Geometry> GeometrySimplifier<G> simplifierFor(
        G geometry,
        int maxPoints,
        SimplificationErrorCalculator calculator,
        StreamingGeometrySimplifier.Monitor monitor
    ) {
        // TODO: Find a way to get this method to return specialized simplifiers for non-identity cases (eg. Line and Polygon)
        if (geometry instanceof Point || geometry instanceof Circle || geometry instanceof Rectangle || geometry instanceof MultiPoint) {
            return new Identity<>(maxPoints, calculator, monitor);
        } else {
            throw new IllegalArgumentException("Unsupported geometry type: " + geometry.type());
        }
    }

    /**
     * This simplifier simply returns the original geometry unsimplified.
     * It is useful for unsimplifiable geometries like Point, Rectangle and Circle.
     */
    public static class Identity<G extends Geometry> extends GeometrySimplifier<G> {
        public Identity(int maxPoints, SimplificationErrorCalculator calculator) {
            this(maxPoints, calculator, null);
        }

        public Identity(int maxPoints, SimplificationErrorCalculator calculator, StreamingGeometrySimplifier.Monitor monitor) {
            super("Identity", maxPoints, calculator, monitor, null);
        }

        @Override
        public G simplify(G geometry) {
            notifyMonitorSimplificationStart();
            try {
                return geometry;
            } finally {
                notifyMonitorSimplificationEnd();
            }
        }
    }

    static int lengthOf(Geometry geometry) {
        if (geometry instanceof Polygon polygon) {
            return polygon.getPolygon().length();
        } else if (geometry instanceof Point) {
            return 1;
        } else if (geometry instanceof Line line) {
            return line.length();
        } else if (geometry instanceof MultiPolygon multiPolygon) {
            int maxPolyLength = 0;
            for (int i = 0; i < multiPolygon.size(); i++) {
                Polygon polygon = multiPolygon.get(i);
                maxPolyLength = Math.max(maxPolyLength, polygon.getPolygon().length());
            }
            return maxPolyLength;
        } else if (geometry instanceof GeometryCollection<?> collection) {
            return maxLengthOf(collection);
        } else {
            throw new IllegalArgumentException("Unsupported geometry type: " + geometry.type());
        }
    }

    private static int maxLengthOf(GeometryCollection<?> collection) {
        int maxLength = 0;
        for (int i = 0; i < collection.size(); i++) {
            maxLength = Math.max(maxLength, lengthOf(collection.get(i)));
        }
        return maxLength;
    }

    private static int maxLengthOf(MultiPolygon polygons) {
        int maxLength = 0;
        for (int i = 0; i < polygons.size(); i++) {
            maxLength = Math.max(maxLength, lengthOf(polygons.get(i)));
        }
        return maxLength;
    }
}
