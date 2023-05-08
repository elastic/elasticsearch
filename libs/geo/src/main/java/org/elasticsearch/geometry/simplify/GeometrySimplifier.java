/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.geometry.simplify;

import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Polygon;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

public abstract class GeometrySimplifier<T extends Geometry> {
    protected final int maxPoints;
    protected final SimplificationErrorCalculator calculator;
    protected final PointError[] points;
    protected PointError lastRemoved;
    protected final Monitor monitor;
    protected int length;
    protected int objCount = 0;
    protected String description;

    protected final PriorityQueue<PointError> queue = new PriorityQueue<>();

    protected GeometrySimplifier(String description, int maxPoints, SimplificationErrorCalculator calculator, Monitor monitor) {
        this.description = description;
        this.maxPoints = maxPoints;
        this.calculator = calculator;
        this.monitor = monitor;
        this.points = new PointError[maxPoints];
        this.length = 0;
    }

    /**
     * Simplify an entire geometry in a non-streaming fashion
     */
    public abstract T simplify(T geometry);

    /**
     * Initialize for simplifying using a stream of points
     */
    public void reset() {
        this.length = 0;
        this.queue.clear();
    }

    /**
     * Consume a single point on the stream of points to be simplified
     */
    public void consume(double x, double y) {
        PointError pointError = makePointErrorFor(length, x, y);
        if (length > 1) {
            // we need at least three points to calculate the error of the middle point
            points[length - 1].error = calculator.calculateError(points[length - 2], points[length - 1], pointError);
            queue.add(points[length - 1]);
        }
        if (length == maxPoints) {
            // Remove point with lowest error
            PointError toRemove = queue.remove();
            removeAndAdd(toRemove.index, pointError);
            notifyMonitorPointRemoved(toRemove);
        } else {
            this.points[length] = pointError;
            length++;
            notifyMonitorPointAdded();
        }
    }

    /**
     * Produce the simplified geometry from the consumed points
     */
    public abstract T produce();

    private PointError makePointErrorFor(int index, double x, double y) {
        if (index == maxPoints) {
            if (lastRemoved == null) {
                this.objCount++;
                return new PointError(index, x, y);
            } else {
                return lastRemoved.reset(index, x, y);
            }
        } else {
            if (points[index] == null) {
                this.objCount++;
                return new PointError(index, x, y);
            } else {
                return points[index].reset(index, x, y);
            }
        }
    }

    private void removeAndAdd(int toRemove, PointError pointError) {
        assert toRemove > 0;  // priority queue can never include first point as that always has zero error by definition
        this.lastRemoved = this.points[toRemove];
        // Shift all points to the right of the removed point over it in the array
        System.arraycopy(this.points, toRemove + 1, this.points, toRemove, maxPoints - toRemove - 1);
        // Add the new point to the end of the array
        this.points[length - 1] = pointError;
        // Reset all point indexes for points moved in the array
        for (int i = toRemove; i < length; i++) {
            points[i].index = i;
        }
        // Recalculate errors for points on either side of the removed point
        updateErrorAt(toRemove - 1);
        updateErrorAt(toRemove);
        // Update second last point error since we have a new last point
        if (toRemove < maxPoints - 1) { // if we removed the last point, we already updated it above, so don't bother here
            updateErrorAt(maxPoints - 2);
        }
    }

    private void updateErrorAt(int index) {
        if (index > 0 && index < length - 1) { // do not reset first and last points as they always have error 0 by definition
            double error = calculator.calculateError(points[index - 1], points[index], points[index + 1]);
            double delta = Math.abs(error - points[index].error);
            points[index].error = error;
            if (delta > 1e-10) {
                // If the error has changed, re-index the priority queue
                if (queue.remove(points[index])) {
                    queue.add(points[index]);
                }
            }
        }
    }

    /**
     * Each point on the geometry has an error estimate, which is a measure of how much error would be introduced
     * to the geometry should this point be removed from the geometry. This is a measure of how far from the
     * line connecting the previous and next points, this geometry lies. If it is on that line, the error would
     * be zero, since removing the point does not change the geometry.
     */
    public static class PointError implements SimplificationErrorCalculator.PointLike, Comparable<PointError> {
        private int index;
        private double x;
        private double y;
        double error = 0;

        PointError(int index, double x, double y) {
            this.index = index;
            this.x = x;
            this.y = y;
        }

        @Override
        public int compareTo(PointError o) {
            return (int) (Math.signum(this.error - o.error));
        }

        @Override
        public String toString() {
            return "[" + index + "] POINT( " + x + " " + y + " ) [error:" + error + "]";
        }

        @Override
        public double x() {
            return x;
        }

        @Override
        public double y() {
            return y;
        }

        public PointError reset(int index, double x, double y) {
            this.index = index;
            this.x = x;
            this.y = y;
            return this;
        }
    }

    /**
     * Implementation of this interface will receive calls with internal data at each step of the
     * simplification algorithm. This is of use for debugging complex cases, as well as gaining insight
     * into the way the algorithm works. Data provided in the callback includes:
     * <ul>
     *     <li>String description of current process</li>
     *     <li>List of points in current simplification</li>
     *     <li>Last point removed from the simplification</li>
     * </ul>
     * mode, list of points representing the current linked-list of internal nodes used for
     * triangulation, and a list of triangles so far created by the algorithm.
     */
    public interface Monitor {
        /** Every time a point is added to the collection, this method sends the resulting state */
        void pointAdded(String status, List<SimplificationErrorCalculator.PointLike> points);

        /** Every time a point is added and another is removed from the collection, this method sends the resulting state */
        void pointRemoved(
            String status,
            List<SimplificationErrorCalculator.PointLike> points,
            SimplificationErrorCalculator.PointLike removed,
            double error,
            SimplificationErrorCalculator.PointLike previous,
            SimplificationErrorCalculator.PointLike next
        );

        /**
         * When a new simplification or sub-simplification starts, this provides a description of the simplification,
         * as well as the current maxPoints target for this simplification. For a single simplification, maxPoints
         * will simply be the value passed to the constructor, but compound simplifications will calculate smaller
         * numbers for sub-simplifications (eg. holes in polygons, or shells in multi-polygons).
         */
        void startSimplification(String description, int maxPoints);

        /**
         * When simplification or sub-simplification is completed, this is called.
         */
        void endSimplification(String description, List<SimplificationErrorCalculator.PointLike> points);
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

    protected void notifyMonitorPointRemoved(PointError removed) {
        if (monitor != null) {
            PointError previous = points[removed.index - 1];
            PointError next = points[removed.index];
            monitor.pointRemoved(description + ".addAndRemovePoint()", getCurrentPoints(), removed, removed.error, previous, next);
        }
    }

    protected void notifyMonitorPointAdded() {
        if (monitor != null) {
            monitor.pointAdded(description + ".addPoint()", getCurrentPoints());
        }
    }

    private List<SimplificationErrorCalculator.PointLike> getCurrentPoints() {
        ArrayList<SimplificationErrorCalculator.PointLike> simplification = new ArrayList<>();
        for (int i = 0; i < length; i++) {
            simplification.add(points[i]);
        }
        return simplification;
    }

    /**
     * Simplifies a Line geometry to the specified maximum number of points.
     */
    public static class LineStrings extends GeometrySimplifier<Line> {
        public LineStrings(int maxPoints, SimplificationErrorCalculator calculator) {
            this(maxPoints, calculator, null);
        }

        public LineStrings(int maxPoints, SimplificationErrorCalculator calculator, Monitor monitor) {
            super("LineString", maxPoints, calculator, monitor);
        }

        @Override
        public Line simplify(Line line) {
            if (line.length() <= maxPoints) {
                return line;
            }
            reset();
            notifyMonitorSimplificationStart();
            for (int i = 0; i < line.length(); i++) {
                consume(line.getX(i), line.getY(i));
            }
            notifyMonitorSimplificationEnd();
            return produce();
        }

        @Override
        public Line produce() {
            if (length < 1) {
                throw new IllegalArgumentException("No points have been consumed");
            }
            double[] x = new double[length];
            double[] y = new double[length];
            for (int i = 0; i < length; i++) {
                x[i] = points[i].x;
                y[i] = points[i].y;
            }
            return new Line(x, y);
        }
    }

    /**
     * This behaves the same as the Line simplifier except that it assumes the first and last point are the same point.
     * The minimum acceptable polygon size is therefor 4 points.
     */
    public static class LinearRings extends GeometrySimplifier<LinearRing> {
        public LinearRings(int maxPoints, SimplificationErrorCalculator calculator) {
            this(maxPoints, calculator, null);
        }

        public LinearRings(int maxPoints, SimplificationErrorCalculator calculator, Monitor monitor) {
            super("LinearRing", maxPoints, calculator, monitor);
            assert maxPoints >= 4;
        }

        @Override
        public LinearRing simplify(LinearRing ring) {
            if (ring.length() <= maxPoints) {
                return ring;
            }
            reset();
            notifyMonitorSimplificationStart();
            for (int i = 0; i < ring.length(); i++) {
                consume(ring.getX(i), ring.getY(i));
            }
            notifyMonitorSimplificationEnd();
            return produce();
        }

        @Override
        public LinearRing produce() {
            return GeometrySimplifier.produceLinearRing(this);
        }
    }

    /**
     * This class wraps a collection of LinearRing simplifiers for polygon holes.
     * It also uses its own simplifier capabilities for the outer ring simplification.
     * The outer ring is simplified to the specified maxPoints, while the holes are simplified
     * to a maxPoints value that is a fraction of the holes size compared to the outer ring size.
     *
     * Note that while the polygon simplifier can work in both streaming and non-streaming modes,
     * the streaming mode will assume all points consumed belong to the outer shell. If you want
     * to simplify polygons with holes, use the <code>simplify(polygon)</code> method instead.
     */
    public static class Polygons extends GeometrySimplifier<Polygon> {
        ArrayList<GeometrySimplifier<LinearRing>> holeSimplifiers = new ArrayList<>();

        public Polygons(int maxPoints, SimplificationErrorCalculator calculator) {
            this(maxPoints, calculator, null);
        }

        public Polygons(int maxPoints, SimplificationErrorCalculator calculator, Monitor monitor) {
            super("Polygon", maxPoints, calculator, monitor);
        }

        @Override
        public void reset() {
            super.reset();
            holeSimplifiers.clear();
        }

        @Override
        public Polygon simplify(Polygon geometry) {
            LinearRing ring = geometry.getPolygon();
            if (ring.length() <= maxPoints) {
                return geometry;
            }
            reset();
            notifyMonitorSimplificationStart();
            for (int i = 0; i < ring.length(); i++) {
                consume(ring.getX(i), ring.getY(i));
            }
            notifyMonitorSimplificationEnd();
            for (int i = 0; i < geometry.getNumberOfHoles(); i++) {
                LinearRing hole = geometry.getHole(i);
                double simplificationFactor = (double) maxPoints / ring.length();
                int maxHolePoints = Math.max(4, (int) (simplificationFactor * hole.length()));
                LinearRings holeSimplifier = new LinearRings(maxHolePoints, calculator, this.monitor);
                holeSimplifier.description = "Polygon.Hole";
                holeSimplifiers.add(holeSimplifier);
                holeSimplifier.simplify(hole);
            }
            return produce();
        }

        @Override
        public Polygon produce() {
            return new Polygon(GeometrySimplifier.produceLinearRing(this), produceHoles());
        }

        private List<LinearRing> produceHoles() {
            return holeSimplifiers.stream().map(GeometrySimplifier::produceLinearRing).collect(Collectors.toList());
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
    public static class MultiPolygons extends GeometrySimplifier<MultiPolygon> {
        ArrayList<GeometrySimplifier<Polygon>> polygonSimplifiers = new ArrayList<>();
        ArrayList<Integer> indexes = new ArrayList<>();

        public MultiPolygons(int maxPoints, SimplificationErrorCalculator calculator) {
            this(maxPoints, calculator, null);
        }

        public MultiPolygons(int maxPoints, SimplificationErrorCalculator calculator, Monitor monitor) {
            super("MultiPolygon", maxPoints, calculator, monitor);
        }

        @Override
        public void reset() {
            super.reset();
            polygonSimplifiers.clear();
            indexes.clear();
        }

        @Override
        public MultiPolygon simplify(MultiPolygon geometry) {
            int maxPolyLength = 0;
            for (int i = 0; i < geometry.size(); i++) {
                Polygon polygon = geometry.get(i);
                maxPolyLength = Math.max(maxPolyLength, polygon.getPolygon().length());
            }
            notifyMonitorSimplificationStart();
            for (int i = 0; i < geometry.size(); i++) {
                Polygon polygon = geometry.get(i);
                double simplificationFactor = (double) maxPoints / maxPolyLength;
                int maxPolyPoints = Math.max(4, (int) (simplificationFactor * polygon.getPolygon().length()));
                Polygons simplifier = new Polygons(maxPolyPoints, calculator, monitor);
                simplifier.description = "MultiPolygon.Polygon[" + i + "]";
                simplifier.simplify(polygon);
                if (simplifier.length > 0) {
                    // Invalid polygons (all points co-located) will not be simplified
                    polygonSimplifiers.add(simplifier);
                    indexes.add(i);
                }
            }
            notifyMonitorSimplificationEnd();
            return produce();
        }

        @Override
        public void consume(double x, double y) {
            throw new IllegalArgumentException("MultiPolygon geometry simplifier cannot work in streaming mode");
        }

        @Override
        public MultiPolygon produce() {
            List<Polygon> polygons = polygonSimplifiers.stream().map(GeometrySimplifier::produce).collect(Collectors.toList());
            return new MultiPolygon(polygons);
        }

        /**
         * Provide the index of the original un-simplified polygon given the index of the simplified polygon.
         */
        public int indexOf(int simplified) {
            return indexes.get(simplified);
        }
    }

    private static LinearRing produceLinearRing(GeometrySimplifier<?> simplifier) {
        if (simplifier.length < 1) {
            throw new IllegalArgumentException("No points have been consumed");
        }
        if (simplifier.length < 4) {
            throw new IllegalArgumentException("LinearRing cannot have less than 4 points");
        }
        double[] x = new double[simplifier.length];
        double[] y = new double[simplifier.length];
        for (int i = 0; i < simplifier.length; i++) {
            x[i] = simplifier.points[i].x;
            y[i] = simplifier.points[i].y;
        }
        return new LinearRing(x, y);
    }
}
