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
import org.elasticsearch.geometry.Polygon;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;

/**
 * The streaming geometry simplifier can only simplify geometries composed on a single list of Points provided on a stream.
 * This includes Line, LinearRing and Polygon (with no holes). To produce such a geometry from a stream, perform the following steps:
 * <ul>
 *     <li>Construct the simplifier with the maximum number of points allowed,
 *     or call <code>reset()</code> on a previously used simplifier</li>
 *     <li>Call <code>consume(x, y)</code> for each incoming point on the stream</li>
 *     <li>Call <code>produce()</code> to generate the final simplified geometry of the desired type</li>
 * </ul>
 * Regardless of the number of times <code>consume(x, y)</code> is called, the internal state will never contain more than
 * the stated maximum number of points (plus one for sorting),
 * and the final geometry will be that size, or smaller (if fewer points were consumed).
 */
public abstract class StreamingGeometrySimplifier<T extends Geometry> {
    protected final int maxPoints;
    protected final SimplificationErrorCalculator calculator;
    protected final PointError[] points;
    protected PointError lastRemoved;
    protected final Monitor monitor;
    protected int length;
    protected int objCount = 0;
    protected String description;

    protected final PriorityQueue<PointError> queue = new PriorityQueue<>();

    protected StreamingGeometrySimplifier(String description, int maxPoints, SimplificationErrorCalculator calculator, Monitor monitor) {
        this.description = description;
        this.maxPoints = maxPoints;
        this.calculator = calculator;
        this.monitor = monitor;
        this.points = new PointError[maxPoints];
        this.length = 0;
    }

    /**
     * Initialize for simplifying using a stream of points. To save on memory it is useful to re-use the same simplifier instance
     * for multiple geometries. Make sure to call reset before each to mark a separation.
     */
    public void reset() {
        this.length = 0;
        this.queue.clear();
    }

    /**
     * Consume a single point on the stream of points to be simplified.
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
        return new ArrayList<>(Arrays.asList(points).subList(0, length));
    }

    /**
     * Simplifies a Line geometry to the specified maximum number of points.
     */
    public static class LineSimplifier extends StreamingGeometrySimplifier<Line> {
        public LineSimplifier(int maxPoints, SimplificationErrorCalculator calculator) {
            this(maxPoints, calculator, null);
        }

        public LineSimplifier(int maxPoints, SimplificationErrorCalculator calculator, Monitor monitor) {
            super("Line", maxPoints, calculator, monitor);
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
     * This behaves the same as the <code>LineSimplifier</code> except that it assumes the first and last point are the same point.
     * The minimum acceptable polygon size is therefor 4 points.
     */
    public static class LinearRingSimplifier extends StreamingGeometrySimplifier<LinearRing> {
        public LinearRingSimplifier(int maxPoints, SimplificationErrorCalculator calculator) {
            this(maxPoints, calculator, null);
        }

        public LinearRingSimplifier(int maxPoints, SimplificationErrorCalculator calculator, Monitor monitor) {
            super("LinearRing", maxPoints, calculator, monitor);
            assert maxPoints >= 4;
        }

        @Override
        public LinearRing produce() {
            return StreamingGeometrySimplifier.produceLinearRing(this);
        }
    }

    /**
     * As with the <code>LinearRingSimplifier</code>, this works similarly to the <code>LineSimplifier</code>
     * except that it assumes the and requires that first and last point are the same point.
     */
    public static class PolygonSimplifier extends StreamingGeometrySimplifier<Polygon> {
        public PolygonSimplifier(int maxPoints, SimplificationErrorCalculator calculator) {
            this(maxPoints, calculator, null);
        }

        public PolygonSimplifier(int maxPoints, SimplificationErrorCalculator calculator, Monitor monitor) {
            super("Polygon", maxPoints, calculator, monitor);
        }

        @Override
        public Polygon produce() {
            return new Polygon(StreamingGeometrySimplifier.produceLinearRing(this));
        }
    }

    static LinearRing produceLinearRing(StreamingGeometrySimplifier<?> simplifier) {
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
        if (x[simplifier.length - 1] != x[0] || y[simplifier.length - 1] != y[0]) {
            String inequality = "(" + x[0] + " " + y[0] + ") != (" + x[simplifier.length - 1] + " " + y[simplifier.length - 1] + ")";
            throw new IllegalArgumentException("LinearRing cannot have first and last points differ: " + inequality);
        }
        return new LinearRing(x, y);
    }
}
