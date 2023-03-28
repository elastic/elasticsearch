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
    protected int length;

    protected final PriorityQueue<PointError> queue = new PriorityQueue<>();

    public GeometrySimplifier(int maxPoints, SimplificationErrorCalculator calculator) {
        this.maxPoints = maxPoints;
        this.calculator = calculator;
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
    }

    /**
     * Consume a single point on the stream of points to be simplified
     */
    public void consume(double x, double y) {
        PointError pointError = new PointError(length, x, y);
        if (length > 1) {
            // we need at least three points to calculate the error of the middle point
            points[length - 1].error = calculator.calculateError(points[length - 2], points[length - 1], pointError);
            queue.add(points[length - 1]);
        }
        if (length == maxPoints) {
            // Remove point with lowest error
            PointError toRemove = queue.remove();
            removeAndAdd(toRemove.index, pointError);
        } else {
            this.points[length] = pointError;
            length++;
        }
    }

    private void removeAndAdd(int toRemove, PointError pointError) {
        assert toRemove > 0;  // priority queue can never include first point as that always has zero error by definition
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
     * Produce the simplified geometry from the consumed points
     */
    public abstract T produce();

    public static class PointError implements SimplificationErrorCalculator.PointLike, Comparable<PointError> {
        private int index;
        final double x;
        final double y;
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
        public double getX() {
            return x;
        }

        @Override
        public double getY() {
            return y;
        }
    }

    /**
     * Simplifies a Line geometry to the specified maximum number of points.
     */
    public static class LineStrings extends GeometrySimplifier<Line> {
        public LineStrings(int maxPoints, SimplificationErrorCalculator calculator) {
            super(maxPoints, calculator);
        }

        @Override
        public Line simplify(Line line) {
            if (line.length() <= maxPoints) {
                return line;
            }
            reset();
            for (int i = 0; i < line.length(); i++) {
                consume(line.getX(i), line.getY(i));
            }
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
            super(maxPoints, calculator);
            assert maxPoints >= 4;
        }

        @Override
        public LinearRing simplify(LinearRing ring) {
            if (ring.length() <= maxPoints) {
                return ring;
            }
            reset();
            for (int i = 0; i < ring.length(); i++) {
                consume(ring.getX(i), ring.getY(i));
            }
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
     */
    public static class Polygons extends GeometrySimplifier<Polygon> {
        ArrayList<GeometrySimplifier<LinearRing>> holeSimplifiers = new ArrayList<>();

        public Polygons(int maxPoints, SimplificationErrorCalculator calculator) {
            super(maxPoints, calculator);
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
            for (int i = 0; i < ring.length(); i++) {
                consume(ring.getX(i), ring.getY(i));
            }
            for (int i = 0; i < geometry.getNumberOfHoles(); i++) {
                LinearRing hole = geometry.getHole(i);
                double simplificationFactor = (double) maxPoints / ring.length();
                int maxHolePoints = Math.max(4, (int) (simplificationFactor * hole.length()));
                LinearRings holeSimplifier = new LinearRings(maxHolePoints, calculator);
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
     */
    public static class MultiPolygons extends GeometrySimplifier<MultiPolygon> {
        ArrayList<GeometrySimplifier<Polygon>> polygonSimplifiers = new ArrayList<>();

        public MultiPolygons(int maxPoints, SimplificationErrorCalculator calculator) {
            super(maxPoints, calculator);
        }

        @Override
        public void reset() {
            super.reset();
            polygonSimplifiers.clear();
        }

        @Override
        public MultiPolygon simplify(MultiPolygon geometry) {
            int maxPolyLength = 0;
            for (int i = 0; i < geometry.size(); i++) {
                Polygon polygon = geometry.get(i);
                maxPolyLength = Math.max(maxPolyLength, polygon.getPolygon().length());
            }
            for (int i = 0; i < geometry.size(); i++) {
                Polygon polygon = geometry.get(i);
                double simplificationFactor = (double) maxPoints / maxPolyLength;
                int maxPolyPoints = Math.max(4, (int) (simplificationFactor * polygon.getPolygon().length()));
                Polygons simplifier = new Polygons(maxPolyPoints, calculator);
                simplifier.simplify(polygon);
                if (simplifier.length > 0) {
                    // Invalid polygons (all points co-located) will not be simplified
                    polygonSimplifiers.add(simplifier);
                }
            }
            return produce();
        }

        @Override
        public MultiPolygon produce() {
            int i = 0;
            for (GeometrySimplifier<Polygon> simplifier : polygonSimplifiers) {
                if (simplifier.length == 0) {
                    System.out.println(i + "\t" + simplifier.length);
                }
                i++;
            }
            List<Polygon> polygons = polygonSimplifiers.stream().map(GeometrySimplifier::produce).collect(Collectors.toList());
            return new MultiPolygon(polygons);
        }
    }

    private static LinearRing produceLinearRing(GeometrySimplifier<?> simplifier) {
        if (simplifier.length < 1) {
            throw new IllegalArgumentException("No points have been consumed");
        }
        if (simplifier.length < 4) {
            throw new IllegalArgumentException("Polygon cannot have less than 4 points");
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
