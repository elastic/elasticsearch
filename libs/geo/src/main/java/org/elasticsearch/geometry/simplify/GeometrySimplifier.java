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
import org.elasticsearch.geometry.Point;

import java.util.PriorityQueue;

public abstract class GeometrySimplifier<T extends Geometry> {
    /**
     * Simplify an entire geometry in a non-streaming fashion
     */
    public abstract T simplify(T geometry);

    /**
     * Initialize for simplifying using a stream of points
     */
    public abstract void reset();

    /**
     * Consume a single point on the stream of points to be simplified
     */
    public abstract void consume(double x, double y);

    /**
     * Produce the simplified geometry from the consumed points
     */
    public abstract T produce();

    public static class Points extends GeometrySimplifier<Point> {
        private Point current;

        @Override
        public Point simplify(Point geometry) {
            return geometry;
        }

        @Override
        public void reset() {
            this.current = null;
        }

        @Override
        public void consume(double x, double y) {
            if (current != null) {
                throw new IllegalArgumentException("Already consumed a point: " + current);
            }
            current = new Point(x, y);
        }

        @Override
        public Point produce() {
            if (current == null) {
                throw new IllegalArgumentException("No point has been consumed");
            }
            return current;
        }
    }

    public static class PointError implements Comparable<PointError> {
        private int index;
        private final double x;
        private final double y;
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
    }

    public static class LineStrings extends GeometrySimplifier<Line> {
        private final int maxPoints;
        private final PointError[] points;
        private int length;

        private final PriorityQueue<PointError> queue = new PriorityQueue<>();

        public LineStrings(int maxPoints) {
            this.maxPoints = maxPoints;
            this.points = new PointError[maxPoints];
            this.length = 0;
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
        public void reset() {
            this.length = 0;
        }

        @Override
        public void consume(double x, double y) {
            PointError pointError = new PointError(length, x, y);
            if (length > 1) {
                // we need at least three points to calculate the error of the middle point
                points[length - 1].error = calculateError(points[length - 2], points[length - 1], pointError);
                queue.add(points[length - 1]);
            }
            if (length == maxPoints) {
                // Remove point with lowest error
                PointError toRemove = queue.remove();
                removeAndAdd(toRemove.index, pointError);
                queue.add(pointError);
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
                double error = calculateError(points[index - 1], points[index], points[index + 1]);
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

        private double calculateError(PointError left, PointError middle, PointError right) {
            // Offset coordinates so left is at the origin
            double leftX = 0;
            double leftY = 0;
            double middleX = middle.x - left.x;
            double middleY = middle.y - left.y;
            double rightX = right.x - left.x;
            double rightY = right.y - left.y;
            // Rotate coordinates so that a->c is horizontal
            double len = Math.sqrt(rightX * rightX + rightY * rightY);
            double cos = rightX / len;
            double sin = rightY / len;
            double middleXrotated = middleX * cos + middleY * sin;
            double middleYrotated = middleY * cos - middleX * sin;
            double rightXrotated = rightX * cos + rightY * sin;
            double rightYrotated = rightY * cos - rightX * sin;
            assert Math.abs(rightYrotated) < 1e-10;
            assert Math.abs(rightXrotated - len) < len / 1e10;
            // Return distance to x-axis TODO: also include back-paths for Frechet distance calculation
            return Math.abs(middleYrotated);
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
}
