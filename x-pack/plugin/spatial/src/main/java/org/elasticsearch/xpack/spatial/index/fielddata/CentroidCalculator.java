/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.GeometryVisitor;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiLine;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.search.aggregations.metrics.CompensatedSum;

/**
 * This class keeps a running Kahan-sum of coordinates
 * that are to be averaged in {@code TriangleTreeWriter} for use
 * as the centroid of a shape.
 */
public class CentroidCalculator {

    private final CentroidCalculatorVisitor visitor;

    public CentroidCalculator() {
        this.visitor = new CentroidCalculatorVisitor();
    }

    /**
     * Add a geometry to the calculator
     *
     * @param geometry the geometry to add
     */
    public void add(Geometry geometry) {
        geometry.visit(visitor);
    }

    /**
     * @return the x-coordinate centroid
     */
    public double getX() {
        return visitor.compSumX.value() / visitor.compSumWeight.value();
    }

    /**
     * @return the y-coordinate centroid
     */
    public double getY() {
        return visitor.compSumY.value() / visitor.compSumWeight.value();
    }

    /**
     * @return the sum of all the weighted coordinates summed in the calculator
     */
    public double sumWeight() {
        return visitor.compSumWeight.value();
    }

    /**
     * @return the highest dimensional shape type summed in the calculator
     */
    public DimensionalShapeType getDimensionalShapeType() {
        return visitor.dimensionalShapeType;
    }

    private static class CentroidCalculatorVisitor implements GeometryVisitor<Void, IllegalArgumentException> {

        final CompensatedSum compSumX;
        final CompensatedSum compSumY;
        final CompensatedSum compSumWeight;
        DimensionalShapeType dimensionalShapeType;

        private CentroidCalculatorVisitor() {
            this.compSumX = new CompensatedSum(0, 0);
            this.compSumY = new CompensatedSum(0, 0);
            this.compSumWeight = new CompensatedSum(0, 0);
            this.dimensionalShapeType = DimensionalShapeType.POINT;
        }

        @Override
        public Void visit(Circle circle) {
            throw new IllegalArgumentException("invalid shape type found [Circle] while calculating centroid");
        }

        @Override
        public Void visit(GeometryCollection<?> collection) {
            for (Geometry shape : collection) {
                shape.visit(this);
            }
            return null;
        }

        @Override
        public Void visit(Line line) {
            if (dimensionalShapeType != DimensionalShapeType.POLYGON) {
                visitLine(line.length(), line::getX, line::getY);
            }
            return null;
        }

        @Override
        public Void visit(LinearRing ring) {
            throw new IllegalArgumentException("invalid shape type found [LinearRing] while calculating centroid");
        }

        @Override
        public Void visit(MultiLine multiLine) {
            if (dimensionalShapeType != DimensionalShapeType.POLYGON) {
                for (Line line : multiLine) {
                    visit(line);
                }
            }
            return null;
        }

        @Override
        public Void visit(MultiPoint multiPoint) {
            if (dimensionalShapeType == DimensionalShapeType.POINT) {
                for (Point point : multiPoint) {
                    visit(point);
                }
            }
            return null;
        }

        @Override
        public Void visit(MultiPolygon multiPolygon) {
            for (Polygon polygon : multiPolygon) {
                visit(polygon);
            }
            return null;
        }

        @Override
        public Void visit(Point point) {
            if (dimensionalShapeType == DimensionalShapeType.POINT) {
                visitPoint(point.getX(), point.getY());
            }
            return null;
        }

        @Override
        public Void visit(Polygon polygon) {
            // check area of polygon

            double[] centroidX = new double[1 + polygon.getNumberOfHoles()];
            double[] centroidY = new double[1 + polygon.getNumberOfHoles()];
            double[] weight = new double[1 + polygon.getNumberOfHoles()];
            visitLinearRing(
                polygon.getPolygon().length(),
                polygon.getPolygon()::getX,
                polygon.getPolygon()::getY,
                false,
                centroidX,
                centroidY,
                weight,
                0
            );
            for (int i = 0; i < polygon.getNumberOfHoles(); i++) {
                visitLinearRing(
                    polygon.getHole(i).length(),
                    polygon.getHole(i)::getX,
                    polygon.getHole(i)::getY,
                    true,
                    centroidX,
                    centroidY,
                    weight,
                    i + 1
                );
            }

            double sumWeight = 0;
            for (double w : weight) {
                sumWeight += w;
            }

            if (sumWeight == 0 && dimensionalShapeType != DimensionalShapeType.POLYGON) {
                visitLine(polygon.getPolygon().length(), polygon.getPolygon()::getX, polygon.getPolygon()::getY);
            } else {
                for (int i = 0; i < 1 + polygon.getNumberOfHoles(); i++) {
                    addCoordinate(centroidX[i], centroidY[i], weight[i], DimensionalShapeType.POLYGON);
                }
            }

            return null;
        }

        @Override
        public Void visit(Rectangle rectangle) {
            double diffX = rectangle.getMaxX() - rectangle.getMinX();
            double diffY = rectangle.getMaxY() - rectangle.getMinY();
            double rectWeight = Math.abs(diffX * diffY);
            if (rectWeight != 0) {
                double sumX = rectangle.getMaxX() + rectangle.getMinX();
                double sumY = rectangle.getMaxY() + rectangle.getMinY();
                addCoordinate(sumX / 2, sumY / 2, rectWeight, DimensionalShapeType.POLYGON);
            } else {
                // degenerated rectangle, transform to Line
                Line line = new Line(
                    new double[] { rectangle.getMinX(), rectangle.getMaxX() },
                    new double[] { rectangle.getMinY(), rectangle.getMaxY() }
                );
                visit(line);
            }
            return null;
        }

        private void visitPoint(double x, double y) {
            addCoordinate(x, y, 1.0, DimensionalShapeType.POINT);
        }

        private void visitLine(int length, CoordinateSupplier x, CoordinateSupplier y) {
            // a line's centroid is calculated by summing the center of each
            // line segment weighted by the line segment's length in degrees
            for (int i = 0; i < length - 1; i++) {
                double diffX = x.get(i) - x.get(i + 1);
                double diffY = y.get(i) - y.get(i + 1);
                double xAvg = (x.get(i) + x.get(i + 1)) / 2;
                double yAvg = (y.get(i) + y.get(i + 1)) / 2;
                double weight = Math.sqrt(diffX * diffX + diffY * diffY);
                if (weight == 0) {
                    // degenerated line, it can be considered a point
                    visitPoint(x.get(i), y.get(i));
                } else {
                    addCoordinate(xAvg, yAvg, weight, DimensionalShapeType.LINE);
                }
            }
        }

        private void visitLinearRing(
            int length,
            CoordinateSupplier x,
            CoordinateSupplier y,
            boolean isHole,
            double[] centroidX,
            double[] centroidY,
            double[] weight,
            int idx
        ) {
            // implementation of calculation defined in
            // https://www.seas.upenn.edu/~sys502/extra_materials/Polygon%20Area%20and%20Centroid.pdf
            //
            // centroid of a ring is a weighted coordinate based on the ring's area.
            // the sign of the area is positive for the outer-shell of a polygon and negative for the holes

            int sign = isHole ? -1 : 1;
            double totalRingArea = 0.0;
            for (int i = 0; i < length - 1; i++) {
                totalRingArea += (x.get(i) * y.get(i + 1)) - (x.get(i + 1) * y.get(i));
            }
            totalRingArea = totalRingArea / 2;

            double sumX = 0.0;
            double sumY = 0.0;
            for (int i = 0; i < length - 1; i++) {
                double twiceArea = (x.get(i) * y.get(i + 1)) - (x.get(i + 1) * y.get(i));
                sumX += twiceArea * (x.get(i) + x.get(i + 1));
                sumY += twiceArea * (y.get(i) + y.get(i + 1));
            }
            centroidX[idx] = sumX / (6 * totalRingArea);
            centroidY[idx] = sumY / (6 * totalRingArea);
            weight[idx] = sign * Math.abs(totalRingArea);
        }

        /**
         * adds a single coordinate to the running sum and count of coordinates
         * for centroid calculation
         *  @param x the x-coordinate of the point
         * @param y the y-coordinate of the point
         * @param weight the associated weight of the coordinate
         * @param shapeType the associated shape type of the coordinate
         */
        private void addCoordinate(double x, double y, double weight, DimensionalShapeType shapeType) {
            // x and y can be infinite due to really small areas and rounding problems
            if (Double.isFinite(x) && Double.isFinite(y)) {
                if (this.dimensionalShapeType == shapeType) {
                    compSumX.add(x * weight);
                    compSumY.add(y * weight);
                    compSumWeight.add(weight);
                    this.dimensionalShapeType = shapeType;
                } else if (shapeType.compareTo(this.dimensionalShapeType) > 0) {
                    // reset counters
                    compSumX.reset(x * weight, 0);
                    compSumY.reset(y * weight, 0);
                    compSumWeight.reset(weight, 0);
                    this.dimensionalShapeType = shapeType;
                }
            }
        }
    }

    @FunctionalInterface
    private interface CoordinateSupplier {
        double get(int idx);
    }
}
