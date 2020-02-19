/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.geo;

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

/**
 * This class keeps a running Kahan-sum of coordinates
 * that are to be averaged in {@link TriangleTreeWriter} for use
 * as the centroid of a shape.
 */
public class CentroidCalculator {
    private double compX;
    private double compY;
    private double sumX;
    private double sumY;
    private double sumWeight;
    private DimensionalShapeType dimensionalShapeType;

    public CentroidCalculator(Geometry geometry) {
        this.sumX = 0.0;
        this.compX = 0.0;
        this.sumY = 0.0;
        this.compY = 0.0;
        this.sumWeight = 0.0;
        CentroidCalculatorVisitor visitor = new CentroidCalculatorVisitor(this);
        geometry.visit(visitor);
        this.dimensionalShapeType = DimensionalShapeType.forGeometry(geometry);
    }

    /**
     * adds a single coordinate to the running sum and count of coordinates
     * for centroid calculation
     *  @param x the x-coordinate of the point
     * @param y the y-coordinate of the point
     * @param weight the associated weight of the coordinate
     */
    private void addCoordinate(double x, double y, double weight) {
        double correctedX = weight * x - compX;
        double newSumX = sumX + correctedX;
        compX = (newSumX - sumX) - correctedX;
        sumX = newSumX;

        double correctedY = weight * y - compY;
        double newSumY = sumY + correctedY;
        compY = (newSumY - sumY) - correctedY;
        sumY = newSumY;

        sumWeight += weight;
    }

    /**
     * Adjusts the existing calculator to add the running sum and count
     * from another {@link CentroidCalculator}. This is used to keep
     * a running count of points from different sub-shapes of a single
     * geo-shape field
     *
     * @param otherCalculator the other centroid calculator to add from
     */
    public void addFrom(CentroidCalculator otherCalculator) {
        int compared = DimensionalShapeType.COMPARATOR.compare(dimensionalShapeType, otherCalculator.dimensionalShapeType);
        if (compared < 0) {
            sumWeight = otherCalculator.sumWeight;
            dimensionalShapeType = otherCalculator.dimensionalShapeType;
            sumX = otherCalculator.sumX;
            sumY = otherCalculator.sumY;
            compX = otherCalculator.compX;
            compY = otherCalculator.compY;
        } else if (compared == 0) {
            addCoordinate(otherCalculator.sumX, otherCalculator.sumY, otherCalculator.sumWeight);
        } // else (compared > 0) do not modify centroid calculation since otherCalculator is of lower dimension than this calculator
    }

    /**
     * @return the x-coordinate centroid
     */
    public double getX() {
        // normalization required due to floating point precision errors
        return GeoUtils.normalizeLon(sumX / sumWeight);
    }

    /**
     * @return the y-coordinate centroid
     */
    public double getY() {
        // normalization required due to floating point precision errors
        return GeoUtils.normalizeLat(sumY / sumWeight);
    }

    /**
     * @return the sum of all the weighted coordinates summed in the calculator
     */
    public double sumWeight() {
        return sumWeight;
    }

    /**
     * @return the highest dimensional shape type summed in the calculator
     */
    public DimensionalShapeType getDimensionalShapeType() {
        return dimensionalShapeType;
    }

    private static class CentroidCalculatorVisitor implements GeometryVisitor<Void, IllegalArgumentException> {

        private final CentroidCalculator calculator;

        private CentroidCalculatorVisitor(CentroidCalculator calculator) {
            this.calculator = calculator;
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
            // a line's centroid is calculated by summing the center of each
            // line segment weighted by the line segment's length in degrees
            for (int i = 0; i < line.length() - 1; i++) {
                double diffX = line.getX(i) - line.getX(i + 1);
                double diffY = line.getY(i) - line.getY(i + 1);
                double x = (line.getX(i) + line.getX(i + 1)) / 2;
                double y = (line.getY(i) + line.getY(i + 1)) / 2;
                calculator.addCoordinate(x, y, Math.sqrt(diffX * diffX + diffY * diffY));
            }
            return null;
        }
        @Override
        public Void visit(LinearRing ring) {
            throw new IllegalArgumentException("invalid shape type found [LinearRing] while calculating centroid");
        }

        private Void visit(LinearRing ring, boolean isHole) {
            // implementation of calculation defined in
            // https://www.seas.upenn.edu/~sys502/extra_materials/Polygon%20Area%20and%20Centroid.pdf
            //
            // centroid of a ring is a weighted coordinate based on the ring's area.
            // the sign of the area is positive for the outer-shell of a polygon and negative for the holes

            int sign = isHole ? -1 : 1;
            double totalRingArea = 0.0;
            for (int i = 0; i < ring.length() - 1; i++) {
                totalRingArea += (ring.getX(i) * ring.getY(i + 1)) - (ring.getX(i + 1) * ring.getY(i));
            }
            totalRingArea = totalRingArea / 2;

            double sumX = 0.0;
            double sumY = 0.0;
            for (int i = 0; i < ring.length() - 1; i++) {
                double twiceArea = (ring.getX(i) * ring.getY(i + 1)) - (ring.getX(i + 1) * ring.getY(i));
                sumX += twiceArea * (ring.getX(i) + ring.getX(i + 1));
                sumY += twiceArea * (ring.getY(i) + ring.getY(i + 1));
            }
            double cX = sumX / (6 * totalRingArea);
            double cY = sumY / (6 * totalRingArea);
            calculator.addCoordinate(cX, cY, sign * Math.abs(totalRingArea));

            return null;
        }

        @Override
        public Void visit(MultiLine multiLine) {
            for (Line line : multiLine) {
                visit(line);
            }
            return null;
        }

        @Override
        public Void visit(MultiPoint multiPoint) {
            for (Point point : multiPoint) {
                visit(point);
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
            calculator.addCoordinate(point.getX(), point.getY(), 1.0);
            return null;
        }

        @Override
        public Void visit(Polygon polygon) {
            visit(polygon.getPolygon(), false);
            for (int i = 0; i < polygon.getNumberOfHoles(); i++) {
                visit(polygon.getHole(i), true);
            }
            return null;
        }

        @Override
        public Void visit(Rectangle rectangle) {
            double sumX = rectangle.getMaxX() + rectangle.getMinX();
            double sumY = rectangle.getMaxY() + rectangle.getMinY();
            double diffX = rectangle.getMaxX() - rectangle.getMinX();
            double diffY = rectangle.getMaxY() - rectangle.getMinY();
            calculator.addCoordinate(sumX / 2, sumY / 2, Math.abs(diffX * diffY));
            return null;
        }
    }

}
