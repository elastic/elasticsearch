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
import org.elasticsearch.geometry.ShapeType;

import java.util.Comparator;

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
    private int count;
    private DimensionalShapeType dimensionalShapeType;

    public CentroidCalculator(Geometry geometry) {
        this.sumX = 0.0;
        this.compX = 0.0;
        this.sumY = 0.0;
        this.compY = 0.0;
        this.count = 0;
        CentroidCalculatorVisitor visitor = new CentroidCalculatorVisitor(this);
        geometry.visit(visitor);
        if (ShapeType.GEOMETRYCOLLECTION.equals(geometry.type())) {
            switch (visitor.dimensionalShapeType) {
                case POINT:
                case MULTIPOINT:
                    dimensionalShapeType = DimensionalShapeType.GEOMETRYCOLLECTION_POINTS;
                    break;
                case LINESTRING:
                case MULTILINESTRING:
                    dimensionalShapeType = DimensionalShapeType.GEOMETRYCOLLECTION_LINES;
                    break;
                case POLYGON:
                case MULTIPOLYGON:
                case CIRCLE:
                case ENVELOPE:
                case LINEARRING:
                    dimensionalShapeType = DimensionalShapeType.GEOMETRYCOLLECTION_POLYGONS;
                    break;
                default:
                    throw new IllegalStateException("unexpected DimensionalShapeType [" + visitor.dimensionalShapeType + "]");
            }
        } else {
            dimensionalShapeType = visitor.dimensionalShapeType;
        }
    }

    /**
     * adds a single coordinate to the running sum and count of coordinates
     * for centroid calculation
     *
     * @param x the x-coordinate of the point
     * @param y the y-coordinate of the point
     */
    private void addCoordinate(double x, double y) {
        double correctedX = x - compX;
        double newSumX = sumX + correctedX;
        compX = (newSumX - sumX) - correctedX;
        sumX = newSumX;

        double correctedY = y - compY;
        double newSumY = sumY + correctedY;
        compY = (newSumY - sumY) - correctedY;
        sumY = newSumY;

        count += 1;
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
        addCoordinate(otherCalculator.sumX, otherCalculator.sumY);
        // adjust count
        count += otherCalculator.count - 1;
        dimensionalShapeType = DimensionalShapeType.max(dimensionalShapeType, otherCalculator.dimensionalShapeType);
    }

    /**
     * @return the x-coordinate centroid
     */
    public double getX() {
        return sumX / count;
    }

    /**
     * @return the y-coordinate centroid
     */
    public double getY() {
        return sumY / count;
    }

    public DimensionalShapeType getDimensionalShapeType() {
        return dimensionalShapeType;
    }

    private static class CentroidCalculatorVisitor implements GeometryVisitor<Void, IllegalArgumentException> {

        private final CentroidCalculator calculator;
        private DimensionalShapeType dimensionalShapeType;
        private boolean isGeometryCollection;

        private CentroidCalculatorVisitor(CentroidCalculator calculator) {
            this.calculator = calculator;
            this.isGeometryCollection = false;
        }

        @Override
        public Void visit(Circle circle) {
            calculator.addCoordinate(circle.getX(), circle.getY());
            dimensionalShapeType = DimensionalShapeType.max(dimensionalShapeType, DimensionalShapeType.CIRCLE);
            return null;
        }

        @Override
        public Void visit(GeometryCollection<?> collection) {
            isGeometryCollection = true;
            for (Geometry shape : collection) {
                shape.visit(this);
            }
            return null;
        }

        @Override
        public Void visit(Line line) {

            for (int i = 0; i < line.length(); i++) {
                calculator.addCoordinate(line.getX(i), line.getY(i));
            }
            dimensionalShapeType = DimensionalShapeType.max(dimensionalShapeType, DimensionalShapeType.LINESTRING);
            return null;
        }

        @Override
        public Void visit(LinearRing ring) {
            for (int i = 0; i < ring.length() - 1; i++) {
                calculator.addCoordinate(ring.getX(i), ring.getY(i));
            }
            dimensionalShapeType = DimensionalShapeType.max(dimensionalShapeType, DimensionalShapeType.LINEARRING);
            return null;
        }

        @Override
        public Void visit(MultiLine multiLine) {
            dimensionalShapeType = DimensionalShapeType.max(dimensionalShapeType, DimensionalShapeType.MULTILINESTRING);
            for (Line line : multiLine) {
                visit(line);
            }
            return null;
        }

        @Override
        public Void visit(MultiPoint multiPoint) {
            dimensionalShapeType = DimensionalShapeType.max(dimensionalShapeType, DimensionalShapeType.MULTIPOINT);
            for (Point point : multiPoint) {
                visit(point);
            }
            return null;
        }

        @Override
        public Void visit(MultiPolygon multiPolygon) {
            dimensionalShapeType = DimensionalShapeType.max(dimensionalShapeType, DimensionalShapeType.MULTIPOLYGON);
            for (Polygon polygon : multiPolygon) {
                visit(polygon);
            }
            return null;
        }

        @Override
        public Void visit(Point point) {
            calculator.addCoordinate(point.getX(), point.getY());
            dimensionalShapeType = DimensionalShapeType.max(dimensionalShapeType, DimensionalShapeType.POINT);
            return null;
        }

        @Override
        public Void visit(Polygon polygon) {
            // TODO: incorporate holes into centroid calculation
            dimensionalShapeType = DimensionalShapeType.max(dimensionalShapeType, DimensionalShapeType.POLYGON);
            return visit(polygon.getPolygon());
        }

        @Override
        public Void visit(Rectangle rectangle) {
            calculator.addCoordinate(rectangle.getMinX(), rectangle.getMinY());
            calculator.addCoordinate(rectangle.getMinX(), rectangle.getMaxY());
            calculator.addCoordinate(rectangle.getMaxX(), rectangle.getMinY());
            calculator.addCoordinate(rectangle.getMaxX(), rectangle.getMaxY());
            dimensionalShapeType = DimensionalShapeType.max(dimensionalShapeType, DimensionalShapeType.ENVELOPE);
            return null;
        }
    }

    public enum DimensionalShapeType {
        POINT,
        MULTIPOINT,
        LINESTRING,
        MULTILINESTRING,
        POLYGON,
        MULTIPOLYGON,
        GEOMETRYCOLLECTION_POINTS,    // highest-dimensional shapes are Points
        GEOMETRYCOLLECTION_LINES,     // highest-dimensional shapes are Lines
        GEOMETRYCOLLECTION_POLYGONS,  // highest-dimensional shapes are Polygons
        LINEARRING, // not serialized by itself in WKT or WKB
        ENVELOPE, // not part of the actual WKB spec
        CIRCLE; // not part of the actual WKB spec

        private static DimensionalShapeType[] values = values();

        public static Comparator<DimensionalShapeType> COMPARATOR = Comparator.comparingInt(DimensionalShapeType::centroidDimension);

        public static DimensionalShapeType max(DimensionalShapeType s1, DimensionalShapeType s2) {
            if (s1 == null) {
                return s2;
            } else if (s2 == null) {
                return s1;
            }
            return COMPARATOR.compare(s1, s2) >= 0 ? s1 : s2;
        }

        public static DimensionalShapeType forOrdinal(int ordinal) {
            return values[ordinal];
        }

        public int centroidDimension() {
            switch (this) {
                case POINT:
                case MULTIPOINT:
                case GEOMETRYCOLLECTION_POINTS:
                    return 0;
                case LINESTRING:
                case MULTILINESTRING:
                case GEOMETRYCOLLECTION_LINES:
                    return 1;
                case POLYGON:
                case MULTIPOLYGON:
                case GEOMETRYCOLLECTION_POLYGONS:
                case LINEARRING:
                case ENVELOPE:
                case CIRCLE:
                    return 2;
                default:
                    throw new IllegalStateException("dimension calculation of DimensionalShapeType [" + this + "] is not supported");
            }
        }
    }
}
