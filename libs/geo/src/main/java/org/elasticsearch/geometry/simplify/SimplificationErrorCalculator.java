/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.geometry.simplify;

import java.util.Locale;

import static java.lang.Math.acos;
import static java.lang.Math.min;
import static org.elasticsearch.geometry.simplify.SimplificationErrorCalculator.Point3D.sin;
import static org.elasticsearch.geometry.simplify.SloppyMath.cos;

public interface SimplificationErrorCalculator {
    double calculateError(PointLike left, PointLike middle, PointLike right);

    interface PointLike {
        double x();

        double y();
    }

    SimplificationErrorCalculator CARTESIAN_TRIANGLE_AREA = new CartesianTriangleAreaCalculator();
    SimplificationErrorCalculator TRIANGLE_AREA = new TriangleAreaCalculator();
    SimplificationErrorCalculator TRIANGLE_HEIGHT = new TriangleHeightCalculator();
    SimplificationErrorCalculator HEIGHT_AND_BACKPATH_DISTANCE = new CartesianHeightAndBackpathDistanceCalculator();
    SimplificationErrorCalculator SPHERICAL_HEIGHT_AND_BACKPATH_DISTANCE = new SphericalHeightAndBackpathDistanceCalculator();

    static SimplificationErrorCalculator byName(String calculatorName) {
        return switch (calculatorName.toLowerCase(Locale.ROOT)) {
            case "cartesiantrianglearea" -> CARTESIAN_TRIANGLE_AREA;
            case "trianglearea" -> TRIANGLE_AREA;
            case "triangleheight" -> TRIANGLE_HEIGHT;
            case "heightandbackpathdistance" -> HEIGHT_AND_BACKPATH_DISTANCE;
            case "sphericalheightandbackpathdistance" -> SPHERICAL_HEIGHT_AND_BACKPATH_DISTANCE;
            default -> throw new IllegalArgumentException("Unknown simplification error calculator: " + calculatorName);
        };
    }

    /**
     * Calculate the triangle area using cartesian coordinates as described at
     * <a href="https://en.wikipedia.org/wiki/Area_of_a_triangle">Area of a triangle</a>
     */
    class CartesianTriangleAreaCalculator implements SimplificationErrorCalculator {

        @Override
        public double calculateError(PointLike left, PointLike middle, PointLike right) {
            double xb = middle.x() - left.x();
            double yb = middle.y() - left.y();
            double xc = right.x() - left.x();
            double yc = right.y() - left.y();
            return 0.5 * Math.abs(xb * yc - xc * yb);
        }
    }

    /**
     * Calculate the triangle area using geographic coordinates and Herons formula (side lengths) as described at
     * <a href="https://en.wikipedia.org/wiki/Area_of_a_triangle">Area of a triangle</a>
     */
    class TriangleAreaCalculator implements SimplificationErrorCalculator {

        @Override
        public double calculateError(PointLike left, PointLike middle, PointLike right) {
            // Calculate side lengths using approximate haversine
            double a = distance(left, right);
            double b = distance(right, middle);
            double c = distance(middle, left);
            // semi-perimeter
            double s = 0.5 * (a + b + c);  // Semi-perimeter
            double da = s - a;
            double db = s - b;
            double dc = s - c;
            if (da >= 0 && db >= 0 && dc >= 0) {
                // Herons formula
                return Math.sqrt(s * da * db * dc);
            } else {
                // rounding errors can cause flat triangles to have negative values, leading to NaN areas
                return 0.0;
            }
        }

        private static double distance(PointLike a, PointLike b) {
            return SloppyMath.haversinMeters(a.y(), a.x(), b.y(), b.x());
        }
    }

    /**
     * Calculate the triangle area using geographic coordinates and Herons formula (side lengths) as described at
     * <a href="https://en.wikipedia.org/wiki/Area_of_a_triangle">Area of a triangle</a>, but scale the area down
     * by the inverse of the length of the base (left-right), which estimates the height of the triangle.
     */
    class TriangleHeightCalculator implements SimplificationErrorCalculator {

        @Override
        public double calculateError(PointLike left, PointLike middle, PointLike right) {
            // Calculate side lengths using approximate haversine
            double a = distance(left, right);
            double b = distance(right, middle);
            double c = distance(middle, left);
            // semi-perimeter
            double s = 0.5 * (a + b + c);  // Semi-perimeter
            double da = s - a;
            double db = s - b;
            double dc = s - c;
            if (da >= 0 && db >= 0 && dc >= 0) {
                // Herons formula, scaled by 2/a to estimate height
                return 2.0 * Math.sqrt(s * da * db * dc) / a;
            } else {
                // rounding errors can cause flat triangles to have negative values, leading to NaN areas
                return 0.0;
            }
        }

        private static double distance(PointLike a, PointLike b) {
            return SloppyMath.haversinMeters(a.y(), a.x(), b.y(), b.x());
        }
    }

    /**
     * Estimate the error as the height of the point above the base, but including support for back-paths
     * in the sense that of the point to be removed is father from either end than the height, we take that distance instead.
     * <p>
     * Rotate all three points such that left-right are a horizontal line on the x-axis. Then the numbers of interest are:
     * <ol>
     *     <li>height: y-value of middle point</li>
     *     <li>deltaL: -min(0, middleX - leftX)</li>
     *     <li>deltaR: max(0, middleX - rightX)</li>
     * </ol>
     * And the final error is: error = max(height, max(deltaL, deltaR))
     * <p>
     * This is not a full Frechet error calculation as it does not maintain state of all removed points,
     * only calculating the error incurred by removal of the current point, as if the current simplified line is
     * a good enough approximation of the original line. This restriction is currently true of all the
     * calculations implemented so far.
     */
    class CartesianHeightAndBackpathDistanceCalculator implements SimplificationErrorCalculator {

        @Override
        public double calculateError(PointLike left, PointLike middle, PointLike right) {
            // Offset coordinates so left is at the origin
            double rightX = right.x() - left.x();
            double rightY = right.y() - left.y();
            if (Math.abs(rightX) > 1e-10 || Math.abs(rightY) > 1e-10) {
                // Rotate coordinates so that left->right is horizontal
                double len = Math.sqrt(rightX * rightX + rightY * rightY);
                double cos = rightX / len;
                double sin = rightY / len;
                double middleX = middle.x() - left.x();
                double middleY = middle.y() - left.y();
                double middleXrotated = middleX * cos + middleY * sin;
                double middleYrotated = middleY * cos - middleX * sin;
                double rightXrotated = rightX * cos + rightY * sin;
                double rightYrotated = rightY * cos - rightX * sin;
                assert Math.abs(rightYrotated) < 1e-10;
                assert Math.abs(rightXrotated - len) < len / 1e10;
                double height = Math.abs(middleYrotated);
                double deltaL = -min(0, middleXrotated);
                double deltaR = Math.max(0, middleXrotated - rightXrotated);
                double backDistance = Math.max(deltaR, deltaL);
                return Math.max(height, backDistance);
            } else {
                // If left and right points are co-located, we assume no consequence to removing the middle point
                return 0.0;
            }
        }
    }

    /**
     * Estimate the error as the height of the point above the base, but including support for back-paths
     * in the sense that of the point to be removed is father from either end than the height, we take that distance instead.
     * <p>
     * Rotate all three points such that left-right are a horizontal line on the x-axis. Then the numbers of interest are:
     * <ol>
     *     <li>height: y-value of middle point</li>
     *     <li>deltaL: -min(0, middleX - leftX)</li>
     *     <li>deltaR: max(0, middleX - rightX)</li>
     * </ol>
     * And the final error is: error = max(height, max(deltaL, deltaR))
     * <p>
     * This is not a full Frechet error calculation as it does not maintain state of all removed points,
     * only calculating the error incurred by removal of the current point, as if the current simplified line is
     * a good enough approximation of the original line. This restriction is currently true of all the
     * calculations implemented so far.
     */
    class SphericalHeightAndBackpathDistanceCalculator implements SimplificationErrorCalculator {
        @Override
        public double calculateError(PointLike leftLL, PointLike middleLL, PointLike rightLL) {
            // Convert to 3D
            Point3D left = Point3D.from(leftLL);
            Point3D middle = Point3D.from(middleLL);
            Point3D right = Point3D.from(rightLL);

            // Rotate to horizontal
            Point3D rotationAxis = right.cross(left);
            Point3D xAxis = new Point3D(1, 0, 0);
            double rotationAngle = xAxis.angleTo(left);
            left = left.rotate(rotationAxis, rotationAngle);
            middle = middle.rotate(rotationAxis, rotationAngle);
            right = right.rotate(rotationAxis, rotationAngle);
            double len = left.angleTo(right);
            if (len > 1e-10) {
                double height = Math.abs(middle.z);
                double deltaL = -min(0, middle.y);
                double deltaR = Math.max(0, middle.y - right.y);
                double backDistance = Math.max(deltaR, deltaL);
                return Math.max(height, backDistance);
            } else {
                // If left and right points are co-located, we assume no consequence to removing the middle point
                return 0.0;
            }
        }
    }

    /** This record captures a point defined using a 3D vector (x, y, z) */
    record Point3D(double x, double y, double z) {
        /** Convert a geographic point from latitude/longitude to x,y,z of unit length */
        public static Point3D from(double latDegrees, double lonDegrees) {
            double lat = Math.toRadians(latDegrees);
            double lon = Math.toRadians(lonDegrees);
            double x = cos(lat) * cos(lon);
            double y = cos(lat) * sin(lon);
            double z = sin(lat);
            return new Point3D(x, y, z);
        }

        /** Convert a geographic point from latitude/longitude to x,y,z */
        public static Point3D from(PointLike point) {
            return from(point.y(), point.x());
        }

        /** Vector cross-product of this vector against the provided vector */
        public Point3D cross(Point3D o) {
            return new Point3D(y * o.z - z * o.y, z * o.x - x * o.z, x * o.y - y * o.x);
        }

        static double sin(double radians) {
            return cos(radians - Math.PI / 2);
        }

        /** Calculate the length of this vector, should be 1 for unit vectors */
        public double length() {
            return Math.sqrt(x * x + y * y + z * z);
        }

        /** Invert this vector, providing a parallel vector pointing in the opposite direction */
        public Point3D inverse() {
            return new Point3D(-x, -y, -z);
        }

        /** Calculate the angle in radians between this vector and the specified vector */
        public double angleTo(Point3D other) {
            double a = this.dot(other) / (this.length() * other.length());
            return acos(min(1.0, a));
        }

        /** Calculate the dot-product of this vector with the specified vector */
        public double dot(Point3D o) {
            return x * o.x + y * o.y + z * o.z;
        }

        /** Rotate this vector about the specified axis, by the specified angle in radian */
        public Point3D rotate(Point3D rotationAxis, double rotationAngle) {
            RotationMatrix rotation = new RotationMatrix(rotationAxis, rotationAngle);
            return rotation.multiply(this);
        }
    }

    /**
     * This 3D rotation matrix allows for rotating 3D vectors (eg. Point3D above) about a specified axis (also Point3D)
     * and by a specified angle in radian.
     */
    record RotationMatrix(double[][] matrix) {
        public RotationMatrix {
            assert matrix.length == 3;
            for (double[] doubles : matrix) {
                assert doubles.length == 3;
            }
        }

        public RotationMatrix(Point3D axis, double angleInRadian) {
            this(matrixFrom(axis, angleInRadian));
        }

        /** Perform the rotation by multiplying this matrix by the incoming vector */
        public Point3D multiply(Point3D incoming) {
            double x = dot(incoming, matrix[0]);
            double y = dot(incoming, matrix[1]);
            double z = dot(incoming, matrix[2]);
            return new Point3D(x, y, z);
        }

        private static double dot(Point3D incoming, double[] row) {
            return incoming.x * row[0] + incoming.y * row[1] + incoming.z * row[2];
        }

        /**
         * Create the actual rotation matrix cell values according to the formulas at
         * <a href="https://en.wikipedia.org/wiki/Rotation_matrix#Rotation_matrix_from_axis_and_angle">Rotation matrix (wikipedia)</a>
         */
        private static double[][] matrixFrom(Point3D axis, double angle) {
            double[][] matrix = new double[3][];
            double x = axis.x;
            double y = axis.y;
            double z = axis.z;
            matrix[0] = new double[] { diag(angle, x), cell(angle, x, y, -z), cell(angle, x, z, y) };
            matrix[1] = new double[] { cell(angle, y, x, z), diag(angle, y), cell(angle, y, z, -x) };
            matrix[2] = new double[] { cell(angle, z, x, -y), cell(angle, z, y, x), diag(angle, z) };
            return matrix;
        }

        /** The values along the diagonal */
        private static double diag(double angle, double coord) {
            double cosTheta = cos(angle);
            return cosTheta + coord * coord * (1 - cosTheta);
        }

        /** The values along the diagonal */
        private static double cell(double angle, double a, double b, double c) {
            return a * b * (1 - cos(angle)) + c * sin(angle);
        }
    }
}
