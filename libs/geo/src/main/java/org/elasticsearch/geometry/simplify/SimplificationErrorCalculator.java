/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.geometry.simplify;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;

public interface SimplificationErrorCalculator {
    double calculateError(PointLike left, PointLike middle, PointLike right);

    interface PointLike {
        double x();

        double y();
    }

    class Registry {
        static ArrayList<String> names = new ArrayList<>();
        static HashMap<String, SimplificationErrorCalculator> map = new HashMap<>();

        static {
            add("cartesiantrianglearea", new CartesianTriangleAreaCalculator());
            add("trianglearea", new TriangleAreaCalculator());
            add("triangleheight", new TriangleHeightCalculator());
            add("simplefrecheterror", new SimpleFrechetErrorCalculator());
        }

        static void add(String name, SimplificationErrorCalculator calculator) {
            String key = name.toLowerCase(Locale.ROOT);
            if (map.containsKey(key)) {
                throw new IllegalArgumentException("Duplicate calculator name key: " + key);
            }
            map.put(key, calculator);
            names.add(key);
        }

        static SimplificationErrorCalculator get(String calculatorName) {
            SimplificationErrorCalculator calculator = map.get(calculatorName.toLowerCase(Locale.ROOT));
            if (calculator == null) {
                throw new IllegalArgumentException("Unknown geometry simplification error calculator: " + calculatorName);
            }
            return calculator;
        }
    }

    static List<String> names() {
        return Registry.names;
    }

    static SimplificationErrorCalculator byName(String calculatorName) {
        return Registry.get(calculatorName);
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

        private double distance(PointLike a, PointLike b) {
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

        private double distance(PointLike a, PointLike b) {
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
    class SimpleFrechetErrorCalculator implements SimplificationErrorCalculator {

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
                double deltaL = -Math.min(0, middleXrotated);
                double deltaR = Math.max(0, middleXrotated - rightXrotated);
                double backDistance = Math.max(deltaR, deltaL);
                return Math.max(height, backDistance);
            } else {
                // If left and right points are co-located, we assume no consequence to removing the middle point
                return 0.0;
            }
        }
    }
}
