/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.geometry.simplify;

import java.util.Locale;

public interface SimplificationErrorCalculator {
    double calculateError(PointLike left, PointLike middle, PointLike right);

    interface PointLike {
        double x();

        double y();
    }

    static SimplificationErrorCalculator byName(String calculatorName) {
        return switch (calculatorName.toLowerCase(Locale.ROOT)) {
            case "cartesiantrianglearea" -> new CartesianTriangleAreaCalculator();
            case "trianglearea" -> new TriangleAreaCalculator();
            case "triangleheight" -> new TriangleHeightCalculator();
            case "frecheterror" -> new FrechetErrorCalculator();
            default -> throw new IllegalArgumentException("Unknown geometry simplification error calculator: " + calculatorName);
        };
    }

    /**
     * Calculate the triangle area using cartesian coordinates as described at https://en.wikipedia.org/wiki/Area_of_a_triangle
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
     * Calculate the triangle area using geographic coordinates and Herons formula (side lengths)
     * as described at https://en.wikipedia.org/wiki/Area_of_a_triangle
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
     * Calculate the triangle area using geographic coordinates and Herons formula (side lengths)
     * as described at https://en.wikipedia.org/wiki/Area_of_a_triangle, but scale the area down
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
                // Herons formula, scaled by 1/a
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

    class FrechetErrorCalculator implements SimplificationErrorCalculator {

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
                // Return distance to x-axis TODO: also include back-paths for Frechet distance calculation
                return Math.abs(middleYrotated);
            } else {
                // If left and right points are co-located, we assume no consequence to removing the middle point
                return 0.0;
            }
        }
    }
}
