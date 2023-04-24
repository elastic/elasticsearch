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
        double getX();

        double getY();
    }

    static SimplificationErrorCalculator byName(String calculatorName) {
        return switch (calculatorName.toLowerCase(Locale.ROOT)) {
            case "cartesiantrianglearea" -> new CartesianTriangleAreaCalculator();
            case "trianglearea" -> new TriangleAreaCalculator();
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
            double xb = middle.getX() - left.getX();
            double yb = middle.getY() - left.getY();
            double xc = right.getX() - left.getX();
            double yc = right.getY() - left.getY();
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
            return SloppyMath.haversinMeters(a.getY(), a.getX(), b.getY(), b.getX());
        }
    }

    class FrechetErrorCalculator implements SimplificationErrorCalculator {

        @Override
        public double calculateError(PointLike left, PointLike middle, PointLike right) {
            // Offset coordinates so left is at the origin
            double rightX = right.getX() - left.getX();
            double rightY = right.getY() - left.getY();
            if (Math.abs(rightX) > 1e-10 || Math.abs(rightY) > 1e-10) {
                // Rotate coordinates so that left->right is horizontal
                double len = Math.sqrt(rightX * rightX + rightY * rightY);
                double cos = rightX / len;
                double sin = rightY / len;
                double middleX = middle.getX() - left.getX();
                double middleY = middle.getY() - left.getY();
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
