/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.geometry.simplify;

public interface SimplificationErrorCalculator {
    double calculateError(PointLike left, PointLike middle, PointLike right);

    interface PointLike {
        double getX();

        double getY();
    }

    class TriangleAreaCalculator implements SimplificationErrorCalculator {

        @Override
        public double calculateError(PointLike left, PointLike middle, PointLike right) {
            double xb = middle.getX() - left.getX();
            double yb = middle.getY() - left.getY();
            double xc = right.getX() - left.getX();
            double yc = right.getY() - left.getY();
            return 0.5 * Math.abs(xb * yc - xc * yb);
        }
    }

    class FrechetErrorCalculator implements SimplificationErrorCalculator {

        @Override
        public double calculateError(PointLike left, PointLike middle, PointLike right) {
            // Offset coordinates so left is at the origin
            double leftX = 0;
            double leftY = 0;
            double middleX = middle.getX() - left.getX();
            double middleY = middle.getY() - left.getY();
            double rightX = right.getX() - left.getX();
            double rightY = right.getY() - left.getY();
            // Rotate coordinates so that left->right is horizontal
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
    }
}
