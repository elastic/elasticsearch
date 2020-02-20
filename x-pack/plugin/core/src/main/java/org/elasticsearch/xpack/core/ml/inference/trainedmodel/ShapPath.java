/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel;


/**
 * Ported from https://github.com/elastic/ml-cpp/blob/master/include/maths/CTreeShapFeatureImportance.h Path struct
 */
public class ShapPath  {
    private static final double DBL_EPSILON = Math.ulp(1.0);

    private final PathElement[] pathElements;
    private final double[] scale;
    private int currFraction;
    private int currScale;

    public ShapPath(ShapPath path, int nextIndex) {
        this.currFraction = path.currFraction + nextIndex;
        this.currScale = path.currScale + nextIndex;
        this.pathElements = path.pathElements;
        this.scale = path.scale;
        for (int i = 0; i < nextIndex; i++) {
            pathElements[currFraction + i] = new PathElement(path.getElement(i));
            scale[currScale + i] = path.getScale(i);
        }
    }

    public ShapPath(PathElement[] elements, double[] scale) {
        this.pathElements = elements;
        this.scale = scale;
    }

    // Update binomial coefficients to be able to compute Equation (2) from the paper.  In particular,
    // we have in the line path.scale[i + 1] += fractionOne * path.scale[i] * (i + 1.0) / (pathDepth +
    // 1.0) that if we're on the "one" path, i.e. if the last feature selects this path if we include that
    // feature in S (then fractionOne is 1), and we need to consider all the additional ways we now have of
    // constructing each S of each given cardinality i + 1. Each of these come by adding the last feature
    // to sets of size i and we **also** need to scale by the difference in binomial coefficients as both M
    // increases by one and i increases by one. So we get additive term 1{last feature selects path if in S}
    // * scale(i) * (i+1)! (M+1-(i+1)-1)!/(M+1)! / (i! (M-i-1)!/ M!), whence += scale(i) * (i+1) / (M+1).
    public int extend(double fractionZero, double fractionOne, int featureIndex, int nextIndex) {
        setValues(nextIndex, fractionOne, fractionZero, featureIndex);
        setScale(nextIndex, nextIndex == 0 ? 1.0 : 0.0);
        double stepDown = fractionOne / (double)(nextIndex + 1);
        double stepUp = fractionZero / (double)(nextIndex + 1);
        double countDown = nextIndex * stepDown;
        double countUp = stepUp;
        for (int i = (nextIndex - 1); i >= 0; --i, countDown -= stepDown, countUp += stepUp) {
            setScale(i + 1, getScale(i + 1) + getScale(i) * countDown);
            setScale(i, getScale(i) * countUp);
        }
        return nextIndex + 1;
    }

    public double sumUnwoundPath(int pathIndex, int nextIndex) {
        double total = 0.0;
        int pathDepth = nextIndex - 1;
        double nextFractionOne = getScale(pathDepth);
        double fractionOne = fractionOnes(pathIndex);
        double fractionZero = fractionZeros(pathIndex);
        if (fractionOne != 0) {
            double pD = pathDepth + 1;
            double stepUp = fractionZero / pD;
            double stepDown = fractionOne / pD;
            double countUp = stepUp;
            double countDown = (pD - 1.0) * stepDown;
            for (int i = pathDepth - 1; i >= 0; --i, countUp += stepUp, countDown -= stepDown) {
                double tmp = nextFractionOne / countDown;
                nextFractionOne = getScale(i) - tmp * countUp;
                total += tmp;
            }
        } else {
            double pD = pathDepth;

            for(int i = 0; i < pathDepth; i++) {
                total += getScale(i) / pD--;
            }
            total *= (pathDepth + 1) / (fractionZero + DBL_EPSILON);
        }

        return total;
    }

    public int unwind(int pathIndex, int nextIndex) {
        int pathDepth = nextIndex - 1;
        double nextFractionOne = getScale(pathDepth);
        double fractionOne = fractionOnes(pathIndex);
        double fractionZero = fractionZeros(pathIndex);

        if (fractionOne != 0) {
            double stepUp = fractionZero / (double)(pathDepth + 1);
            double stepDown = fractionOne / (double)nextIndex;
            double countUp = 0.0;
            double countDown = nextIndex * stepDown;
            for (int i = pathDepth; i >= 0; --i, countUp += stepUp, countDown -= stepDown) {
                double tmp = nextFractionOne / countDown;
                nextFractionOne = getScale(i) - tmp * countUp;
                setScale(i, tmp);
            }
        } else {
            double stepDown = (fractionZero + DBL_EPSILON) / (double)(pathDepth + 1);
            double countDown = pathDepth * stepDown;
            for (int i = 0; i <= pathDepth; ++i, countDown -= stepDown) {
                setScale(i, getScale(i) / countDown);
            }
        }
        for (int i = pathIndex; i < pathDepth; ++i) {
            PathElement element = getElement(i + 1);
            setValues(i, element.fractionOnes, element.fractionZeros, element.featureIndex);
        }
        return nextIndex - 1;
    }

    private void setValues(int index, double fractionOnes, double fractionZeros, int featureIndex) {
        pathElements[index + currFraction].fractionOnes = fractionOnes;
        pathElements[index + currFraction].fractionZeros = fractionZeros;
        pathElements[index + currFraction].featureIndex = featureIndex;
    }

    private double getScale(int offset) {
        return scale[offset + currScale];
    }

    private void setScale(int offset, double value) {
        scale[offset + currScale] = value;
    }

    public double fractionOnes(int pathIndex) {
        return pathElements[pathIndex + currFraction].fractionOnes;
    }

    public double fractionZeros(int pathIndex) {
        return pathElements[pathIndex + currFraction].fractionZeros;
    }

    public int findFeatureIndex(int splitFeature, int nextIndex) {
        for (int i = currFraction; i < currFraction + nextIndex; i++) {
            if (pathElements[i].featureIndex == splitFeature) {
                return i - currFraction;
            }
        }
        return -1;
    }

    public int featureIndex(int pathIndex) {
        return pathElements[pathIndex + currFraction].featureIndex;
    }

    private PathElement getElement(int offset) {
        return pathElements[offset + currFraction];
    }

    public final static class PathElement {
        double fractionOnes = 1.0;
        double fractionZeros = 1.0;
        int featureIndex = -1;
        public PathElement() {}

        public PathElement(PathElement element) {
            this.featureIndex  = element.featureIndex;
            this.fractionOnes  = element.fractionOnes;
            this.fractionZeros = element.fractionZeros;
        }
    }
}
