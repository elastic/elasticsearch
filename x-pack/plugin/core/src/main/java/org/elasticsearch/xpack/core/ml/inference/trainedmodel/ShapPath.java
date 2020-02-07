/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel;


import java.util.Arrays;
import java.util.Collections;

/**
 * Ported from https://github.com/elastic/ml-cpp/blob/master/include/maths/CTreeShapFeatureImportance.h Path struct
 */
public class ShapPath  {
    private static final double DBL_EPSILON = Math.ulp(1.0);

    private double[] fractionOnes;
    private double[] fractionZeros;
    private int[] featureIndex;
    private double[] scale;
    private int nextIndex = 0;
    private int maxLength;

    public ShapPath(ShapPath path) {
        this.featureIndex = Arrays.copyOf(path.featureIndex, path.maxLength);
        this.fractionOnes = Arrays.copyOf(path.fractionOnes, path.maxLength);
        this.fractionZeros = Arrays.copyOf(path.fractionZeros, path.maxLength);
        this.scale = Arrays.copyOf(path.scale, path.maxLength);
        this.maxLength = path.maxLength;
        this.nextIndex = path.nextIndex;
    }

    public ShapPath(int length) {
        fractionOnes = new double[length];
        fractionZeros = new double[length];
        featureIndex = Collections.nCopies(length, -1).stream().mapToInt(Integer::intValue).toArray();
        scale = new double[length];
        maxLength = length;
    }

    public void reset(ShapPath path) {
        if (path == null) {
            return;
        }
        if (this.maxLength != path.maxLength) {
            throw new IllegalArgumentException("paths must have the same length");
        }
        for (int i = 0; i < this.maxLength; i++) {
            this.featureIndex[i] = path.featureIndex[i];
            this.fractionOnes[i] = path.fractionOnes[i];
            this.fractionZeros[i] = path.fractionZeros[i];
            this.scale[i] = path.scale[i];
        }
        this.nextIndex = path.nextIndex;
    }

    // Update binomial coefficients to be able to compute Equation (2) from the paper.  In particular,
    // we have in the line path.scale[i + 1] += fractionOne * path.scale[i] * (i + 1.0) / (pathDepth +
    // 1.0) that if we're on the "one" path, i.e. if the last feature selects this path if we include that
    // feature in S (then fractionOne is 1), and we need to consider all the additional ways we now have of
    // constructing each S of each given cardinality i + 1. Each of these come by adding the last feature
    // to sets of size i and we **also** need to scale by the difference in binomial coefficients as both M
    // increases by one and i increases by one. So we get additive term 1{last feature selects path if in S}
    // * scale(i) * (i+1)! (M+1-(i+1)-1)!/(M+1)! / (i! (M-i-1)!/ M!), whence += scale(i) * (i+1) / (M+1).
    public void extend(int featureIndex, double fractionZero, double fractionOne) {
        if (nextIndex < maxLength) {
            this.featureIndex[nextIndex] = featureIndex;
            fractionZeros[nextIndex] = fractionZero;
            fractionOnes[nextIndex] = fractionOne;
            if (nextIndex == 0) {
                scale[nextIndex] = 1.0;
            } else {
                scale[nextIndex] = 0.0;
            }
            ++nextIndex;
        }
        int pathDepth = depth();
        for (int i = (pathDepth - 1); i >= 0; --i) {
            scale[i + 1] += fractionOne * scale[i] * (i + 1.0) / (pathDepth + 1.0);
            scale[i] = fractionZero * scale[i] * (pathDepth - i) / (pathDepth + 1.0);
        }
    }

    private void reduce(int pathIndex) {
        for (int i = pathIndex; i < depth(); ++i) {
            featureIndex[i] = featureIndex[i + 1];
            fractionZeros[i] = fractionZeros[i + 1];
            fractionOnes[i] = fractionOnes[i + 1];
        }
        --nextIndex;
    }

    public double sumUnwoundPath(int pathIndex) {
        double total = 0.0;
        int pathDepth = depth();
        double nextFractionOne = scale[pathDepth];
        double fractionOne = fractionOnes[pathIndex];
        double fractionZero = fractionZeros[pathIndex];

        if (fractionOne != 0) {
            for (int i = pathDepth - 1; i >= 0; --i) {
                double tmp = nextFractionOne * (pathDepth + 1) / ((i + 1) * fractionOne + DBL_EPSILON);
                nextFractionOne = scale[i] - tmp * fractionZero * (pathDepth - i) / (pathDepth + 1);
                total += tmp;
            }
        } else {
            for (int i = pathDepth - 1; i >= 0; --i) {
                total += scale[i] * (pathDepth + 1) / (fractionZero * (pathDepth - i) + DBL_EPSILON);
            }
        }

        return total;
    }

    public void unwind(int pathIndex) {
        int pathDepth = depth();
        double nextFractionOne = scale[pathDepth];
        double fractionOne = fractionOnes[pathIndex];
        double fractionZero = fractionZeros[pathIndex];

        if (fractionOne != 0) {
            for (int i = pathDepth - 1; i >= 0; --i) {
                double tmp = nextFractionOne * (pathDepth + 1) / ((i + 1) * fractionOne);
                nextFractionOne = scale[i] - tmp * fractionZero * (pathDepth - i) / (pathDepth + 1);
                scale[i] = tmp;
            }
        } else {
            for (int i = pathDepth - 1; i >= 0; --i) {
                scale[i] = scale[i] * (pathDepth + 1) / (fractionZero * (pathDepth - i));
            }
        }
        reduce(pathIndex);
    }

    //! Indicator whether or not the feature \p pathIndex is decicive for the path.
    public double fractionOnes(int pathIndex) {
        return fractionOnes[pathIndex];
    }

    //! Fraction of all training data that reached the \pathIndex in the path.
    public double fractionZeros(int pathIndex) {
        return fractionZeros[pathIndex];
    }
    //! Current depth in the tree
    public int depth() { return nextIndex - 1; }

    public int findFeatureIndex(int splitFeature) {
        for (int i = 0; i < nextIndex; i++) {
            if (featureIndex[i] == splitFeature) {
                return i;
            }
        }
        return -1;
    }

    public int featureIndex(int pathIndex) {
        return featureIndex[pathIndex];
    }

    public int nextIndex() {
        return nextIndex;
    }
}
