/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * This project is based on a modification of https://github.com/tdunning/t-digest which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.tdigest;

import java.util.Collection;
import java.util.List;
import java.util.Locale;

/**
 * Adaptive histogram based on something like streaming k-means crossed with Q-digest.
 * The special characteristics of this algorithm are:
 * - smaller summaries than Q-digest
 * - works on doubles as well as integers.
 * - provides part per million accuracy for extreme quantiles and typically &lt;1000 ppm accuracy for middle quantiles
 * - fast
 * - simple
 * - test coverage roughly at 90%
 * - easy to adapt for use with map-reduce
 */
public abstract class TDigest {
    protected ScaleFunction scale = ScaleFunction.K_2;
    double min = Double.POSITIVE_INFINITY;
    double max = Double.NEGATIVE_INFINITY;

    /**
     * Creates an {@link MergingDigest}.  This is the fastest implementation for large sample populations, with constant memory
     * allocation while delivering relating accuracy close to 1%.
     *
     * @param compression The compression parameter.  100 is a common value for normal uses.  1000 is extremely large.
     *                    The number of centroids retained will be a smallish (usually less than 10) multiple of this number.
     * @return the MergingDigest
     */
    public static TDigest createMergingDigest(double compression) {
        return new MergingDigest(compression);
    }

    /**
     * Creates an {@link AVLTreeDigest}.  This is the most accurate implementation, delivering relative accuracy close to 0.01% for large
     * sample populations. Still, its construction takes 2x-10x longer than {@link MergingDigest}, while its memory footprint increases
     * (slowly) with the sample population size.
     *
     * @param compression The compression parameter.  100 is a common value for normal uses.  1000 is extremely large.
     *                    The number of centroids retained will be a smallish (usually less than 10) multiple of this number.
     * @return the AvlTreeDigest
     */
    public static TDigest createAvlTreeDigest(double compression) {
        return new AVLTreeDigest(compression);
    }

    /**
     * Creates a {@link SortingDigest}.  SortingDigest is the most accurate and an extremely fast implementation but stores all samples
     * internally so it uses much more memory than the rest, for sample populations of 1000 or higher.
     *
     * @return the SortingDigest
     */
    public static TDigest createSortingDigest() {
        return new SortingDigest();
    }

    /**
     * Creates a {@link HybridDigest}.  HybridDigest uses a SortingDigest for small sample populations, then switches to a MergingDigest,
     * thus combining the best of both implementations:  fastest overall, small footprint and perfect accuracy for small populations,
     * constant memory footprint and acceptable accuracy for larger ones.
     *
     * @param compression The compression parameter.  100 is a common value for normal uses.  1000 is extremely large.
     *                    The number of centroids retained will be a smallish (usually less than 10) multiple of this number.
     * @return the HybridDigest
     */
    public static TDigest createHybridDigest(double compression) {
        return new HybridDigest(compression);
    }

    /**
     * Adds a sample to a histogram.
     *
     * @param x The value to add.
     * @param w The weight of this point.
     */
    public abstract void add(double x, int w);

    /**
     * Add a single sample to this TDigest.
     *
     * @param x The data value to add
     */
    public final void add(double x) {
        add(x, 1);
    }

    final void checkValue(double x) {
        if (Double.isNaN(x) || Double.isInfinite(x)) {
            throw new IllegalArgumentException("Invalid value: " + x);
        }
    }

    public abstract void add(List<? extends TDigest> others);

    /**
     * Re-examines a t-digest to determine whether some centroids are redundant.  If your data are
     * perversely ordered, this may be a good idea.  Even if not, this may save 20% or so in space.
     *
     * The cost is roughly the same as adding as many data points as there are centroids.  This
     * is typically &lt; 10 * compression, but could be as high as 100 * compression.
     *
     * This is a destructive operation that is not thread-safe.
     */
    public abstract void compress();

    /**
     * Returns the number of points that have been added to this TDigest.
     *
     * @return The sum of the weights on all centroids.
     */
    public abstract long size();

    /**
     * Returns the fraction of all points added which are &le; x. Points
     * that are exactly equal get half credit (i.e. we use the mid-point
     * rule)
     *
     * @param x The cutoff for the cdf.
     * @return The fraction of all data which is less or equal to x.
     */
    public abstract double cdf(double x);

    /**
     * Returns an estimate of a cutoff such that a specified fraction of the data
     * added to this TDigest would be less than or equal to the cutoff.
     *
     * @param q The desired fraction
     * @return The smallest value x such that cdf(x) &ge; q
     */
    public abstract double quantile(double q);

    /**
     * A {@link Collection} that lets you go through the centroids in ascending order by mean.  Centroids
     * returned will not be re-used, but may or may not share storage with this TDigest.
     *
     * @return The centroids in the form of a Collection.
     */
    public abstract Collection<Centroid> centroids();

    /**
     * Returns the current compression factor.
     *
     * @return The compression factor originally used to set up the TDigest.
     */
    public abstract double compression();

    /**
     * Returns the number of bytes required to encode this TDigest using #asBytes().
     *
     * @return The number of bytes required.
     */
    public abstract int byteSize();

    public void setScaleFunction(ScaleFunction scaleFunction) {
        if (scaleFunction.toString().endsWith("NO_NORM")) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Can't use %s as scale with %s", scaleFunction, this.getClass()));
        }
        this.scale = scaleFunction;
    }

    /**
     * Add all of the centroids of another TDigest to this one.
     *
     * @param other The other TDigest
     */
    public abstract void add(TDigest other);

    public abstract int centroidCount();

    /**
     * Prepare internal structure for loading the requested number of samples.
     * @param size number of samples to be loaded
     */
    public void reserve(long size) {}

    public double getMin() {
        return min;
    }

    public double getMax() {
        return max;
    }

    /**
     * Override the min and max values for testing purposes
     */
    void setMinMax(double min, double max) {
        this.min = min;
        this.max = max;
    }
}
