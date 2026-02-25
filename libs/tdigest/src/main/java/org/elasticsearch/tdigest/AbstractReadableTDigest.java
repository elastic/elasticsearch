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

/**
 * Base class for read-only digest implementations backed by centroid iterators.
 */
public abstract class AbstractReadableTDigest implements ReadableTDigest {

    /**
     * Iterator over centroids in increasing mean order.
     */
    public interface CentroidIterator {
        boolean next();

        long currentCount();

        double currentMean();

        /**
         * Returns {@code true} iff a call to {@link #next()} would return {@code false}.
         */
        boolean endReached();
    }

    /**
     * Returns an iterator over this digest's centroids.
     */
    protected abstract CentroidIterator centroidIterator();

    @Override
    public double cdf(double x) {
        TDigest.checkValue(x);
        long digestSize = size();
        if (digestSize == 0) {
            return Double.NaN;
        }
        CentroidIterator centroids = centroidIterator();
        if (centroids.next() == false) {
            return Double.NaN;
        }
        double firstMean = centroids.currentMean();
        long firstCount = centroids.currentCount();
        if (centroids.next() == false) {
            if (x < firstMean) {
                return 0;
            }
            if (x > firstMean) {
                return 1;
            }
            return 0.5;
        }

        double min = firstMean;
        double max = getMax();
        if (x < min) {
            return 0;
        }
        if (Double.compare(x, min) == 0) {
            double dw = firstCount;
            while (Double.compare(centroids.currentMean(), x) == 0) {
                dw += centroids.currentCount();
                if (centroids.next() == false) {
                    break;
                }
            }
            return dw / 2.0 / digestSize;
        }

        if (x > max) {
            return 1;
        }
        if (Double.compare(x, max) == 0) {
            double dw = 0;
            CentroidIterator it = centroidIterator();
            while (it.next()) {
                if (Double.compare(it.currentMean(), x) == 0) {
                    dw += it.currentCount();
                }
            }
            return (digestSize - dw / 2.0) / digestSize;
        }

        double aMean = firstMean;
        long aCount = firstCount;
        double bMean = centroids.currentMean();
        long bCount = centroids.currentCount();
        boolean hasMore = centroids.next();

        double left = (bMean - aMean) / 2;
        double right = left;
        double weightSoFar = 0;
        while (hasMore) {
            if (x < aMean + right) {
                double value = (weightSoFar + aCount * AbstractTDigest.interpolate(x, aMean - left, aMean + right)) / digestSize;
                return Math.max(value, 0.0);
            }
            weightSoFar += aCount;
            aMean = bMean;
            aCount = bCount;
            left = right;
            bMean = centroids.currentMean();
            bCount = centroids.currentCount();
            hasMore = centroids.next();
            right = (bMean - aMean) / 2;
        }
        if (x < aMean + right) {
            return (weightSoFar + aCount * AbstractTDigest.interpolate(x, aMean - right, aMean + right)) / digestSize;
        }
        return 1;
    }

    @Override
    public double quantile(double q) {
        if (q < 0 || q > 1) {
            throw new IllegalArgumentException("q should be in [0,1], got " + q);
        }
        long digestSize = size();
        CentroidIterator centroids = centroidIterator();
        if (digestSize == 0) {
            return Double.NaN;
        }
        if (centroids.next() == false) {
            return Double.NaN;
        }
        double currentMean = centroids.currentMean();
        long currentWeight = centroids.currentCount();
        if (centroids.next() == false) {
            return currentMean;
        }

        double target = q * digestSize;
        double min = getMin();
        if (target <= 0) {
            return min;
        }
        double max = getMax();
        if (target >= digestSize) {
            return max;
        }

        double weightSoFar = currentWeight / 2.0;
        if (target <= weightSoFar && weightSoFar > 1) {
            return AbstractTDigest.weightedAverage(min, weightSoFar - target, currentMean, target);
        }
        do {
            double nextMean = centroids.currentMean();
            long nextWeight = centroids.currentCount();
            double dw = (currentWeight + nextWeight) / 2.0;
            if (target < weightSoFar + dw) {
                double w1 = target - weightSoFar;
                double w2 = weightSoFar + dw - target;
                return AbstractTDigest.weightedAverage(currentMean, w2, nextMean, w1);
            }
            weightSoFar += dw;
            currentMean = nextMean;
            currentWeight = nextWeight;
        } while (centroids.next());
        double w1 = target - weightSoFar;
        double w2 = currentWeight / 2.0 - w1;
        return AbstractTDigest.weightedAverage(currentMean, w2, max, w1);
    }

}
