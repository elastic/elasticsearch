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

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.tdigest.arrays.TDigestArrays;
import org.elasticsearch.tdigest.arrays.TDigestDoubleArray;
import org.elasticsearch.tdigest.arrays.TDigestIntArray;

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;

/**
 * Maintains a t-digest by collecting new points in a buffer that is then sorted occasionally and merged
 * into a sorted array that contains previously computed centroids.
 * <p>
 * This can be very fast because the cost of sorting and merging is amortized over several insertion. If
 * we keep N centroids total and have the input array is k long, then the amortized cost is something like
 * <p>
 * N/k + log k
 * <p>
 * These costs even out when N/k = log k.  Balancing costs is often a good place to start in optimizing an
 * algorithm.  For different values of compression factor, the following table shows estimated asymptotic
 * values of N and suggested values of k:
 * <table>
 * <thead>
 * <tr><td>Compression</td><td>N</td><td>k</td></tr>
 * </thead>
 * <tbody>
 * <tr><td>50</td><td>78</td><td>25</td></tr>
 * <tr><td>100</td><td>157</td><td>42</td></tr>
 * <tr><td>200</td><td>314</td><td>73</td></tr>
 * </tbody>
 * <caption>Sizing considerations for t-digest</caption>
 * </table>
 * <p>
 * The virtues of this kind of t-digest implementation include:
 * <ul>
 * <li>No allocation is required after initialization</li>
 * <li>The data structure automatically compresses existing centroids when possible</li>
 * <li>No Java object overhead is incurred for centroids since data is kept in primitive arrays</li>
 * </ul>
 * <p>
 * The current implementation takes the liberty of using ping-pong buffers for implementing the merge resulting
 * in a substantial memory penalty, but the complexity of an in place merge was not considered as worthwhile
 * since even with the overhead, the memory cost is less than 40 bytes per centroid which is much less than half
 * what the AVLTreeDigest uses and no dynamic allocation is required at all.
 */
public class MergingDigest extends AbstractTDigest {
    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(MergingDigest.class);

    private final TDigestArrays arrays;
    private boolean closed = false;

    private int mergeCount = 0;

    private final double publicCompression;
    private final double compression;

    // points to the first unused centroid
    private int lastUsedCell;

    // sum_i weight.get(i) See also unmergedWeight
    private double totalWeight = 0;

    // number of points that have been added to each merged centroid
    private final TDigestDoubleArray weight;
    // mean of points added to each merged centroid
    private final TDigestDoubleArray mean;

    // sum_i tempWeight.get(i)
    private double unmergedWeight = 0;

    // this is the index of the next temporary centroid
    // this is a more Java-like convention than lastUsedCell uses
    private int tempUsed = 0;
    private final TDigestDoubleArray tempWeight;
    private final TDigestDoubleArray tempMean;

    // array used for sorting the temp centroids. This is a field
    // to avoid allocations during operation
    private final TDigestIntArray order;

    // if true, alternate upward and downward merge passes
    public boolean useAlternatingSort = true;
    // if true, use higher working value of compression during construction, then reduce on presentation
    public boolean useTwoLevelCompression = true;

    // this forces centroid merging based on size limit rather than
    // based on accumulated k-index. This can be much faster since we
    // scale functions are more expensive than the corresponding
    // weight limits.
    public static final boolean useWeightLimit = true;

    static MergingDigest create(TDigestArrays arrays, double compression) {
        arrays.adjustBreaker(SHALLOW_SIZE);
        try {
            return new MergingDigest(arrays, compression);
        } catch (Exception e) {
            arrays.adjustBreaker(-SHALLOW_SIZE);
            throw e;
        }
    }

    static MergingDigest create(TDigestArrays arrays, double compression, int bufferSize, int size) {
        arrays.adjustBreaker(SHALLOW_SIZE);
        try {
            return new MergingDigest(arrays, compression, bufferSize, size);
        } catch (Exception e) {
            arrays.adjustBreaker(-SHALLOW_SIZE);
            throw e;
        }
    }

    /**
     * Allocates a buffer merging t-digest.  This is the normally used constructor that
     * allocates default sized internal arrays.  Other versions are available, but should
     * only be used for special cases.
     *
     * @param compression The compression factor
     */
    private MergingDigest(TDigestArrays arrays, double compression) {
        this(arrays, compression, -1);
    }

    /**
     * If you know the size of the temporary buffer for incoming points, you can use this entry point.
     *
     * @param compression Compression factor for t-digest.  Same as 1/\delta in the paper.
     * @param bufferSize  How many samples to retain before merging.
     */
    private MergingDigest(TDigestArrays arrays, double compression, int bufferSize) {
        // we can guarantee that we only need ceiling(compression).
        this(arrays, compression, bufferSize, -1);
    }

    /**
     * Fully specified constructor.  Normally only used for deserializing a buffer t-digest.
     *
     * @param compression Compression factor
     * @param bufferSize  Number of temporary centroids
     * @param size        Size of main buffer
     */
    private MergingDigest(TDigestArrays arrays, double compression, int bufferSize, int size) {
        this.arrays = arrays;

        // ensure compression >= 10
        // default size = 2 * ceil(compression)
        // default bufferSize = 5 * size
        // scale = max(2, bufferSize / size - 1)
        // compression, publicCompression = sqrt(scale-1)*compression, compression
        // ensure size > 2 * compression + weightLimitFudge
        // ensure bufferSize > 2*size

        // force reasonable value. Anything less than 10 doesn't make much sense because
        // too few centroids are retained
        if (compression < 10) {
            compression = 10;
        }

        // the weight limit is too conservative about sizes and can require a bit of extra room
        double sizeFudge = 0;
        if (useWeightLimit) {
            sizeFudge = 10;
        }

        // default size
        size = (int) Math.max(compression + sizeFudge, size);

        // default buffer size has enough capacity
        if (bufferSize < 5 * size) {
            // TODO update with current numbers
            // having a big buffer is good for speed
            // experiments show bufferSize = 1 gives half the performance of bufferSize=10
            // bufferSize = 2 gives 40% worse performance than 10
            // but bufferSize = 5 only costs about 5-10%
            //
            // compression factor time(us)
            // 50 1 0.275799
            // 50 2 0.151368
            // 50 5 0.108856
            // 50 10 0.102530
            // 100 1 0.215121
            // 100 2 0.142743
            // 100 5 0.112278
            // 100 10 0.107753
            // 200 1 0.210972
            // 200 2 0.148613
            // 200 5 0.118220
            // 200 10 0.112970
            // 500 1 0.219469
            // 500 2 0.158364
            // 500 5 0.127552
            // 500 10 0.121505
            bufferSize = 5 * size;
        }

        // scale is the ratio of extra buffer to the final size
        // we have to account for the fact that we copy all live centroids into the incoming space
        double scale = Math.max(1, bufferSize / size - 1);
        if (useTwoLevelCompression == false) {
            scale = 1;
        }

        // publicCompression is how many centroids the user asked for
        // compression is how many we actually keep
        this.publicCompression = compression;
        this.compression = Math.sqrt(scale) * publicCompression;

        // changing the compression could cause buffers to be too small, readjust if so
        if (size < this.compression + sizeFudge) {
            size = (int) Math.ceil(this.compression + sizeFudge);
        }

        // ensure enough space in buffer (possibly again)
        if (bufferSize <= 2 * size) {
            bufferSize = 2 * size;
        }

        TDigestDoubleArray weight = null;
        TDigestDoubleArray mean = null;
        TDigestDoubleArray tempWeight = null;
        TDigestDoubleArray tempMean = null;
        TDigestIntArray order = null;

        try {
            this.weight = weight = arrays.newDoubleArray(size);
            this.mean = mean = arrays.newDoubleArray(size);

            this.tempWeight = tempWeight = arrays.newDoubleArray(bufferSize);
            this.tempMean = tempMean = arrays.newDoubleArray(bufferSize);
            this.order = order = arrays.newIntArray(bufferSize);
        } catch (Exception e) {
            Releasables.close(weight, mean, tempWeight, tempMean, order);
            throw e;
        }

        lastUsedCell = 0;
    }

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE + weight.ramBytesUsed() + mean.ramBytesUsed() + tempWeight.ramBytesUsed() + tempMean.ramBytesUsed() + order
            .ramBytesUsed();
    }

    @Override
    public void add(double x, long w) {
        checkValue(x);
        if (tempUsed >= tempWeight.size() - lastUsedCell - 1) {
            mergeNewValues();
        }
        int where = tempUsed++;
        tempWeight.set(where, w);
        tempMean.set(where, x);
        unmergedWeight += w;
        if (x < min) {
            min = x;
        }
        if (x > max) {
            max = x;
        }
    }

    private void mergeNewValues() {
        mergeNewValues(compression);
    }

    private void mergeNewValues(double compression) {
        if (totalWeight == 0 && unmergedWeight == 0) {
            // seriously nothing to do
            return;
        }
        if (unmergedWeight > 0) {
            // note that we run the merge in reverse every other merge to avoid left-to-right bias in merging
            merge(tempMean, tempWeight, tempUsed, order, unmergedWeight, useAlternatingSort & mergeCount % 2 == 1, compression);
            mergeCount++;
            tempUsed = 0;
            unmergedWeight = 0;
        }
    }

    private void merge(
        TDigestDoubleArray incomingMean,
        TDigestDoubleArray incomingWeight,
        int incomingCount,
        TDigestIntArray incomingOrder,
        double unmergedWeight,
        boolean runBackwards,
        double compression
    ) {
        // when our incoming buffer fills up, we combine our existing centroids with the incoming data,
        // and then reduce the centroids by merging if possible
        incomingMean.set(incomingCount, mean, 0, lastUsedCell);
        incomingWeight.set(incomingCount, weight, 0, lastUsedCell);
        incomingCount += lastUsedCell;

        Sort.stableSort(incomingOrder, incomingMean, incomingCount);

        totalWeight += unmergedWeight;

        // option to run backwards is to help investigate bias in errors
        if (runBackwards) {
            Sort.reverse(incomingOrder, 0, incomingCount);
        }

        // start by copying the least incoming value to the normal buffer
        lastUsedCell = 0;
        mean.set(lastUsedCell, incomingMean.get(incomingOrder.get(0)));
        weight.set(lastUsedCell, incomingWeight.get(incomingOrder.get(0)));
        double wSoFar = 0;

        // weight will contain all zeros after this loop

        double normalizer = scale.normalizer(compression, totalWeight);
        double k1 = scale.k(0, normalizer);
        double wLimit = totalWeight * scale.q(k1 + 1, normalizer);
        for (int i = 1; i < incomingCount; i++) {
            int ix = incomingOrder.get(i);
            double proposedWeight = weight.get(lastUsedCell) + incomingWeight.get(ix);
            double projectedW = wSoFar + proposedWeight;
            boolean addThis;
            if (useWeightLimit) {
                double q0 = wSoFar / totalWeight;
                double q2 = (wSoFar + proposedWeight) / totalWeight;
                addThis = proposedWeight <= totalWeight * Math.min(scale.max(q0, normalizer), scale.max(q2, normalizer));
            } else {
                addThis = projectedW <= wLimit;
            }
            if (i == 1 || i == incomingCount - 1) {
                // force first and last centroid to never merge
                addThis = false;
            }
            if (lastUsedCell == mean.size() - 1) {
                // use the last centroid, there's no more
                addThis = true;
            }

            if (addThis) {
                // next point will fit
                // so merge into existing centroid
                weight.set(lastUsedCell, weight.get(lastUsedCell) + incomingWeight.get(ix));
                mean.set(
                    lastUsedCell,
                    mean.get(lastUsedCell) + (incomingMean.get(ix) - mean.get(lastUsedCell)) * incomingWeight.get(ix) / weight.get(
                        lastUsedCell
                    )
                );
                incomingWeight.set(ix, 0);
            } else {
                // didn't fit ... move to next output, copy out first centroid
                wSoFar += weight.get(lastUsedCell);
                if (useWeightLimit == false) {
                    k1 = scale.k(wSoFar / totalWeight, normalizer);
                    wLimit = totalWeight * scale.q(k1 + 1, normalizer);
                }

                lastUsedCell++;
                mean.set(lastUsedCell, incomingMean.get(ix));
                weight.set(lastUsedCell, incomingWeight.get(ix));
                incomingWeight.set(ix, 0);
            }
        }
        // points to next empty cell
        lastUsedCell++;

        // sanity check
        double sum = 0;
        for (int i = 0; i < lastUsedCell; i++) {
            sum += weight.get(i);
        }
        assert sum == totalWeight;
        if (runBackwards) {
            Sort.reverse(mean, 0, lastUsedCell);
            Sort.reverse(weight, 0, lastUsedCell);
        }
        if (totalWeight > 0) {
            min = Math.min(min, mean.get(0));
            max = Math.max(max, mean.get(lastUsedCell - 1));
        }
    }

    /**
     * Merges any pending inputs and compresses the data down to the public setting.
     * Note that this typically loses a bit of precision and thus isn't a thing to
     * be doing all the time. It is best done only when we want to show results to
     * the outside world.
     */
    @Override
    public void compress() {
        mergeNewValues(publicCompression);
    }

    @Override
    public long size() {
        return (long) (totalWeight + unmergedWeight);
    }

    @Override
    public double cdf(double x) {
        checkValue(x);
        mergeNewValues();

        if (lastUsedCell == 0) {
            // no data to examine
            return Double.NaN;
        }
        if (lastUsedCell == 1) {
            if (x < min) return 0;
            if (x > max) return 1;
            return 0.5;
        } else {
            if (x < min) {
                return 0;
            }
            if (Double.compare(x, min) == 0) {
                // we have one or more centroids == x, treat them as one
                // dw will accumulate the weight of all of the centroids at x
                double dw = 0;
                for (int i = 0; i < lastUsedCell && Double.compare(mean.get(i), x) == 0; i++) {
                    dw += weight.get(i);
                }
                return dw / 2.0 / size();
            }

            if (x > max) {
                return 1;
            }
            if (x == max) {
                double dw = 0;
                for (int i = lastUsedCell - 1; i >= 0 && Double.compare(mean.get(i), x) == 0; i--) {
                    dw += weight.get(i);
                }
                return (size() - dw / 2.0) / size();
            }

            // initially, we set left width equal to right width
            double left = (mean.get(1) - mean.get(0)) / 2;
            double weightSoFar = 0;

            for (int i = 0; i < lastUsedCell - 1; i++) {
                double right = (mean.get(i + 1) - mean.get(i)) / 2;
                if (x < mean.get(i) + right) {
                    double value = (weightSoFar + weight.get(i) * interpolate(x, mean.get(i) - left, mean.get(i) + right)) / size();
                    return Math.max(value, 0.0);
                }
                weightSoFar += weight.get(i);
                left = right;
            }

            // for the last element, assume right width is same as left
            int lastOffset = lastUsedCell - 1;
            double right = (mean.get(lastOffset) - mean.get(lastOffset - 1)) / 2;
            if (x < mean.get(lastOffset) + right) {
                return (weightSoFar + weight.get(lastOffset) * interpolate(x, mean.get(lastOffset) - right, mean.get(lastOffset) + right))
                    / size();
            }
            return 1;
        }
    }

    @Override
    public double quantile(double q) {
        if (q < 0 || q > 1) {
            throw new IllegalArgumentException("q should be in [0,1], got " + q);
        }
        mergeNewValues();

        if (lastUsedCell == 0) {
            // no centroids means no data, no way to get a quantile
            return Double.NaN;
        } else if (lastUsedCell == 1) {
            // with one data point, all quantiles lead to Rome
            return mean.get(0);
        }

        // we know that there are at least two centroids now
        int n = lastUsedCell;

        // if values were stored in a sorted array, index would be the offset we are interested in
        final double index = q * totalWeight;

        // beyond the boundaries, we return min or max
        // usually, the first and last centroids have unit weights so this will make it moot
        if (index < 0) {
            return min;
        }
        if (index >= totalWeight) {
            return max;
        }

        double weightSoFar = weight.get(0) / 2;

        // if the left centroid has more than one sample, we still know
        // that one sample occurred at min so we can do some interpolation
        if (weight.get(0) > 1 && index < weightSoFar) {
            // there is a single sample at min so we interpolate with less weight
            return weightedAverage(min, weightSoFar - index, mean.get(0), index);
        }

        // if the right-most centroid has more than one sample, we still know
        // that one sample occurred at max so we can do some interpolation
        if (weight.get(n - 1) > 1 && totalWeight - index <= weight.get(n - 1) / 2) {
            return max - (totalWeight - index - 1) / (weight.get(n - 1) / 2 - 1) * (max - mean.get(n - 1));
        }

        // in between extremes we interpolate between centroids
        for (int i = 0; i < n - 1; i++) {
            double dw = (weight.get(i) + weight.get(i + 1)) / 2;
            if (weightSoFar + dw > index) {
                // centroids i and i+1 bracket our current point
                double z1 = index - weightSoFar;
                double z2 = weightSoFar + dw - index;
                return weightedAverage(mean.get(i), z2, mean.get(i + 1), z1);
            }
            weightSoFar += dw;
        }

        assert weight.get(n - 1) >= 1;
        assert index >= totalWeight - weight.get(n - 1);

        // Interpolate between the last mean and the max.
        double z1 = index - weightSoFar;
        double z2 = weight.get(n - 1) / 2.0 - z1;
        return weightedAverage(mean.get(n - 1), z1, max, z2);
    }

    @Override
    public int centroidCount() {
        mergeNewValues();
        return lastUsedCell;
    }

    @Override
    public Collection<Centroid> centroids() {
        mergeNewValues();

        // we don't actually keep centroid structures around so we have to fake it
        return new AbstractCollection<>() {
            @Override
            public Iterator<Centroid> iterator() {
                return new Iterator<>() {
                    int i = 0;

                    @Override
                    public boolean hasNext() {
                        return i < lastUsedCell;
                    }

                    @Override
                    public Centroid next() {
                        Centroid rc = new Centroid(mean.get(i), (long) weight.get(i));
                        i++;
                        return rc;
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException("Default operation");
                    }
                };
            }

            @Override
            public int size() {
                return lastUsedCell;
            }
        };
    }

    @Override
    public double compression() {
        return publicCompression;
    }

    public ScaleFunction getScaleFunction() {
        return scale;
    }

    @Override
    public void setScaleFunction(ScaleFunction scaleFunction) {
        super.setScaleFunction(scaleFunction);
    }

    @Override
    public int byteSize() {
        return 48 + 8 * (mean.size() + weight.size() + tempMean.size() + tempWeight.size()) + 4 * order.size();
    }

    @Override
    public String toString() {
        return "MergingDigest"
            + "-"
            + getScaleFunction()
            + "-"
            + (useWeightLimit ? "weight" : "kSize")
            + "-"
            + (useAlternatingSort ? "alternating" : "stable")
            + "-"
            + (useTwoLevelCompression ? "twoLevel" : "oneLevel");
    }

    @Override
    public void close() {
        if (closed == false) {
            closed = true;
            arrays.adjustBreaker(-SHALLOW_SIZE);
            Releasables.close(weight, mean, tempWeight, tempMean, order);
        }
    }
}
