/*
 * Licensed to Ted Dunning under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.tdigest;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static org.elasticsearch.tdigest.IntAVLTree.NIL;

public class AVLTreeDigest extends AbstractTDigest {
    final Random gen = new Random();
    private final double compression;
    private AVLGroupTree summary;

    private long count = 0; // package private for testing

    /**
     * A histogram structure that will record a sketch of a distribution.
     *
     * @param compression How should accuracy be traded for size?  A value of N here will give quantile errors
     *                    almost always less than 3/N with considerably smaller errors expected for extreme
     *                    quantiles.  Conversely, you should expect to track about 5 N centroids for this
     *                    accuracy.
     */
    @SuppressWarnings("WeakerAccess")
    public AVLTreeDigest(double compression) {
        this.compression = compression;
        summary = new AVLGroupTree(false);
    }

    /**
     * Sets the seed for the RNG.
     * In cases where a predicatable tree should be created, this function may be used to make the
     * randomness in this AVLTree become more deterministic.
     *
     * @param seed The random seed to use for RNG purposes
     */
    public void setRandomSeed(long seed) {
        gen.setSeed(seed);
    }

    @Override
    public TDigest recordAllData() {
        if (summary.size() != 0) {
            throw new IllegalStateException("Can only ask to record added data on an empty summary");
        }
        summary = new AVLGroupTree(true);
        return super.recordAllData();
    }

    @Override
    public int centroidCount() {
        return summary.size();
    }

    @Override
    void add(double x, int w, Centroid base) {
        if (x != base.mean() || w != base.count()) {
            throw new IllegalArgumentException();
        }
        add(x, w, base.data());
    }

    @Override
    public void add(double x, int w) {
        add(x, w, (List<Double>) null);
    }

    @Override
    public void add(List<? extends TDigest> others) {
        for (TDigest other : others) {
            setMinMax(Math.min(min, other.getMin()), Math.max(max, other.getMax()));
            for (Centroid centroid : other.centroids()) {
                add(centroid.mean(), centroid.count(), recordAllData ? centroid.data() : null);
            }
        }
    }

    public void add(double x, int w, List<Double> data) {
        checkValue(x);
        if (x < min) {
            min = x;
        }
        if (x > max) {
            max = x;
        }
        int start = summary.floor(x);
        if (start == NIL) {
            start = summary.first();
        }

        if (start == NIL) { // empty summary
            assert summary.size() == 0;
            summary.add(x, w, data);
            count = w;
        } else {
            double minDistance = Double.MAX_VALUE;
            int lastNeighbor = NIL;
            for (int neighbor = start; neighbor != NIL; neighbor = summary.next(neighbor)) {
                double z = Math.abs(summary.mean(neighbor) - x);
                if (z < minDistance) {
                    start = neighbor;
                    minDistance = z;
                } else if (z > minDistance) {
                    // as soon as z increases, we have passed the nearest neighbor and can quit
                    lastNeighbor = neighbor;
                    break;
                }
            }

            int closest = NIL;
            double n = 0;
            for (int neighbor = start; neighbor != lastNeighbor; neighbor = summary.next(neighbor)) {
                assert minDistance == Math.abs(summary.mean(neighbor) - x);
                double q0 = (double) summary.headSum(neighbor) / count;
                double q1 = q0 + (double) summary.count(neighbor) / count;
                double k = count * Math.min(scale.max(q0, compression, count), scale.max(q1, compression, count));

                // this slightly clever selection method improves accuracy with lots of repeated points
                // what it does is sample uniformly from all clusters that have room
                if (summary.count(neighbor) + w <= k) {
                    n++;
                    if (gen.nextDouble() < 1 / n) {
                        closest = neighbor;
                    }
                }
            }

            if (closest == NIL) {
                summary.add(x, w, data);
            } else {
                // if the nearest point was not unique, then we may not be modifying the first copy
                // which means that ordering can change
                double centroid = summary.mean(closest);
                int count = summary.count(closest);
                List<Double> d = summary.data(closest);
                if (d != null) {
                    if (w == 1) {
                        d.add(x);
                    } else {
                        d.addAll(data);
                    }
                }
                centroid = weightedAverage(centroid, count, x, w);
                count += w;
                summary.update(closest, centroid, count, d, false);
            }
            count += w;

            if (summary.size() > 20 * compression) {
                // may happen in case of sequential points
                compress();
            }
        }
    }

    @Override
    public void compress() {
        if (summary.size() <= 1) {
            return;
        }

        double n0 = 0;
        double k0 = count * scale.max(n0 / count, compression, count);
        int node = summary.first();
        int w0 = summary.count(node);
        double n1 = n0 + summary.count(node);

        int w1 = 0;
        double k1;
        while (node != NIL) {
            int after = summary.next(node);
            while (after != NIL) {
                w1 = summary.count(after);
                k1 = count * scale.max((n1 + w1) / count, compression, count);
                if (w0 + w1 > Math.min(k0, k1)) {
                    break;
                } else {
                    double mean = weightedAverage(summary.mean(node), w0, summary.mean(after), w1);
                    List<Double> d1 = summary.data(node);
                    List<Double> d2 = summary.data(after);
                    if (d1 != null && d2 != null) {
                        d1.addAll(d2);
                    }
                    summary.update(node, mean, w0 + w1, d1, true);

                    int tmp = summary.next(after);
                    summary.remove(after);
                    after = tmp;
                    n1 += w1;
                    w0 += w1;
                }
            }
            node = after;
            if (node != NIL) {
                n0 = n1;
                k0 = count * scale.max(n0 / count, compression, count);
                w0 = w1;
                n1 = n0 + w0;
            }
        }
    }

    /**
     * Returns the number of samples represented in this histogram.  If you want to know how many
     * centroids are being used, try centroids().size().
     *
     * @return the number of samples that have been added.
     */
    @Override
    public long size() {
        return count;
    }

    /**
     * @param x the value at which the CDF should be evaluated
     * @return the approximate fraction of all samples that were less than or equal to x.
     */
    @Override
    public double cdf(double x) {
        AVLGroupTree values = summary;
        if (values.size() == 0) {
            return Double.NaN;
        } else if (values.size() == 1) {
            if (x < values.mean(values.first())) return 0;
            else if (x > values.mean(values.first())) return 1;
            else return 0.5;
        } else {
            if (x < min) {
                return 0;
            } else if (x == min) {
                // we have one or more centroids == x, treat them as one
                // dw will accumulate the weight of all of the centroids at x
                double dw = 0;
                for (Centroid value : values) {
                    if (value.mean() != x) {
                        break;
                    }
                    dw += value.count();
                }
                return dw / 2.0 / size();
            }
            assert x > min;

            if (x > max) {
                return 1;
            } else if (x == max) {
                int ix = values.last();
                double dw = 0;
                while (ix != NIL && values.mean(ix) == x) {
                    dw += values.count(ix);
                    ix = values.prev(ix);
                }
                long n = size();
                return (n - dw / 2.0) / n;
            }
            assert x < max;

            int first = values.first();
            double firstMean = values.mean(first);
            if (x > min && x < firstMean) {
                return interpolateTail(values, x, first, firstMean, min);
            }

            int last = values.last();
            double lastMean = values.mean(last);
            if (x < max && x > lastMean) {
                return 1 - interpolateTail(values, x, last, lastMean, max);
            }
            assert values.size() >= 2;
            assert x >= firstMean;
            assert x <= lastMean;

            // we scan a across the centroids
            Iterator<Centroid> it = values.iterator();
            Centroid a = it.next();
            double aMean = a.mean();
            double aWeight = a.count();

            if (x == aMean) {
                return aWeight / 2.0 / size();
            }
            assert x > aMean;

            // b is the look-ahead to the next centroid
            Centroid b = it.next();
            double bMean = b.mean();
            double bWeight = b.count();

            assert bMean >= aMean;

            double weightSoFar = 0;

            // scan to last element
            while (bWeight > 0) {
                assert x > aMean;
                if (x == bMean) {
                    assert bMean > aMean;
                    weightSoFar += aWeight;
                    while (it.hasNext()) {
                        b = it.next();
                        if (x == b.mean()) {
                            bWeight += b.count();
                        } else {
                            break;
                        }
                    }
                    return (weightSoFar + bWeight / 2.0) / size();
                }
                assert x < bMean || x > bMean;

                if (x < bMean) {
                    // we are strictly between a and b
                    assert aMean < bMean;
                    if (aWeight == 1) {
                        // but a might be a singleton
                        if (bWeight == 1) {
                            // we have passed all of a, but none of b, no interpolation
                            return (weightSoFar + 1.0) / size();
                        } else {
                            // only get to interpolate b's weight because a is a singleton and to our left
                            double partialWeight = (x - aMean) / (bMean - aMean) * bWeight / 2.0;
                            return (weightSoFar + 1.0 + partialWeight) / size();
                        }
                    } else if (bWeight == 1) {
                        // only get to interpolate a's weight because b is a singleton
                        double partialWeight = (x - aMean) / (bMean - aMean) * aWeight / 2.0;
                        // half of a is to left of aMean, and half is interpolated
                        return (weightSoFar + aWeight / 2.0 + partialWeight) / size();
                    } else {
                        // neither is singleton
                        double partialWeight = (x - aMean) / (bMean - aMean) * (aWeight + bWeight) / 2.0;
                        return (weightSoFar + aWeight / 2.0 + partialWeight) / size();
                    }
                }
                weightSoFar += aWeight;

                assert x > bMean;

                if (it.hasNext()) {
                    aMean = bMean;
                    aWeight = bWeight;

                    b = it.next();
                    bMean = b.mean();
                    bWeight = b.count();

                    assert bMean >= aMean;
                } else {
                    bWeight = 0;
                }
            }
            // shouldn't be possible because x <= lastMean
            throw new IllegalStateException("Ran out of centroids");
        }
    }

    private double interpolateTail(AVLGroupTree values, double x, int node, double mean, double extremeValue) {
        int count = values.count(node);
        assert count > 1;
        if (count == 2) {
            // other sample must be on the other side of the mean
            return 1.0 / size();
        } else {
            // how much weight is available for interpolation?
            double weight = count / 2.0 - 1;
            // how much is between min and here?
            double partialWeight = (extremeValue - x) / (extremeValue - mean) * weight;
            // account for sample at min along with interpolated weight
            return (partialWeight + 1.0) / size();
        }
    }

    /**
     * @param q The quantile desired.  Can be in the range [0,1].
     * @return The minimum value x such that we think that the proportion of samples is &le; x is q.
     */
    @Override
    public double quantile(double q) {
        if (q < 0 || q > 1) {
            throw new IllegalArgumentException("q should be in [0,1], got " + q);
        }

        AVLGroupTree values = summary;
        if (values.size() == 0) {
            // no centroids means no data, no way to get a quantile
            return Double.NaN;
        } else if (values.size() == 1) {
            // with one data point, all quantiles lead to Rome
            return values.iterator().next().mean();
        }

        // if values were stored in a sorted array, index would be the offset we are interested in
        final double index = q * (count - 1);

        // deal with min and max as a special case singletons
        if (index <= 0) {
            return min;
        }

        if (index >= count - 1) {
            return max;
        }

        int currentNode = values.first();
        int currentWeight = values.count(currentNode);

        if (currentWeight == 2 && index <= 2) {
            // first node is a doublet with one sample at min
            // so we can infer location of other sample
            return 2 * values.mean(currentNode) - min;
        }

        if (values.count(values.last()) == 2 && index > count - 2) {
            // likewise for last centroid
            return 2 * values.mean(values.last()) - max;
        }

        // weightSoFar represents the total mass to the left of the center of the current node.
        // If there's a singleton on the left boundary, there's no more mass on the left.
        double weightSoFar = (currentWeight == 1) ? 0 : currentWeight / 2.0;

        if (index <= weightSoFar || (currentWeight == 1 && index <= currentWeight)) {
            // If there's a singleton on the left boundary, interpolate between the first two centroids.
            if (currentWeight == 1) {
                return min + index * (values.mean(values.next(currentNode)) - min);
            }
            // Interpolate between min and first mean, otherwise.
            return min + index * (values.mean(currentNode) - min);
        }

        for (int i = 0; i < values.size() - 1; i++) {
            int nextNode = values.next(currentNode);
            int nextWeight = values.count(nextNode);
            // this is the mass between current center and next center
            double dw = (currentWeight + nextWeight) / 2.0;

            if (index < weightSoFar + dw) {
                // index is bracketed between centroids i and i+1
                assert dw >= 1;

                double w1 = index - weightSoFar;
                double w2 = weightSoFar + dw - index;
                return weightedAverage(values.mean(currentNode), w2, values.mean(nextNode), w1);
            }
            weightSoFar += dw;
            currentNode = nextNode;
            currentWeight = nextWeight;
        }

        // Index is close or after the last centroid.
        assert currentWeight >= 1;
        assert index - weightSoFar < count - currentWeight / 2.0;
        assert count - weightSoFar >= 0.5;

        double w1 = index - weightSoFar;
        double w2 = count - weightSoFar - w1;

        // If there's a singleton on the right boundary, interpolate with the previous mean.
        if (currentWeight == 1) {
            return weightedAverage(values.mean(values.prev(currentNode)), w2, values.mean(currentNode), w1);
        }
        // Interpolate between the last mean and the max.
        return weightedAverage(values.mean(currentNode), w2, max, w1);
    }

    @Override
    public Collection<Centroid> centroids() {
        return Collections.unmodifiableCollection(summary);
    }

    @Override
    public double compression() {
        return compression;
    }

    /**
     * Returns an upper bound on the number bytes that will be required to represent this histogram.
     */
    @Override
    public int byteSize() {
        compress();
        return 32 + summary.size() * 12;
    }

    /**
     * Returns an upper bound on the number of bytes that will be required to represent this histogram in
     * the tighter representation.
     */
    @Override
    public int smallByteSize() {
        int bound = byteSize();
        ByteBuffer buf = ByteBuffer.allocate(bound);
        asSmallBytes(buf);
        return buf.position();
    }

    private static final int VERBOSE_ENCODING = 1;
    private static final int SMALL_ENCODING = 2;

    /**
     * Outputs a histogram as bytes using a particularly cheesy encoding.
     */
    @Override
    public void asBytes(ByteBuffer buf) {
        buf.putInt(VERBOSE_ENCODING);
        buf.putDouble(min);
        buf.putDouble(max);
        buf.putDouble((float) compression());
        buf.putInt(summary.size());
        for (Centroid centroid : summary) {
            buf.putDouble(centroid.mean());
        }

        for (Centroid centroid : summary) {
            buf.putInt(centroid.count());
        }
    }

    @Override
    public void asSmallBytes(ByteBuffer buf) {
        buf.putInt(SMALL_ENCODING);
        buf.putDouble(min);
        buf.putDouble(max);
        buf.putDouble(compression());
        buf.putInt(summary.size());

        double x = 0;
        for (Centroid centroid : summary) {
            double delta = centroid.mean() - x;
            x = centroid.mean();
            buf.putFloat((float) delta);
        }

        for (Centroid centroid : summary) {
            int n = centroid.count();
            encode(buf, n);
        }
    }

    /**
     * Reads a histogram from a byte buffer
     *
     * @param buf The buffer to read from.
     * @return The new histogram structure
     */
    @SuppressWarnings("WeakerAccess")
    public static AVLTreeDigest fromBytes(ByteBuffer buf) {
        int encoding = buf.getInt();
        if (encoding == VERBOSE_ENCODING) {
            double min = buf.getDouble();
            double max = buf.getDouble();
            double compression = buf.getDouble();
            AVLTreeDigest r = new AVLTreeDigest(compression);
            r.setMinMax(min, max);
            int n = buf.getInt();
            double[] means = new double[n];
            for (int i = 0; i < n; i++) {
                means[i] = buf.getDouble();
            }
            for (int i = 0; i < n; i++) {
                r.add(means[i], buf.getInt());
            }
            return r;
        } else if (encoding == SMALL_ENCODING) {
            double min = buf.getDouble();
            double max = buf.getDouble();
            double compression = buf.getDouble();
            AVLTreeDigest r = new AVLTreeDigest(compression);
            r.setMinMax(min, max);
            int n = buf.getInt();
            double[] means = new double[n];
            double x = 0;
            for (int i = 0; i < n; i++) {
                double delta = buf.getFloat();
                x += delta;
                means[i] = x;
            }

            for (int i = 0; i < n; i++) {
                int z = decode(buf);
                r.add(means[i], z);
            }
            return r;
        } else {
            throw new IllegalStateException("Invalid format for serialized histogram");
        }
    }

}
