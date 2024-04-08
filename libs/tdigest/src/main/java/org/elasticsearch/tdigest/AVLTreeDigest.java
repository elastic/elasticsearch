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
import java.util.Collections;
import java.util.Iterator;
import java.util.Random;

import static org.elasticsearch.tdigest.IntAVLTree.NIL;

public class AVLTreeDigest extends AbstractTDigest {
    final Random gen = new Random();
    private final double compression;
    private AVLGroupTree summary;

    private long count = 0; // package private for testing

    // Indicates if a sample has been added after the last compression.
    private boolean needsCompression;

    /**
     * A histogram structure that will record a sketch of a distribution.
     *
     * @param compression How should accuracy be traded for size?  A value of N here will give quantile errors
     *                    almost always less than 3/N with considerably smaller errors expected for extreme
     *                    quantiles.  Conversely, you should expect to track about 5 N centroids for this
     *                    accuracy.
     */
    public AVLTreeDigest(double compression) {
        this.compression = compression;
        summary = new AVLGroupTree();
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
    public int centroidCount() {
        return summary.size();
    }

    @Override
    public void add(double x, long w) {
        checkValue(x);
        needsCompression = true;

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
            assert summary.isEmpty();
            summary.add(x, w);
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
            long sum = summary.headSum(start);
            for (int neighbor = start; neighbor != lastNeighbor; neighbor = summary.next(neighbor)) {
                assert minDistance == Math.abs(summary.mean(neighbor) - x);
                double q = count == 1 ? 0.5 : (sum + (summary.count(neighbor) - 1) / 2.0) / (count - 1);
                double k = 4 * count * q * (1 - q) / compression;

                // this slightly clever selection method improves accuracy with lots of repeated points
                // what it does is sample uniformly from all clusters that have room
                if (summary.count(neighbor) + w <= k) {
                    n++;
                    if (gen.nextDouble() < 1 / n) {
                        closest = neighbor;
                    }
                }
                sum += summary.count(neighbor);
            }

            if (closest == NIL) {
                summary.add(x, w);
            } else {
                // if the nearest point was not unique, then we may not be modifying the first copy
                // which means that ordering can change
                double centroid = summary.mean(closest);
                long count = summary.count(closest);
                centroid = weightedAverage(centroid, count, x, w);
                count += w;
                summary.update(closest, centroid, count);
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
        if (needsCompression == false) {
            return;
        }
        needsCompression = false;

        AVLGroupTree centroids = summary;
        this.summary = new AVLGroupTree();

        final int[] nodes = new int[centroids.size()];
        nodes[0] = centroids.first();
        for (int i = 1; i < nodes.length; ++i) {
            nodes[i] = centroids.next(nodes[i - 1]);
            assert nodes[i] != IntAVLTree.NIL;
        }
        assert centroids.next(nodes[nodes.length - 1]) == IntAVLTree.NIL;

        for (int i = centroids.size() - 1; i > 0; --i) {
            final int other = gen.nextInt(i + 1);
            final int tmp = nodes[other];
            nodes[other] = nodes[i];
            nodes[i] = tmp;
        }

        for (int node : nodes) {
            add(centroids.mean(node), centroids.count(node));
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
        if (values.isEmpty()) {
            return Double.NaN;
        }
        if (values.size() == 1) {
            if (x < values.mean(values.first())) return 0;
            if (x > values.mean(values.first())) return 1;
            return 0.5;
        } else {
            if (x < min) {
                return 0;
            }
            if (Double.compare(x, min) == 0) {
                // we have one or more centroids == x, treat them as one
                // dw will accumulate the weight of all of the centroids at x
                double dw = 0;
                for (Centroid value : values) {
                    if (Double.compare(value.mean(), x) != 0) {
                        break;
                    }
                    dw += value.count();
                }
                return dw / 2.0 / size();
            }

            if (x > max) {
                return 1;
            }
            if (Double.compare(x, max) == 0) {
                int ix = values.last();
                double dw = 0;
                while (ix != NIL && Double.compare(values.mean(ix), x) == 0) {
                    dw += values.count(ix);
                    ix = values.prev(ix);
                }
                long n = size();
                return (n - dw / 2.0) / n;
            }

            // we scan a across the centroids
            Iterator<Centroid> it = values.iterator();
            Centroid a = it.next();

            // b is the look-ahead to the next centroid
            Centroid b = it.next();

            // initially, we set left width equal to right width
            double left = (b.mean() - a.mean()) / 2;
            double right = left;

            // scan to next to last element
            double r = 0;
            while (it.hasNext()) {
                if (x < a.mean() + right) {
                    double value = (r + a.count() * interpolate(x, a.mean() - left, a.mean() + right)) / count;
                    return Math.max(value, 0.0);
                }

                r += a.count();
                a = b;
                left = right;
                b = it.next();
                right = (b.mean() - a.mean()) / 2;
            }

            // for the last element, assume right width is same as left
            if (x < a.mean() + right) {
                return (r + a.count() * interpolate(x, a.mean() - right, a.mean() + right)) / count;
            }
            return 1;
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
        if (values.isEmpty()) {
            // no centroids means no data, no way to get a quantile
            return Double.NaN;
        } else if (values.size() == 1) {
            // with one data point, all quantiles lead to Rome
            return values.iterator().next().mean();
        }

        // if values were stored in a sorted array, index would be the offset we are interested in
        final double index = q * count;

        // deal with min and max as a special case singletons
        if (index <= 0) {
            return min;
        }

        if (index >= count) {
            return max;
        }

        int currentNode = values.first();
        long currentWeight = values.count(currentNode);

        // Total mass to the left of the center of the current node.
        double weightSoFar = currentWeight / 2.0;

        if (index <= weightSoFar && weightSoFar > 1) {
            // Interpolate between min and first mean, if there's no singleton on the left boundary.
            return weightedAverage(min, weightSoFar - index, values.mean(currentNode), index);
        }

        for (int i = 0; i < values.size() - 1; i++) {
            int nextNode = values.next(currentNode);
            long nextWeight = values.count(nextNode);
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

        // Interpolate between the last mean and the max.
        double w1 = index - weightSoFar;
        double w2 = currentWeight / 2.0 - w1;
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
        return 64 + summary.size() * 13;
    }
}
