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
import java.util.List;
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
    public void add(List<? extends TDigest> others) {
        for (TDigest other : others) {
            setMinMax(Math.min(min, other.getMin()), Math.max(max, other.getMax()));
            for (Centroid centroid : other.centroids()) {
                add(centroid.mean(), centroid.count());
            }
        }
    }

    @Override
    public void add(double x, int w) {
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
            assert summary.size() == 0;
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
                int count = summary.count(closest);
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
                if (x == bMean) {
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

                if (x < bMean) {
                    // we are strictly between a and b
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
        final double index = q * count;

        // deal with min and max as a special case singletons
        if (index <= 0) {
            return min;
        }

        if (index >= count) {
            return max;
        }

        int currentNode = values.first();
        int currentWeight = values.count(currentNode);

        // Total mass to the left of the center of the current node.
        double weightSoFar = currentWeight / 2.0;

        if (index <= weightSoFar && weightSoFar > 1) {
            // Interpolate between min and first mean, if there's no singleton on the left boundary.
            return weightedAverage(min, weightSoFar - index, values.mean(currentNode), index);
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
}
