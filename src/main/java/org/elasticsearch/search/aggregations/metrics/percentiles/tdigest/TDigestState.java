/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements. See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.elasticsearch.search.aggregations.metrics.percentiles.tdigest;

import com.carrotsearch.hppc.cursors.IntCursor;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.metrics.percentiles.tdigest.GroupRedBlackTree.SizeAndSum;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 * Fork of https://github.com/tdunning/t-digest/blob/master/src/main/java/com/tdunning/math/stats/TDigest.java
 * Modified for less object allocation, faster estimations and integration with Elasticsearch serialization.
 */
public class TDigestState {

    private final Random gen;
    private double compression = 100;
    private GroupRedBlackTree summary;
    private long count = 0;
    private final SizeAndSum sizeAndSum = new SizeAndSum();

    /**
     * A histogram structure that will record a sketch of a distribution.
     *
     * @param compression How should accuracy be traded for size?  A value of N here will give quantile errors
     *                    almost always less than 3/N with considerably smaller errors expected for extreme
     *                    quantiles.  Conversely, you should expect to track about 5 N centroids for this
     *                    accuracy.
     */
    public TDigestState(double compression) {
        this.compression = compression;
        gen = new Random();
        summary = new GroupRedBlackTree((int) compression);
    }

    /**
     * Adds a sample to a histogram.
     *
     * @param x The value to add.
     */
    public void add(double x) {
        add(x, 1);
    }

    /**
     * Adds a sample to a histogram.
     *
     * @param x The value to add.
     * @param w The weight of this point.
     */
    public void add(double x, long w) {
        int startNode = summary.floorNode(x);
        if (startNode == RedBlackTree.NIL) {
            startNode = summary.ceilingNode(x);
        }

        if (startNode == RedBlackTree.NIL) {
            summary.addGroup(x, w);
            count = w;
        } else {
            double minDistance = Double.POSITIVE_INFINITY;
            int lastNeighbor = 0;
            summary.headSum(startNode, sizeAndSum);
            final int headSize = sizeAndSum.size;
            int i = headSize;
            for (int node = startNode; node != RedBlackTree.NIL; node = summary.nextNode(node)) {
                double z = Math.abs(summary.mean(node) - x);
                if (z <= minDistance) {
                    minDistance = z;
                    lastNeighbor = i;
                } else {
                    break;
                }
                i++;
            }

            int closest = RedBlackTree.NIL;
            long sum = sizeAndSum.sum;
            i = headSize;
            double n = 1;
            for (int node = startNode; node != RedBlackTree.NIL; node = summary.nextNode(node)) {
                if (i > lastNeighbor) {
                    break;
                }
                double z = Math.abs(summary.mean(node) - x);
                double q = (sum + summary.count(node) / 2.0) / count;
                double k = 4 * count * q * (1 - q) / compression;

                // this slightly clever selection method improves accuracy with lots of repeated points
                if (z == minDistance && summary.count(node) + w <= k) {
                    if (gen.nextDouble() < 1 / n) {
                        closest = node;
                    }
                    n++;
                }
                sum += summary.count(node);
                i++;
            }

            if (closest == RedBlackTree.NIL) {
                summary.addGroup(x, w);
            } else {
                double centroid = summary.mean(closest);
                long count = summary.count(closest);
                count += w;
                centroid += w * (x - centroid) / count;
                summary.updateGroup(closest, centroid, count);
            }
            count += w;

            if (summary.size() > 100 * compression) {
                // something such as sequential ordering of data points
                // has caused a pathological expansion of our summary.
                // To fight this, we simply replay the current centroids
                // in random order.

                // this causes us to forget the diagnostic recording of data points
                compress();
            }
        }
    }

    private int[] shuffleNodes(RedBlackTree tree) {
        int[] nodes = new int[tree.size()];
        int i = 0;
        for (IntCursor cursor : tree) {
            nodes[i++] = cursor.value;
        }
        assert i == tree.size();
        for (i = tree.size() - 1; i > 0; --i) {
            final int slot = gen.nextInt(i + 1);
            final int tmp = nodes[slot];
            nodes[slot] = nodes[i];
            nodes[i] = tmp;
        }
        return nodes;
    }

    public void add(TDigestState other) {
        final int[] shuffledNodes = shuffleNodes(other.summary);
        for (int node : shuffledNodes) {
            add(other.summary.mean(node), other.summary.count(node));
        }
    }

    public static TDigestState merge(double compression, Iterable<TDigestState> subData) {
        Preconditions.checkArgument(subData.iterator().hasNext(), "Can't merge 0 digests");
        List<TDigestState> elements = Lists.newArrayList(subData);
        int n = Math.max(1, elements.size() / 4);
        TDigestState r = new TDigestState(compression);
        for (int i = 0; i < elements.size(); i += n) {
            if (n > 1) {
                r.add(merge(compression, elements.subList(i, Math.min(i + n, elements.size()))));
            } else {
                r.add(elements.get(i));
            }
        }
        return r;
    }

    public void compress() {
        compress(summary);
    }

    private void compress(GroupRedBlackTree other) {
        TDigestState reduced = new TDigestState(compression);
        final int[] shuffledNodes = shuffleNodes(other);
        for (int node : shuffledNodes) {
            reduced.add(other.mean(node), other.count(node));
        }
        summary = reduced.summary;
    }

    /**
     * Returns the number of samples represented in this histogram.  If you want to know how many
     * centroids are being used, try centroids().size().
     *
     * @return the number of samples that have been added.
     */
    public long size() {
        return count;
    }

    public GroupRedBlackTree centroids() {
        return summary;
    }

    /**
     * @param x the value at which the CDF should be evaluated
     * @return the approximate fraction of all samples that were less than or equal to x.
     */
    public double cdf(double x) {
        GroupRedBlackTree values = summary;
        if (values.size() == 0) {
            return Double.NaN;
        } else if (values.size() == 1) {
            return x < values.mean(values.root()) ? 0 : 1;
        } else {
            double r = 0;

            // we scan a across the centroids
            Iterator<IntCursor> it = values.iterator();
            int a = it.next().value;

            // b is the look-ahead to the next centroid
            int b = it.next().value;

            // initially, we set left width equal to right width
            double left = (values.mean(b) - values.mean(a)) / 2;
            double right = left;

            // scan to next to last element
            while (it.hasNext()) {
                if (x < values.mean(a) + right) {
                    return (r + values.count(a) * interpolate(x, values.mean(a) - left, values.mean(a) + right)) / count;
                }
                r += values.count(a);

                a = b;
                b = it.next().value;

                left = right;
                right = (values.mean(b) - values.mean(a)) / 2;
            }

            // for the last element, assume right width is same as left
            left = right;
            a = b;
            if (x < values.mean(a) + right) {
                return (r + values.count(a) * interpolate(x, values.mean(a) - left, values.mean(a) + right)) / count;
            } else {
                return 1;
            }
        }
    }

    /**
     * @param q The quantile desired.  Can be in the range [0,1].
     * @return The minimum value x such that we think that the proportion of samples is <= x is q.
     */
    public double quantile(double q) {
        if (q < 0 || q > 1) {
            throw new ElasticsearchIllegalArgumentException("q should be in [0,1], got " + q);
        }
        GroupRedBlackTree values = summary;
        if (values.size() == 0) {
            return Double.NaN;
        } else if (values.size() == 1) {
            return values.mean(values.root());
        }

        // if values were stored in a sorted array, index would be the offset we are interested in
        final double index = q * (count - 1);

        double previousMean = Double.NaN, previousIndex = 0;
        long total = 0;
        int next;
        Iterator<IntCursor> it = centroids().iterator();
        while (true) {
            next = it.next().value;
            final double nextIndex = total + (values.count(next) - 1.0) / 2;
            if (nextIndex >= index) {
                if (Double.isNaN(previousMean)) {
                    // special case 1: the index we are interested in is before the 1st centroid
                    if (nextIndex == previousIndex) {
                        return values.mean(next);
                    }
                    // assume values grow linearly between index previousIndex=0 and nextIndex2
                    int next2 = it.next().value;
                    final double nextIndex2 = total + values.count(next) + (values.count(next2) - 1.0) / 2;
                    previousMean = (nextIndex2 * values.mean(next) - nextIndex * values.mean(next2)) / (nextIndex2 - nextIndex);
                }
                // common case: we found two centroids previous and next so that the desired quantile is
                // after 'previous' but before 'next'
                return quantile(previousIndex, index, nextIndex, previousMean, values.mean(next));
            } else if (!it.hasNext()) {
                // special case 2: the index we are interested in is beyond the last centroid
                // again, assume values grow linearly between index previousIndex and (count - 1)
                // which is the highest possible index
                final double nextIndex2 = count - 1;
                final double nextMean2 = (values.mean(next) * (nextIndex2 - previousIndex) - previousMean * (nextIndex2 - nextIndex)) / (nextIndex - previousIndex);
                return quantile(nextIndex, index, nextIndex2, values.mean(next), nextMean2);
            }
            total += values.count(next);
            previousMean = values.mean(next);
            previousIndex = nextIndex;
        }
    }

    private static double quantile(double previousIndex, double index, double nextIndex, double previousMean, double nextMean) {
        final double delta = nextIndex - previousIndex;
        final double previousWeight = (nextIndex - index) / delta;
        final double nextWeight = (index - previousIndex) / delta;
        return previousMean * previousWeight + nextMean * nextWeight;
    }

    public int centroidCount() {
        return summary.size();
    }

    public double compression() {
        return compression;
    }

    private double interpolate(double x, double x0, double x1) {
        return (x - x0) / (x1 - x0);
    }

    //===== elastic search serialization ======//

    public static void write(TDigestState state, StreamOutput out) throws IOException {
        out.writeDouble(state.compression);
        out.writeVInt(state.summary.size());
        for (IntCursor cursor : state.summary) {
            final int node = cursor.value;
            out.writeDouble(state.summary.mean(node));
            out.writeVLong(state.summary.count(node));
        }
    }

    public static TDigestState read(StreamInput in) throws IOException {
        double compression = in.readDouble();
        TDigestState state = new TDigestState(compression);
        int n = in.readVInt();
        for (int i = 0; i < n; i++) {
            state.add(in.readDouble(), in.readVInt());
        }
        return state;
    }
}
