/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.metrics.percentiles.tdigest;

import com.google.common.primitives.Longs;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;

import java.util.Arrays;

/** Specialized {@link RedBlackTree} where each node stores aggregated data for its sub-trees. */
public class GroupRedBlackTree extends RedBlackTree {

    private long nextId;
    private double[] centroids;
    private long[] counts;
    private int[] aggregatedSizes;
    private long[] aggregatedCounts;
    private long[] ids;

    // used for comparisons
    double tmpCentroid;
    long tmpCount;
    long tmpId;

    public GroupRedBlackTree(int capacity) {
        super(capacity);
        nextId = 1;
        centroids = new double[1+capacity];
        aggregatedSizes = new int[1+capacity];
        counts = new long[1+capacity];
        aggregatedCounts = new long[1+capacity];
        ids = new long[1+capacity];
    }

    // Internal node management

    @Override
    protected int compare(int node) {
        final double centroid = mean(node);
        int cmp = Double.compare(tmpCentroid, centroid);
        if (cmp == 0) {
            cmp = Long.compare(tmpId, ids[node]);
        }
        return cmp;
    }

    @Override
    protected void copy(int node) {
        centroids[node] = tmpCentroid;
        counts[node] = tmpCount;
        ids[node] = tmpId;
    }

    @Override
    protected void merge(int node) {
        throw new IllegalStateException("compare should never return 0");
    }

    @Override
    protected void swap(int node1, int node2) {
        super.swap(node1, node2);
        for (int n = node1; n != NIL; n = parent(n)) {
            fixAggregates(n);
        }
    }

    @Override
    protected int newNode() {
        final int newNode = super.newNode();
        if (newNode >= centroids.length) {
            final int minSize = ArrayUtil.oversize(newNode + 1, RamUsageEstimator.NUM_BYTES_INT);
            centroids = Arrays.copyOf(centroids, minSize);
            counts = Arrays.copyOf(counts, minSize);
            aggregatedSizes = Arrays.copyOf(aggregatedSizes, minSize);
            aggregatedCounts = Arrays.copyOf(aggregatedCounts, minSize);
            ids = Arrays.copyOf(ids, minSize);
        }
        return newNode;
    }

    private void fixAggregates(int node) {
        final int left = left(node), right = right(node);
        aggregatedCounts[node] = counts[node] + aggregatedCounts[left] + aggregatedCounts[right];
        aggregatedSizes[node] = 1 + aggregatedSizes[left] + aggregatedSizes[right];
    }

    private void fixCounts(int node) {
        final int left = left(node), right = right(node);
        aggregatedCounts[node] = counts[node] + aggregatedCounts[left] + aggregatedCounts[right];
    }

    @Override
    protected void rotateLeft(int node) {
        super.rotateLeft(node);
        fixAggregates(node);
        fixAggregates(parent(node));
    }

    @Override
    protected void rotateRight(int node) {
        super.rotateRight(node);
        fixAggregates(node);
        fixAggregates(parent(node));
    }

    @Override
    protected void beforeRemoval(int node) {
        final long count = count(node);
        for (int n = node; n != NIL; n = parent(n)) {
            aggregatedCounts[n] -= count;
            aggregatedSizes[n]--;
        }
        super.beforeRemoval(node);
    }

    @Override
    protected void afterInsertion(int node) {
        final long count = count(node);
        aggregatedCounts[node] = count;
        aggregatedSizes[node] = 1;
        for (int n = node, p = parent(node); p != NIL; n = p, p = parent(n)) {
            aggregatedCounts[p] += count;
            aggregatedSizes[p] += 1;
        }
        super.afterInsertion(node);
    }

    // Public API

    public double mean(int node) {
        return centroids[node];
    }

    public long count(int node) {
        return counts[node];
    }

    public long id(int node) {
        return ids[node];
    }

    public void addGroup(double centroid, long count, long id) {
        tmpCentroid = centroid;
        tmpCount = count;
        tmpId = id;
        if (id >= nextId) {
            nextId = id + 1;
        }
        addNode();
    }

    public void addGroup(double centroid, long count) {
        addGroup(centroid, count, nextId++);
    }

    public boolean removeGroup(double centroid, int id) {
        tmpCentroid = centroid;
        tmpId = id;
        final int nodeToRemove = getNode();
        if (nodeToRemove != NIL) {
            removeNode(nodeToRemove);
            return true;
        } else {
            return false;
        }
    }

    public void updateGroup(int node, double centroid, long count) {
        tmpCentroid = centroid;
        tmpId = id(node);
        tmpCount = count;
        final int prev = prevNode(node);
        final int next = nextNode(node);
        if ((prev == NIL || compare(prev) > 0) && (next == NIL || compare(next) < 0)) {
            // we can update in place
            copy(node);
            for (int n = node; n != NIL; n = parent(n)) {
                fixCounts(n);
            }
        } else {
            removeNode(node);
            addNode();
        }
    }

    /** Return the last node whose centroid is strictly smaller than <code>centroid</code>. */
    public int floorNode(double centroid) {
        int floor = NIL;
        for (int node = root(); node != NIL; ) {
            final int cmp = Double.compare(centroid, mean(node));
            if (cmp <= 0) {
                node = left(node);
            } else {
                floor = node;
                node = right(node);
            }
        }
        return floor;
    }

    /** Return the first node that is greater than or equal to <code>centroid</code>. */
    public int ceilingNode(double centroid) {
        int ceiling = NIL;
        for (int node = root(); node != NIL; ) {
            final int cmp = Double.compare(mean(node), centroid);
            if (cmp < 0) {
                node = right(node);
            } else {
                ceiling = node;
                node = left(node);
            }
        }
        return ceiling;
    }

    /** Compute the number of elements and sum of counts for every entry that is strictly before <code>node</code>. */
    public void headSum(int node, SizeAndSum sizeAndSum) {
        if (node == NIL) {
            sizeAndSum.size = 0;
            sizeAndSum.sum = 0;
            return;
        }
        final int left = left(node);
        sizeAndSum.size = aggregatedSizes[left];
        sizeAndSum.sum = aggregatedCounts[left];
        for (int n = node, p = parent(node); p != NIL; n = p, p = parent(n)) {
            if (n == right(p)) {
                final int leftP = left(p);
                sizeAndSum.size += 1 + aggregatedSizes[leftP];
                sizeAndSum.sum += counts[p] + aggregatedCounts[leftP];
            }
        }
    }

    /** Wrapper around a size and a sum. */
    public static class SizeAndSum {
        public int size;
        public long sum;
    }

}
