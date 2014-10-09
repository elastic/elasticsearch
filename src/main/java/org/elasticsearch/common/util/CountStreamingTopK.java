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

package org.elasticsearch.common.util;

import com.carrotsearch.hppc.LongIntOpenHashMap;

import java.util.Arrays;


/**
 * A {@link StreamingTopK} implementation that computes top terms, sorted by count.
 * This implementation is based on the space-saving algorithm described in
 * https://icmi.cs.ucsb.edu/research/tech_reports/reports/2005-23.pdf.
 */
public class CountStreamingTopK extends StreamingTopK {

    // a priority queue that keeps bucket ordered by count
    static class PriorityQueue {

        final int size;
        final int[] buckets;       // index in the pq -> bucket
        final int[] bucketToIndex; // bucket -> index in the pq
        final long[] counts;       // bucket -> count
        int maxBucket;

        PriorityQueue(int size) {
            this.size = size;
            buckets = new int[1 + size];
            bucketToIndex = new int[1 + size];
            Arrays.fill(buckets, size);
            counts = new long[1 + size];
        }

        public int size() {
            return size;
        }

        /** Return one greater than the largest allocated bucket so far. */
        public int maxBucket() {
            return maxBucket;
        }

        /** Return the bucket id for the given offset, or {@link #size()} if the bucket is not allocated yet. */
        public int bucket(int i) {
            return buckets[i + 1];
        }

        public long count(int bucket) {
            return counts[bucket];
        }

        private void swap(int i, int j) {
            final int bucketJ = buckets[i];
            final int bucketI = buckets[j];
            buckets[i] = bucketI;
            buckets[j] = bucketJ;
            // keep the reverse mapping up-to-date
            bucketToIndex[bucketI] = i;
            bucketToIndex[bucketJ] = j;
        }

        /**
         * Add one to the term stored in the given bucket.
         */
        public void add(int bucket) {
            assert bucket < size;
            int node = bucketToIndex[bucket];
            final long nodeCount = counts[bucket] + 1;
            counts[bucket] = nodeCount;
            while (true) {
                int left = node << 1;
                int right = left + 1;

                if (left > size) {
                    break;
                }

                int leastChild = left;
                long leastChildCount = counts[buckets[left]];
                if (right <= size) {
                    final long rightChildCount = counts[buckets[right]];
                    if (rightChildCount < leastChildCount) {
                        leastChild = right;
                        leastChildCount = rightChildCount;
                    }
                }

                if (nodeCount > leastChildCount) {
                    swap(node, leastChild);
                    node = leastChild;
                } else {
                    break;
                }
            }
        }

        public int topBucket() {
            int bucket = buckets[1];
            if (bucket == size) { // we use size as a sentinel, means not allocated
                bucket = buckets[1] = maxBucket++;
                bucketToIndex[bucket] = 1;
            }
            return bucket;
        }

    }

    private final LongIntOpenHashMap termToBucket; // term -> bucket
    private final long[] terms;                    // bucket -> term
    private final PriorityQueue counts;

    public CountStreamingTopK(int size) {
        termToBucket = new LongIntOpenHashMap(size);
        counts = new PriorityQueue(size);
        terms = new long[size];
    }

    @Override
    public Status add(long term, Status status) {
        if (termToBucket.containsKey(term)) {
            // already tracked
            final int bucket = termToBucket.lget();
            counts.add(bucket);
            return status.reset(bucket, false);
        }
        // not tracked yet, let's replace the least entry
        final int bucket = counts.topBucket();
        if (counts.count(bucket) > 0) {
            termToBucket.remove(terms[bucket]);
        }
        terms[bucket] = term;
        termToBucket.put(term, bucket);
        assert termToBucket.size() == counts.maxBucket();
        counts.add(bucket);
        return status.reset(bucket, true);
    }

    @Override
    public long[] topTerms() {
        return Arrays.copyOf(terms, counts.maxBucket());
    }

}
