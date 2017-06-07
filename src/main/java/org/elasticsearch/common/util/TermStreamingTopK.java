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
import com.google.common.base.Preconditions;

import java.util.Arrays;

/**
 * A {@link StreamingTopK} implementation that computes top terms, sorted by term.
 */
public class TermStreamingTopK extends StreamingTopK {

    // a priority queue that keeps bucket ordered by term
    static class PriorityQueue {

        final int size;      // max number of elements of the pq
        final long[] terms;  // index in the pq -> term
        final int[] buckets; // index in the pq -> bucket id
        final boolean asc;   
        int maxBucket;

        PriorityQueue(int size, boolean asc) {
            this.size = size;
            terms = new long[1 + size];
            buckets = new int[1 + size];
            Arrays.fill(buckets, -1);
            this.asc = asc;
            if (asc) {
                Arrays.fill(terms, Long.MAX_VALUE);
            } else {
                Arrays.fill(terms, Long.MIN_VALUE);
            }
        }

        public int size() {
            return size;
        }

        public int maxBucket() {
            return maxBucket;
        }

        private void swap(int i, int j) {
            final long tmpTerm = terms[i];
            terms[i] = terms[j];
            terms[j] = tmpTerm;
            final int tmpBucket = buckets[i];
            buckets[i] = buckets[j];
            buckets[j] = tmpBucket;
        }

        public boolean lessThan(long i, long j) {
            return asc ? i <= j : j <= i;
        }

        public long term(int i) {
            return terms[i + 1];
        }

        public int bucket(int i) {
            return buckets[i + 1];
        }

        public long topTerm() {
            return terms[1];
        }

        /**
         * Update the top term and return the new top term.
         */
        public long updateTopTerm(long topTerm, Status status) {
            // get the bucket of the least term or allocate a new bucket
            int bucket = buckets[1];
            if (bucket < 0) {
                status.recycled = false;
                bucket = buckets[1] = maxBucket++;
            } else {
                status.recycled = true;
            }
            status.bucket = bucket;

            // enforce heap structure
            terms[1] = topTerm;
            int node = 1;
            while (true) {
                int left = node << 1;
                int right = left + 1;

                if (left > size) {
                    break;
                }

                int target = left;
                if (right <= size && lessThan(terms[left], terms[right])) {
                    target = right;
                }

                if (lessThan(topTerm, terms[target])) {
                    swap(node, target);
                    node = target;
                } else {
                    break;
                }
            }

            return terms[1];
        }

    }

    private final LongIntOpenHashMap termToBucket;
    private final PriorityQueue terms;
    private long topTerm;

    public TermStreamingTopK(int size, boolean asc) {
        Preconditions.checkArgument(size >= 1);
        termToBucket = new LongIntOpenHashMap(size);
        terms = new PriorityQueue(size, asc);
        topTerm = terms.topTerm();
    }

    @Override
    public Status add(long term, Status status) {
        if (!terms.lessThan(term, topTerm)) {
            // the new term is not competitive, ignore
            return status.reset(-1, false);
        }
        if (termToBucket.containsKey(term)) {
            // already tracked
            final int bucket = termToBucket.lget();
            return status.reset(bucket, false);
        }
        // the new term is not tracked yet, let's replace the least one
        termToBucket.remove(topTerm);
        topTerm = terms.updateTopTerm(term, status);
        termToBucket.put(term, status.bucket);
        assert termToBucket.size() == terms.maxBucket();
        return status;
    }

    @Override
    public long[] topTerms() {
        final long[] topTerms = new long[terms.maxBucket()];
        for (int i = 0; i < terms.size(); ++i) {
            final int bucket = terms.bucket(i);
            if (bucket >= 0) {
                final long term = terms.term(i);
                topTerms[bucket] = term;
            }
        }
        return topTerms;
    }

}
