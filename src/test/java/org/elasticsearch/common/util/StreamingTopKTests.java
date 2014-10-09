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

import com.carrotsearch.hppc.LongLongOpenHashMap;
import org.apache.lucene.util.InPlaceMergeSorter;
import org.elasticsearch.test.ElasticsearchTestCase;

import java.util.Arrays;
import java.util.Random;

public class StreamingTopKTests extends ElasticsearchTestCase {

    private static class TermAndCount {
        public final long term;
        public final long count;

        TermAndCount(long term, long count) {
            this.term = term;
            this.count = count;
        }

        @Override
        public int hashCode() {
            int result = 1;
            result = 31 * result + (int) (count ^ (count >>> 32));
            result = 31 * result + (int) (term ^ (term >>> 32));
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            TermAndCount other = (TermAndCount) obj;
            if (count != other.count)
                return false;
            if (term != other.term)
                return false;
            return true;
        }

        @Override
        public String toString() {
            return "{" + term + ":" + count + "}";
        }
    }

    // Base implementation of a counter
    private static interface Counter {

        public void add(long term);

        public TermAndCount[] topTerms();

    }

    // Dummy implementation that computes all counts and shrinks in the end
    private static class DummyTopTermCounter implements Counter {

        private final boolean asc;
        private final int size;
        private final LongLongOpenHashMap counts;

        public DummyTopTermCounter(boolean asc, int size) {
            this.asc = asc;
            this.size = size;
            counts = new LongLongOpenHashMap();
        }

        @Override
        public void add(long term) {
            if (counts.containsKey(term)) {
                counts.lset(counts.lget() + 1);
            } else {
                counts.put(term, 1);
            }
        }

        @Override
        public TermAndCount[] topTerms() {
            final long[] terms = counts.keys().toArray();
            new InPlaceMergeSorter() {
                @Override
                protected void swap(int i, int j) {
                    final long tmp = terms[i];
                    terms[i] = terms[j];
                    terms[j] = tmp;
                }

                @Override
                protected int compare(int i, int j) {
                    return asc ? Long.compare(terms[i], terms[j]) : Long.compare(terms[j], terms[i]);
                }
            }.sort(0, terms.length);
            TermAndCount[] topTerms = new TermAndCount[Math.min(size, terms.length)];
            for (int i = 0; i < topTerms.length; ++i) {
                topTerms[i] = new TermAndCount(terms[i], counts.get(terms[i]));
            }
            return topTerms;
        }

    }

    private static class StreamingTopTermCounter implements Counter {

        private final boolean asc;
        private final StreamingTopK topK;
        private final long[] counts;
        private final StreamingTopK.Status status;

        StreamingTopTermCounter(boolean asc, int size) {
            this.asc = asc;
            topK = new TermStreamingTopK(size, asc);
            counts = new long[size];
            status = new StreamingTopK.Status();
        }

        @Override
        public void add(long term) {
            topK.add(term, status);
            if (status.bucket >= 0) {
                if (status.recycled) {
                    counts[status.bucket] = 1;
                } else {
                    counts[status.bucket] += 1;
                }
            }
        }

        @Override
        public TermAndCount[] topTerms() {
            final long[] terms = topK.topTerms();
            final TermAndCount[] topTerms = new TermAndCount[terms.length];
            for (int i = 0; i < terms.length; ++i) {
                topTerms[i] = new TermAndCount(terms[i], counts[i]);
            }
            new InPlaceMergeSorter() {

                @Override
                protected void swap(int i, int j) {
                    TermAndCount tmp = topTerms[i];
                    topTerms[i] = topTerms[j];
                    topTerms[j] = tmp;
                }

                @Override
                protected int compare(int i, int j) {
                    return asc ? Long.compare(topTerms[i].term, topTerms[j].term) : Long.compare(topTerms[j].term, topTerms[i].term);
                }
            }.sort(0, topTerms.length);
            return topTerms;
        }

    }

    // Dummy implementation that computes all counts and shrinks in the end
    private static class DummyTopCountCounter implements Counter {

        private final int size;
        private final LongLongOpenHashMap counts;

        public DummyTopCountCounter(int size) {
            this.size = size;
            counts = new LongLongOpenHashMap();
        }

        @Override
        public void add(long term) {
            if (counts.containsKey(term)) {
                counts.lset(counts.lget() + 1);
            } else {
                counts.put(term, 1);
            }
        }

        @Override
        public TermAndCount[] topTerms() {
            final long[] terms = counts.keys().toArray();
            new InPlaceMergeSorter() {
                @Override
                protected void swap(int i, int j) {
                    final long tmp = terms[i];
                    terms[i] = terms[j];
                    terms[j] = tmp;
                }

                @Override
                protected int compare(int i, int j) {
                    return Long.compare(counts.get(terms[j]), counts.get(terms[i]));
                }
            }.sort(0, terms.length);
            TermAndCount[] topTerms = new TermAndCount[Math.min(size, terms.length)];
            for (int i = 0; i < topTerms.length; ++i) {
                topTerms[i] = new TermAndCount(terms[i], counts.get(terms[i]));
            }
            return topTerms;
        }
    }

    private static class StreamingTopCountCounter implements Counter {

        private final StreamingTopK topK;
        private final long[] counts;
        private final StreamingTopK.Status status;

        StreamingTopCountCounter(int size) {
            topK = new CountStreamingTopK(size);
            counts = new long[size];
            status = new StreamingTopK.Status();
        }

        @Override
        public void add(long term) {
            topK.add(term, status);
            if (status.recycled) {
                counts[status.bucket] = 1;
            } else {
                counts[status.bucket] += 1;
            }
        }

        @Override
        public TermAndCount[] topTerms() {
            final long[] terms = topK.topTerms();
            final TermAndCount[] topTerms = new TermAndCount[terms.length];
            for (int i = 0; i < terms.length; ++i) {
                topTerms[i] = new TermAndCount(terms[i], counts[i]);
            }
            new InPlaceMergeSorter() {
                @Override
                protected void swap(int i, int j) {
                    TermAndCount tmp = topTerms[i];
                    topTerms[i] = topTerms[j];
                    topTerms[j] = tmp;
                }

                @Override
                protected int compare(int i, int j) {
                    return Long.compare(topTerms[j].count, topTerms[i].count);
                }
            }.sort(0, topTerms.length);
            return topTerms;
        }
    }

    public void testTerms() {
        // we run a duel between our streaming impl and the dummy impl
        for (boolean asc : new boolean[] { true, false }) {
            final int numberOfUniqueValues = scaledRandomIntBetween(1, 100000);
            final int iters = scaledRandomIntBetween(numberOfUniqueValues, Math.max(100000, numberOfUniqueValues * 3));
            final int k = scaledRandomIntBetween(1, 1000);
            final long[] terms = new long[numberOfUniqueValues];
            for (int i = 0; i < terms.length; ++i) {
                terms[i] = randomLong();
            }

            Counter counter1 = new DummyTopTermCounter(asc, k);
            Counter counter2 = new StreamingTopTermCounter(asc, k);
            for (int i = 0; i < iters; ++i) {
                final long term = terms[randomInt(numberOfUniqueValues - 1)];
                counter1.add(term);
                counter2.add(term);
            }
            final TermAndCount[] topTerms1 = counter1.topTerms();
            final TermAndCount[] topTerms2 = counter2.topTerms();
            assertArrayEquals(topTerms1, topTerms2);
        }
    }

    public void testCounts() {
        // still a duel but this time the streaming impl is not accurate
        final int numberOfUniqueValues = 1000;
        final int iters = 1000000;
        final long[] terms = new long[numberOfUniqueValues];
        for (int i = 0; i < terms.length; ++i) {
            terms[i] = randomLong();
        }

        for (int iter = 0; iter < 10; ++iter) {
            final int k = scaledRandomIntBetween(100, 1100);
            Counter counter1 = new DummyTopCountCounter(k);
            Counter counter2 = new StreamingTopCountCounter(k);
            // the streaming implementation is approximate by nature. If we used a random seed
            // then we could sometimes hit a special sequence of numbers that would increase
            // the error, this is why we test on a single seed
            Random r = new Random(0);
            for (int i = 0; i < iters; ++i) {
                final int index = (int) (Math.pow(r.nextDouble(), 10) * numberOfUniqueValues); // force some terms to be much more frequent
                final long term = terms[index];
                counter1.add(term);
                counter2.add(term);
            }
            final TermAndCount[] topTerms1 = counter1.topTerms();
            final TermAndCount[] topTerms2 = counter2.topTerms();
            assertEquals(topTerms1.length, topTerms2.length);
            // only compare the top 5 elements, afterwards errors are expected
            assertArrayEquals(Arrays.copyOf(topTerms1, 5), Arrays.copyOf(topTerms2, 5));
        }
    }

}
