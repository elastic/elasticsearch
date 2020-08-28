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

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class BucketUtilsTests extends ESTestCase {

    public void testBadInput() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> BucketUtils.suggestShardSideQueueSize(0));
        assertEquals(e.getMessage(), "size must be positive, got 0");
    }

    public void testOverFlow() {
        for (int iter = 0; iter < 10; ++iter) {
            final int size = Integer.MAX_VALUE - randomInt(10);
            final int shardSize = BucketUtils.suggestShardSideQueueSize( size);
            assertThat(shardSize, greaterThanOrEqualTo(shardSize));
        }
    }

    public void testShardSizeIsGreaterThanGlobalSize() {
        for (int iter = 0; iter < 10; ++iter) {
            final int size = randomIntBetween(1, Integer.MAX_VALUE);
            final int shardSize = BucketUtils.suggestShardSideQueueSize( size);
            assertThat(shardSize, greaterThanOrEqualTo(size));
        }
    }

  /*// You may use the code below to evaluate the impact of the BucketUtils.suggestShardSideQueueSize
    // heuristic
    public static void main(String[] args) {
        final int numberOfUniqueTerms = 10000;
        final int totalNumberOfTerms = 1000000;
        final int numberOfShards = 10;
        final double skew = 2; // parameter of the zipf distribution
        final int size = 100;

        double totalWeight = 0;
        for (int rank = 1; rank <= numberOfUniqueTerms; ++rank) {
            totalWeight += weight(rank, skew);
        }

        int[] terms = new int[totalNumberOfTerms];
        int len = 0;

        final int[] actualTopFreqs = new int[size];
        for (int rank = 1; len < totalNumberOfTerms; ++rank) {
            int freq = (int) (weight(rank, skew) / totalWeight * totalNumberOfTerms);
            freq = Math.max(freq, 1);
            Arrays.fill(terms, len, Math.min(len + freq, totalNumberOfTerms), rank - 1);
            len += freq;
            if (rank <= size) {
                actualTopFreqs[rank-1] = freq;
            }
        }

        final int maxTerm = terms[terms.length - 1] + 1;

        // shuffle terms
        Random r = new Random(0);
        for (int i = terms.length - 1; i > 0; --i) {
            final int swapWith = r.nextInt(i);
            int tmp = terms[i];
            terms[i] = terms[swapWith];
            terms[swapWith] = tmp;
        }
        // distribute into shards like routing would
        int[][] shards = new int[numberOfShards][];
        int upTo = 0;
        for (int i = 0; i < numberOfShards; ++i) {
            shards[i] = Arrays.copyOfRange(terms, upTo, upTo + (terms.length - upTo) / (numberOfShards - i));
            upTo += shards[i].length;
        }

        final int[][] topShards = new int[numberOfShards][];
        final int shardSize = BucketUtils.suggestShardSideQueueSize(size, numberOfShards);
        for (int shard = 0; shard < numberOfShards; ++shard) {
            final int[] data = shards[shard];
            final int[] freqs = new int[maxTerm];
            for (int d : data) {
                freqs[d]++;
            }
            int[] termIds = new int[maxTerm];
            for (int i = 0; i < maxTerm; ++i) {
                termIds[i] = i;
            }
            new InPlaceMergeSorter() {

                @Override
                protected void swap(int i, int j) {
                    int tmp = termIds[i];
                    termIds[i] = termIds[j];
                    termIds[j] = tmp;
                    tmp = freqs[i];
                    freqs[i] = freqs[j];
                    freqs[j] = tmp;
                }

                @Override
                protected int compare(int i, int j) {
                    return freqs[j] - freqs[i];
                }
            }.sort(0, maxTerm);

            Arrays.fill(freqs, shardSize, freqs.length, 0);
            new InPlaceMergeSorter() {

                @Override
                protected void swap(int i, int j) {
                    int tmp = termIds[i];
                    termIds[i] = termIds[j];
                    termIds[j] = tmp;
                    tmp = freqs[i];
                    freqs[i] = freqs[j];
                    freqs[j] = tmp;
                }

                @Override
                protected int compare(int i, int j) {
                    return termIds[i] - termIds[j];
                }
            }.sort(0, maxTerm);

            topShards[shard] = freqs;
        }

        final int[] computedTopFreqs = new int[size];
        for (int[] freqs : topShards) {
            for (int i = 0; i < size; ++i) {
                computedTopFreqs[i] += freqs[i];
            }
        }
        int numErrors = 0;
        int totalFreq = 0;
        for (int i = 0; i < size; ++i) {
            numErrors += Math.abs(computedTopFreqs[i] - actualTopFreqs[i]);
            totalFreq += actualTopFreqs[i];
        }
        System.out.println("Number of unique terms: " + maxTerm);
        System.out.println("Global freqs of top terms: " + Arrays.toString(actualTopFreqs));
        System.out.println("Computed freqs of top terms: " + Arrays.toString(computedTopFreqs));
        System.out.println("Number of errors: " + numErrors + "/" + totalFreq);
    }

    private static double weight(int rank, double skew) {
        return 1d / Math.pow(rank, skew);
    }*/

}
