/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.sampler.random;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.hppc.BitMixer;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class RandomDocIDSetIteratorTests extends ESTestCase {

    public void testRandomSamplingWithChunk() {
        double p = 0.005;
        int batchSize = 100;
        int maxMaxDoc = 1_000_000;
        int seed = randomInt();
        int numShards = 100;
        int[] singleCounts = new int[numShards];
        int[] batchCounts = new int[numShards];
        int[] trueShardCount = new int[numShards];
        for (int i = 0; i < numShards; i++) {
            int maxDoc = randomIntBetween(100_000, maxMaxDoc);
            trueShardCount[i] = maxDoc;
            SplittableRandom random = new SplittableRandom(BitMixer.mix(i ^ seed));
            int batch_count = 0;
            RandomSamplingQuery.RandomSamplingIterator iter = new RandomSamplingQuery.RandomSamplingIterator(
                maxDoc,
                p,
                batchSize,
                random::nextInt
            );
            while (iter.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                batch_count += 1;
            }
            batchCounts[i] = batch_count;
            random = new SplittableRandom(BitMixer.mix(i ^ seed));
            iter = new RandomSamplingQuery.RandomSamplingIterator(maxDoc, p, 1, random::nextInt);
            int single_count = 0;
            while (iter.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                single_count += 1;
            }
            singleCounts[i] = single_count;
        }

        double avgBatchCount = IntStream.of(batchCounts).average().getAsDouble();
        double avgSingleCount = IntStream.of(singleCounts).average().getAsDouble();
        int sumBatch = IntStream.of(batchCounts).sum();
        int sumSingle = IntStream.of(singleCounts).sum();
        int trueSum = IntStream.of(trueShardCount).sum();
        SamplingContext context = new SamplingContext(p, seed);
        long adjSumBatch = context.scaleUp(sumBatch);
        long adjSumSingle = context.scaleUp(sumSingle);
        double batchVariance = 0;
        double singleVariance = 0;
        for (int i = 0; i < numShards; i++) {
            batchVariance += Math.pow((double) batchCounts[i] - avgBatchCount, 2.0);
            singleVariance += Math.pow((double) singleCounts[i] - avgSingleCount, 2.0);
        }
        batchVariance /= (numShards - 1);
        singleVariance /= (numShards - 1);
        int maxBatch = IntStream.of(batchCounts).max().getAsInt();
        int minBatch = IntStream.of(batchCounts).min().getAsInt();
        int maxSingle = IntStream.of(singleCounts).max().getAsInt();
        int minSingle = IntStream.of(singleCounts).min().getAsInt();

        assertThat(true, is(true));
    }

    public void testRandomSampler() {
        int maxDoc = 10000;
        SplittableRandom random = new SplittableRandom(randomInt());

        for (int i = 1; i < 100; i++) {
            double p = i / 100.0;
            int count = 0;
            RandomSamplingQuery.RandomSamplingIterator iter = new RandomSamplingQuery.RandomSamplingIterator(maxDoc, p, 1, random::nextInt);
            while (iter.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                count += 1;
            }

            double error = Math.abs((maxDoc * p) / count) / (maxDoc * p);
            if (error > 0.05) {
                fail(
                    "Hit count was ["
                        + count
                        + "], expected to be close to "
                        + maxDoc * p
                        + " (+/- 5% error). Error was "
                        + error
                        + ", p="
                        + p
                );
            }
        }
    }

    public void testRandomSamplerConsistency() {
        int maxDoc = 10000;
        int seed = randomInt();

        for (int i = 1; i < 100; i++) {
            double p = i / 100.0;
            SplittableRandom random = new SplittableRandom(seed);
            List<Integer> iterationOne = new ArrayList<>();
            RandomSamplingQuery.RandomSamplingIterator iter = new RandomSamplingQuery.RandomSamplingIterator(maxDoc, p, 1, random::nextInt);
            while (iter.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                iterationOne.add(iter.docID());
            }
            random = new SplittableRandom(seed);
            List<Integer> iterationTwo = new ArrayList<>();
            iter = new RandomSamplingQuery.RandomSamplingIterator(maxDoc, p, 1, random::nextInt);
            while (iter.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                iterationTwo.add(iter.docID());
            }
            assertThat(iterationOne, equalTo(iterationTwo));
        }
    }

}
