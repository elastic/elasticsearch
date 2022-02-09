/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.sampler.random;

import org.apache.lucene.search.DocIdSetIterator;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;

import static org.hamcrest.Matchers.equalTo;

public class RandomDocIDSetIteratorTests extends ESTestCase {

    public void testRandomSampler() {
        int maxDoc = 10000;
        SplittableRandom random = new SplittableRandom(randomInt());

        for (int i = 1; i < 100; i++) {
            double p = i / 100.0;
            int count = 0;
            RandomSamplingQuery.RandomSamplingIterator iter = new RandomSamplingQuery.RandomSamplingIterator(maxDoc, p, random::nextInt);
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
            RandomSamplingQuery.RandomSamplingIterator iter = new RandomSamplingQuery.RandomSamplingIterator(maxDoc, p, random::nextInt);
            while (iter.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                iterationOne.add(iter.docID());
            }
            random = new SplittableRandom(seed);
            List<Integer> iterationTwo = new ArrayList<>();
            iter = new RandomSamplingQuery.RandomSamplingIterator(maxDoc, p, random::nextInt);
            while (iter.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                iterationTwo.add(iter.docID());
            }
            assertThat(iterationOne, equalTo(iterationTwo));
        }
    }

}
