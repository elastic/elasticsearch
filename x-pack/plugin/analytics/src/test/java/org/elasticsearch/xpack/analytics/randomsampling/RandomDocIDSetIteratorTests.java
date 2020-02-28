/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.randomsampling;

import org.apache.lucene.search.DocIdSetIterator;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class RandomDocIDSetIteratorTests extends ESTestCase {

    public void testRandomSampler() throws IOException {
        int maxDoc = 10000;
        int seed = randomInt();

        for (int i = 1; i < 100; i++) {
            double p = i / 100.0;
            int count = 0;
            RandomSamplingQuery.RandomSamplingIterator iter = new RandomSamplingQuery.RandomSamplingIterator(maxDoc, p, seed);
            while (iter.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                count += 1;
            }

            double error = Math.abs((maxDoc * p)/count) / (maxDoc * p);
            if (error > 0.05) {
                fail("Hit count was [" + count + "], expected to be close to " + maxDoc * p
                    + " (+/- 5% error). Error was " + error + ", p=" + p);
            }
        }
    }

    public void testRandomGapSampler() throws IOException {
        int maxDoc = 10000;
        int seed = randomInt();

        for (int i = 1; i < 100; i++) {
            double p = i / 100.0;
            int count = 0;
            RandomSamplingQuery.RandomGapIterator iter = new RandomSamplingQuery.RandomGapIterator(maxDoc, p, seed);
            while (iter.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                count += 1;
            }

            double error = Math.abs((maxDoc * p)/count) / (maxDoc * p);
            if (error > 0.05) {
                fail("Hit count was [" + count + "], expected to be close to " + maxDoc * p
                    + " (+/- 5% error). Error was " + error + ", p=" + p);
            }
        }
    }
}
