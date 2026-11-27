/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class SharedScoreBoundTests extends ESTestCase {
    public void testStartsAtZero() {
        assertThat(new SharedScoreBound().get(), equalTo(0f));
    }

    public void testOnlyGrows() {
        SharedScoreBound shared = new SharedScoreBound();
        float max = 0;
        for (int i = between(1, 100); i > 0; i--) {
            float score = randomFloat() * between(1, 1000);
            shared.offer(score);
            max = Math.max(max, score);
            assertThat(shared.get(), equalTo(max));
        }
    }

    public void testIgnoresScoresThatAreNotABound() {
        SharedScoreBound shared = new SharedScoreBound();
        float score = randomFloat() + 1;
        shared.offer(score);
        shared.offer(randomFrom(Float.NaN, Float.NEGATIVE_INFINITY, -randomFloat() - 1, 0f, -0f));
        assertThat(shared.get(), equalTo(score));
    }

    /**
     * A collector whose top N is full of {@link Float#MAX_VALUE} sets the next float up, which is infinity.
     */
    public void testKeepsInfinity() {
        SharedScoreBound shared = new SharedScoreBound();
        shared.offer(Float.POSITIVE_INFINITY);
        shared.offer(randomFloat() + 1);
        assertThat(shared.get(), equalTo(Float.POSITIVE_INFINITY));
    }

    public void testConcurrentOffersKeepTheHighest() {
        SharedScoreBound shared = new SharedScoreBound();
        int threads = between(2, 8);
        float[][] scores = new float[threads][between(100, 1000)];
        float max = 0;
        for (float[] perThread : scores) {
            for (int i = 0; i < perThread.length; i++) {
                perThread[i] = randomFloat() * between(1, 1000);
                max = Math.max(max, perThread[i]);
            }
        }
        startInParallel(threads, t -> {
            for (float score : scores[t]) {
                shared.offer(score);
            }
        });
        assertThat(shared.get(), equalTo(max));
    }
}
