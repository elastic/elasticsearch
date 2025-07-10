/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.exponentialhistogram;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class DownscaleStatsTest extends ESTestCase {

    public void testExponential() {
        long[] values = IntStream.range(0, 100).mapToLong(i -> (long) Math.min(Integer.MAX_VALUE, Math.pow(1.1, i))).distinct().toArray();
        verifyFor(values);
    }

    public void testNumericalLimits() {
        verifyFor(Long.MIN_VALUE, Long.MAX_VALUE);
    }

    public void testRandom() {
        Random rnd = new Random(42);

        for (int i = 0; i < 100; i++) {
            List<Long> values = IntStream.range(0, 10_000).mapToObj(j -> rnd.nextLong()).distinct().toList();
            verifyFor(values);
        }
    }

    void verifyFor(long... indices) {
        verifyFor(LongStream.of(indices).boxed().toList());
    }

    void verifyFor(Collection<Long> indices) {
        // sanity check, we require unique indices
        assertThat(indices.size(), equalTo(new HashSet<>(indices).size()));

        List<Long> sorted = new ArrayList<>(indices);
        sorted.sort(Long::compareTo);

        DownscaleStats stats = new DownscaleStats();
        for (int i = 1; i < sorted.size(); i++) {
            long prev = sorted.get(i - 1);
            long curr = sorted.get(i);
            stats.add(prev, curr);
        }

        for (int i = 0; i < 64; i++) {
            int scaleReduction = i;
            long remainingCount = indices.stream().mapToLong(Long::longValue).map(index -> index >> scaleReduction).distinct().count();
            long reduction = sorted.size() - remainingCount;

            assertThat(
                "Expected size after reduction of " + i + " to match",
                stats.getCollapsedBucketCountAfterScaleReduction(scaleReduction),
                equalTo((int) reduction)
            );
        }

    }
}
