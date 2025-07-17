/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.exponentialhistogram;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MAX_INDEX;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MAX_INDEX_BITS;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MIN_INDEX;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class DownscaleStatsTests extends ESTestCase {

    public void testExponential() {
        long[] values = IntStream.range(0, 100).mapToLong(i -> (long) Math.min(MAX_INDEX, Math.pow(1.1, i))).distinct().toArray();
        verifyFor(values);
    }

    public void testNumericalLimits() {
        verifyFor(MIN_INDEX, MAX_INDEX);
    }

    public void testRandom() {
        for (int i = 0; i < 100; i++) {
            List<Long> values = IntStream.range(0, 1000).mapToObj(j -> random().nextLong(MIN_INDEX, MAX_INDEX + 1)).distinct().toList();
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

        for (int i = 0; i <= MAX_INDEX_BITS; i++) {
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
