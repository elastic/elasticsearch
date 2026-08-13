/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.search.msearch;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.action.search.TransportMultiSearchAction;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

import static org.hamcrest.Matchers.greaterThan;

public class MsearchFailureEstimationBenchmarkTests extends ESTestCase {

    private final int shardFailureCount;

    public MsearchFailureEstimationBenchmarkTests(int shardFailureCount) {
        this.shardFailureCount = shardFailureCount;
    }

    public void testSetupAndEstimate() {
        long estimate = estimate(shardFailureCount);
        assertThat("shardFailureCount=" + shardFailureCount + " should produce a positive estimate", estimate, greaterThan(0L));
    }

    public void testEstimateScalesWithShardFailureCount() {
        int[] counts = Utils.possibleValues(MsearchFailureEstimationBenchmark.class, "shardFailureCount")
            .stream()
            .mapToInt(Integer::parseInt)
            .sorted()
            .toArray();
        long previous = 0L;
        for (int count : counts) {
            long estimate = estimate(count);
            assertThat("shardFailureCount=" + count + " should exceed the previous, smaller count", estimate, greaterThan(previous));
            previous = estimate;
        }
    }

    private static long estimate(int shardFailureCount) {
        var bench = new MsearchFailureEstimationBenchmark();
        bench.shardFailureCount = shardFailureCount;
        bench.setup();
        return TransportMultiSearchAction.estimateFailureBytes(bench.failure);
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() {
        String[] counts = Utils.possibleValues(MsearchFailureEstimationBenchmark.class, "shardFailureCount").toArray(new String[0]);
        return () -> Arrays.stream(counts).map(s -> new Object[] { Integer.parseInt(s) }).iterator();
    }
}
