/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.vector;

import org.elasticsearch.test.ESTestCase;

/**
 * Smoke test that drives {@link CentroidPrefetchDepthBenchmark} end-to-end with tiny parameters to prove the
 * directory setup, the prefetching consumer loop and the auxiliary counters wire together. This is a
 * correctness gate for the harness, not a measurement; the JMH numbers are produced by the separate
 * benchmark-run lane.
 */
public class CentroidPrefetchDepthBenchmarkTests extends ESTestCase {

    public void testRunsEndToEnd() throws Exception {
        CentroidPrefetchDepthBenchmark bench = new CentroidPrefetchDepthBenchmark();
        bench.numLists = 4;
        bench.perListBytes = 4096;
        bench.regionSizeBytes = 262144;
        bench.prefetchAhead = 2;
        bench.firstByteLatencyMs = 0;
        bench.cacheState = CentroidPrefetchDepthBenchmark.CacheState.COLD;
        bench.consumerComputeMicros = 0;
        bench.consumeBudgetLists = 4;

        CentroidPrefetchDepthBenchmark.CacheCounters cacheCounters = new CentroidPrefetchDepthBenchmark.CacheCounters();
        CentroidPrefetchDepthBenchmark.OverFetchCounters overFetchCounters = new CentroidPrefetchDepthBenchmark.OverFetchCounters();

        bench.setupTrial();
        try {
            bench.setupInvocation();
            try {
                Object result = bench.runQuery(cacheCounters, overFetchCounters);
                assertNotNull(result);
                // Over-fetch can never be negative: the delegate advances at least as many lists as were consumed.
                assertTrue("over-fetch must be non-negative", overFetchCounters.overFetchedLists >= 0);
            } finally {
                bench.tearDownInvocation();
            }
        } finally {
            bench.tearDownTrial();
        }
    }
}
