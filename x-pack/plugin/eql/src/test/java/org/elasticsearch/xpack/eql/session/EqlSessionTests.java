/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql.session;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.index.store.DirectoryMetrics;
import org.elasticsearch.index.store.StoreMetrics;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.eql.EqlTestUtils;
import org.elasticsearch.xpack.eql.analysis.PostAnalyzer;
import org.elasticsearch.xpack.eql.analysis.PreAnalyzer;
import org.elasticsearch.xpack.eql.analysis.Verifier;
import org.elasticsearch.xpack.eql.expression.function.EqlFunctionRegistry;
import org.elasticsearch.xpack.eql.optimizer.Optimizer;
import org.elasticsearch.xpack.eql.planner.Planner;
import org.elasticsearch.xpack.eql.stats.Metrics;

public class EqlSessionTests extends ESTestCase {

    public void testAccumulateDirectoryMetricsSumsAcrossSubSearches() {
        try (var threadPool = createThreadPool()) {
            EqlSession session = newSession(threadPool);
            assertTrue(session.directoryMetrics().isEmpty());

            long expectedBytesRead = 0;
            int subSearches = randomIntBetween(1, 20);
            for (int i = 0; i < subSearches; i++) {
                long bytesRead = randomLongBetween(1, 1000);
                expectedBytesRead += bytesRead;
                session.accumulateDirectoryMetrics(storeMetrics(bytesRead));
            }

            assertEquals(expectedBytesRead, session.directoryMetrics().metrics(StoreMetrics.NAME).cast(StoreMetrics.class).getBytesRead());
        }
    }

    public void testAccumulateDirectoryMetricsSkipsEmptyAndNull() {
        try (var threadPool = createThreadPool()) {
            EqlSession session = newSession(threadPool);
            assertTrue(session.directoryMetrics().isEmpty());

            session.accumulateDirectoryMetrics(DirectoryMetrics.EMPTY);
            assertTrue(session.directoryMetrics().isEmpty());

            session.accumulateDirectoryMetrics(null);
            assertTrue(session.directoryMetrics().isEmpty());
        }
    }

    private static EqlSession newSession(ThreadPool threadPool) {
        return new EqlSession(
            new NoOpClient(threadPool),
            EqlTestUtils.randomConfiguration(),
            null,
            new PreAnalyzer(),
            new PostAnalyzer(),
            new EqlFunctionRegistry(),
            new Verifier(new Metrics()),
            new Optimizer(),
            new Planner(),
            new NoopCircuitBreaker("test")
        );
    }

    private static DirectoryMetrics storeMetrics(long bytesRead) {
        DirectoryMetrics.Builder builder = new DirectoryMetrics.Builder();
        builder.add(StoreMetrics.NAME, new StoreMetrics(bytesRead));
        return builder.build();
    }
}
