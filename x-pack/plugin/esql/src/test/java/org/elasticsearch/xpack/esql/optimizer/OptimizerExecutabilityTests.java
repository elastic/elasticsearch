/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.TestQueryExecutor;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.ESQL_THREAD_POOL_NAME;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests that run against actual indices, to confirm that optimized plans remain executable.
 */
public class OptimizerExecutabilityTests extends MapperServiceTestCase {
    private final EsqlParser parser = new EsqlParser();

    public void testOutOfRangePushdown() {
        runQuery("row a = 0 | eval b = a+1.1");
    }

    private void runQuery(String query) {
        int numThreads = randomBoolean() ? 1 : between(2, 16);
        var threadPool = new TestThreadPool(
            "OptimizerExecutabilityTests",
            new FixedExecutorBuilder(
                Settings.EMPTY,
                ESQL_THREAD_POOL_NAME,
                numThreads,
                1024,
                "esql",
                EsExecutors.TaskTrackingConfig.DEFAULT
            )
        );
        HeaderWarning.setThreadContext(threadPool.getThreadContext());
        var executor = new TestQueryExecutor(threadPool);

        var parsed = parser.createStatement(query);
        var actualResults = executor.executePlan(
            parsed,
            // TODO: Here, needs Nik's PR first.
            new EsPhysicalOperationProviders(List.of()),
            IndexResolution.valid(new EsIndex("testidx", Map.of())),
            new EnrichResolution(Set.of(), Set.of())
        );
        Releasables.close(() -> Iterators.map(actualResults.pages().iterator(), p -> p::releaseBlocks));
        assertThat(executor.breakerService().getBreaker(CircuitBreaker.REQUEST).getUsed(), equalTo(0L));

        assertWarnings("No limit defined, adding default limit of [500]");

        HeaderWarning.removeThreadContext(threadPool.getThreadContext());
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }
}
