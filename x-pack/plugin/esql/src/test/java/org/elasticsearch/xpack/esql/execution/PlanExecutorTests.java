/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolver;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;

import java.util.List;

import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME;

public class PlanExecutorTests extends ESTestCase {

    /**
     * Locks the PR-A wiring invariants that would otherwise only hold implicitly inside
     * {@link PlanExecutor#esql}: external discovery must run on the {@code esql_worker} pool (isolated
     * from {@code SEARCH}) and its multi-file metadata fan-out must be bounded by exactly that pool's
     * configured max, so a wide discovery cannot outrun the pool it shares with query execution.
     */
    public void testExternalSourceResolverWiredToEsqlWorkerPool() {
        EsqlPlugin plugin = new EsqlPlugin();
        Settings settings = Settings.builder().put("node.name", "test").build();
        List<ExecutorBuilder<?>> builders = plugin.getExecutorBuilders(settings);
        ThreadPool threadPool = new TestThreadPool("test", settings, builders.toArray(new ExecutorBuilder<?>[0]));
        try {
            int esqlWorkerMax = threadPool.info(ESQL_WORKER_THREAD_POOL_NAME).getMax();
            ExternalSourceResolver resolver = PlanExecutor.createExternalSourceResolver(threadPool, null, Settings.EMPTY, null);

            assertEquals("fan-out permit must equal the esql_worker pool size", esqlWorkerMax, resolver.metadataReadConcurrency());
            assertSame(
                "discovery must run on the esql_worker executor",
                threadPool.executor(ESQL_WORKER_THREAD_POOL_NAME),
                resolver.executor()
            );
            assertNotSame("discovery must not run on the SEARCH pool", threadPool.executor(ThreadPool.Names.SEARCH), resolver.executor());
        } finally {
            terminate(threadPool);
        }
    }
}
