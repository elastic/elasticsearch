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
import org.elasticsearch.xpack.esql.datasources.ExternalSourceSettings;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.plugin.TransportEsqlQueryAction;

import java.util.List;
import java.util.concurrent.Executor;

import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.EXTERNAL_IO_THREAD_POOL_NAME;

public class PlanExecutorTests extends ESTestCase {

    /**
     * Locks the PR-A wiring invariants that would otherwise only hold implicitly across
     * {@link TransportEsqlQueryAction} and {@link PlanExecutor#esql}: external discovery must run on the dedicated
     * {@code esql_external_io} pool (isolated from {@code SEARCH}, so a wide wildcard cannot starve regular searches,
     * and separate from {@code esql_worker} so the parse pipeline cannot starve compute) and its multi-file metadata
     * fan-out must be bounded by the shared blob-store access concurrency
     * ({@link ExternalSourceSettings#blobStoreConcurrency(Settings)}, the {@code snapshot_meta} shape capped at
     * 100 — the same effective knob the data-read path uses) rather than the raw pool size. The fan-out may exceed
     * the pool size because footer reads are async (the worker is released across the read).
     * <p>
     * The pool selection lives in {@link EsqlPlugin#externalBlobStorePool()} and the fan-out bound in
     * {@link TransportEsqlQueryAction} ({@code externalSourceConcurrency()}); this test pins both and then confirms
     * {@link PlanExecutor#createExternalSourceResolver} binds the supplied executor and concurrency onto the resolver
     * verbatim (no silent re-homing back to a compute or SEARCH pool).
     */
    public void testExternalSourceResolverWiredToExternalIoPool() {
        assertEquals(
            "external discovery must target the dedicated esql_external_io pool, isolated from SEARCH and compute",
            EXTERNAL_IO_THREAD_POOL_NAME,
            EsqlPlugin.externalBlobStorePool()
        );
        assertNotEquals("esql_external_io must not be the SEARCH pool", ThreadPool.Names.SEARCH, EsqlPlugin.externalBlobStorePool());

        EsqlPlugin plugin = new EsqlPlugin();
        Settings settings = Settings.builder().put("node.name", "test").build();
        List<ExecutorBuilder<?>> builders = plugin.getExecutorBuilders(settings);
        ThreadPool threadPool = new TestThreadPool("test", settings, builders.toArray(new ExecutorBuilder<?>[0]));
        try {
            int fanOut = ExternalSourceSettings.blobStoreConcurrency(settings);
            Executor externalIo = threadPool.executor(EXTERNAL_IO_THREAD_POOL_NAME);

            ExternalSourceResolver resolver = PlanExecutor.createExternalSourceResolver(
                externalIo,
                null,
                Settings.EMPTY,
                null,
                () -> false,
                fanOut,
                threadPool.getThreadContext()
            );

            assertEquals("fan-out permit must be the shared blob-store access concurrency", fanOut, resolver.metadataReadConcurrency());
            assertSame("discovery must run on the esql_external_io executor", externalIo, resolver.executor());
            assertNotSame("discovery must not run on the SEARCH pool", threadPool.executor(ThreadPool.Names.SEARCH), resolver.executor());
        } finally {
            terminate(threadPool);
        }
    }
}
