/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME;

public class TransportEsqlQueryActionTests extends ESTestCase {

    /**
     * All external blob-store access — {@link org.elasticsearch.xpack.esql.datasources.ExternalSourceResolver} glob
     * expansion, footer reads, and schema reconciliation, plus the blocking data reads routed through
     * {@link org.elasticsearch.xpack.esql.datasources.OperatorFactoryRegistry#fileReadExecutor()} — runs on the pool
     * named by {@link EsqlPlugin#externalBlobStorePool()}, bounded by the per-scheme permit semaphore in
     * {@code StorageProviderRegistry} rather than a dedicated thread pool. It resolves to {@code esql_worker} today.
     * It must never be {@link ThreadPool.Names#SEARCH} (a single wildcard query over thousands of files previously
     * consumed nearly the entire SEARCH pool, starving unrelated searches) nor {@link ThreadPool.Names#GENERIC} (which
     * lets a single heavy external query starve the rest of the node); the explicit assertions catch either
     * regression.
     */
    public void testExternalBlobStorePoolIsTheEsqlWorkerPool() {
        assertEquals(ESQL_WORKER_THREAD_POOL_NAME, EsqlPlugin.externalBlobStorePool());
        assertNotEquals(
            "external blob-store access must not run on the shared search pool",
            ThreadPool.Names.SEARCH,
            EsqlPlugin.externalBlobStorePool()
        );
        assertNotEquals(
            "external blob-store access must not run on the shared generic pool",
            ThreadPool.Names.GENERIC,
            EsqlPlugin.externalBlobStorePool()
        );
    }

    /**
     * ES|QL compute — driver execution and the parallel worker fan-out — runs on the pool named by
     * {@link EsqlPlugin#computePool()}, which resolves to {@code esql_worker} today. It is exposed as a separate
     * accessor from {@link EsqlPlugin#externalBlobStorePool()} so the two can later be split onto distinct pools; this
     * test pins that both currently resolve to the same {@code esql_worker} pool.
     */
    public void testComputePoolIsTheEsqlWorkerPool() {
        assertEquals(ESQL_WORKER_THREAD_POOL_NAME, EsqlPlugin.computePool());
        assertEquals(
            "compute and external blob-store access share one pool for now",
            EsqlPlugin.externalBlobStorePool(),
            EsqlPlugin.computePool()
        );
    }
}
