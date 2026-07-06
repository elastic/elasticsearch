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
import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.EXTERNAL_IO_THREAD_POOL_NAME;

public class TransportEsqlQueryActionTests extends ESTestCase {

    /**
     * All external blob-store access — {@link org.elasticsearch.xpack.esql.datasources.ExternalSourceResolver} glob
     * expansion, footer reads, and schema reconciliation, plus the blocking data reads and streaming parse pipeline
     * routed through {@link org.elasticsearch.xpack.esql.datasources.OperatorFactoryRegistry#fileReadExecutor()} —
     * runs on the dedicated pool named by {@link EsqlPlugin#externalBlobStorePool()} ({@code esql_external_io}), with
     * in-flight cloud calls additionally bounded by the per-scheme permit semaphore in {@code StorageProviderRegistry}.
     * It must never be {@link ThreadPool.Names#SEARCH} (a single wildcard query over thousands of files previously
     * consumed nearly the entire SEARCH pool, starving unrelated searches) nor {@link ThreadPool.Names#GENERIC} (which
     * lets a single heavy external query starve the rest of the node); the explicit assertions catch either
     * regression.
     */
    public void testExternalBlobStorePoolIsTheDedicatedExternalIoPool() {
        assertEquals(EXTERNAL_IO_THREAD_POOL_NAME, EsqlPlugin.externalBlobStorePool());
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
     * ES|QL compute — driver execution and the parallel worker fan-out — runs on {@link EsqlPlugin#computePool()}
     * ({@code esql_worker}). This must be a <em>different</em> pool from {@link EsqlPlugin#externalBlobStorePool()}:
     * the blocking external I/O and streaming parse pipeline (segmentator + parser tasks) would otherwise occupy the
     * same threads as the compute drivers that consume their output and deadlock the query (the heap-attack external
     * stall). This test pins that separation.
     */
    public void testComputePoolIsSeparateFromExternalBlobStorePool() {
        assertEquals(ESQL_WORKER_THREAD_POOL_NAME, EsqlPlugin.computePool());
        assertNotEquals(
            "compute must not share the external blob-store pool, or the parse pipeline can starve the drivers",
            EsqlPlugin.externalBlobStorePool(),
            EsqlPlugin.computePool()
        );
    }
}
