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
import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.EXTERNAL_BLOCKING_IO_THREAD_POOL_NAME;

public class TransportEsqlQueryActionTests extends ESTestCase {

    /**
     * Blocking external (GCS/local file) reads must run on the dedicated, bounded
     * {@code esql_external_blocking_io} pool. {@link TransportEsqlQueryAction}'s constructor resolves the
     * {@link org.elasticsearch.xpack.esql.datasources.OperatorFactoryRegistry#fileReadExecutor()} read executor by
     * looking the pool up under {@link TransportEsqlQueryAction#fileReadExecutorName()}, so pinning the returned name
     * here locks the real wiring. The round-1 bug pointed this argument at {@link ThreadPool.Names#GENERIC}, which
     * lets a single heavy external query starve the rest of the node; the explicit not-{@code generic} assertion
     * is what catches that regression.
     */
    public void testFileReadExecutorNameIsTheDedicatedExternalBlockingIoPool() {
        assertEquals(EXTERNAL_BLOCKING_IO_THREAD_POOL_NAME, TransportEsqlQueryAction.fileReadExecutorName());
        assertNotEquals(
            "blocking external reads must not run on the shared generic pool",
            ThreadPool.Names.GENERIC,
            TransportEsqlQueryAction.fileReadExecutorName()
        );
    }

    /**
     * External source coordination — including {@link org.elasticsearch.xpack.esql.datasources.ExternalSourceResolver}
     * glob expansion, footer reads, and schema reconciliation — must run on the dedicated {@code esql_worker} pool.
     * A prior regression routed this work through {@link ThreadPool.Names#SEARCH}, where a single wildcard query
     * over thousands of files consumed nearly the entire SEARCH pool for minutes, starving unrelated ES searches and
     * other ES|QL queries. Pinning the returned name here locks the fix; the explicit not-{@code search} assertion
     * catches any future re-introduction of that starvation.
     */
    public void testExternalSourceExecutorNameIsTheEsqlWorkerPool() {
        assertEquals(ESQL_WORKER_THREAD_POOL_NAME, TransportEsqlQueryAction.externalSourceExecutorName());
        assertNotEquals(
            "external source coordination must not run on the shared search pool",
            ThreadPool.Names.SEARCH,
            TransportEsqlQueryAction.externalSourceExecutorName()
        );
    }
}
