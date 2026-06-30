/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

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
}
