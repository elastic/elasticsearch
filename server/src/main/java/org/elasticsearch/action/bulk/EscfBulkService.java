/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.IndexingPressure;

import java.util.concurrent.Executor;

/**
 * Creates the {@link EscfBulkWriter}s of a node. The writers count the rows they hold in the node's coordinating
 * {@link IndexingPressure}, from the first row until its bulk is acknowledged, like the other bulks the node coordinates.
 */
public final class EscfBulkService {

    private final IndexingPressure indexingPressure;
    private final Executor executor;
    private final Recycler<BytesRef> recycler;

    /**
     * @param executor settles the bulks and calls the writers back, off the threads that complete the bulks
     */
    public EscfBulkService(IndexingPressure indexingPressure, Executor executor, Recycler<BytesRef> recycler) {
        this.indexingPressure = indexingPressure;
        this.executor = executor;
        this.recycler = recycler;
    }

    /**
     * Creates a writer that indexes into {@code index} and charges its memory to the task's {@code breaker}. The breaker is used
     * from several threads, so it must be thread-safe.
     *
     * @param client     sends the bulks; one that sets the task as their parent cancels them with the task
     * @param onResponse reads the response of each bulk, such as {@link EscfBulkWriter.ResponseHandler#FAIL_ON_FAILED_ITEM}
     * @param onCapacity called from the executor when {@link EscfBulkWriter.Producer#hasCapacity()} may have turned true; it may
     *                   not be called once the executor is shut down
     */
    public EscfBulkWriter newWriter(
        Client client,
        CircuitBreaker breaker,
        String index,
        EscfBulkWriter.Policy policy,
        EscfBulkWriter.ResponseHandler onResponse,
        Runnable onCapacity
    ) {
        return new EscfBulkWriter(this, client, breaker, recycler, index, policy, onResponse, onCapacity);
    }

    IndexingPressure indexingPressure() {
        return indexingPressure;
    }

    /**
     * Runs a callback on the executor. On shutdown it is dropped, not run here, since the caller may hold a lock it takes.
     */
    void execute(Runnable callback) {
        try {
            executor.execute(callback);
        } catch (EsRejectedExecutionException e) {
            assert e.isExecutorShutdown() : e;
        }
    }

    /**
     * Runs {@code task} on the executor, or here on shutdown. For work that must happen, like settling a bulk.
     */
    void executeOrRun(Runnable task) {
        try {
            executor.execute(task);
        } catch (EsRejectedExecutionException e) {
            task.run();
        }
    }
}
