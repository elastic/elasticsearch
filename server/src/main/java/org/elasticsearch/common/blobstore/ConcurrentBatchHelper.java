/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.blobstore;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Sibling to {@link ConcurrentMultipartHelper} for concurrent batch operations. Executes a list of
 * pre-formed batches concurrently up to {@code maxConcurrency}, with the calling thread also
 * participating so that a saturated executor pool does not deadlock.
 */
public class ConcurrentBatchHelper {

    private ConcurrentBatchHelper() {}

    /**
     * Callback invoked for each batch.
     */
    @FunctionalInterface
    public interface BatchConsumer<T> {
        void accept(List<T> batch) throws Exception;
    }

    /**
     * Executes pre-formed batches concurrently. The calling thread also participates so that a
     * saturated executor does not deadlock. Rejections from the executor are swallowed; the calling
     * thread will handle any unclaimed batches.
     *
     * @param batches        pre-materialized list of batches; must not be modified concurrently
     * @param maxConcurrency maximum number of concurrent workers (including the calling thread)
     * @param executor       executor used to dispatch concurrent batch operations
     * @param batchConsumer  callback invoked for each batch; must be thread-safe
     */
    public static <T> void runConcurrentBatches(
        List<List<T>> batches,
        int maxConcurrency,
        Executor executor,
        BatchConsumer<T> batchConsumer
    ) throws IOException {
        final int nbBatches = batches.size();
        if (nbBatches == 0) {
            return;
        }
        final AtomicInteger nextBatchIndex = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(nbBatches);
        final ConcurrentLinkedQueue<Exception> exceptions = new ConcurrentLinkedQueue<>();

        final Runnable worker = () -> {
            int batchIndex;
            while ((batchIndex = nextBatchIndex.getAndIncrement()) < nbBatches) {
                if (exceptions.isEmpty()) {
                    try {
                        batchConsumer.accept(batches.get(batchIndex));
                    } catch (Exception e) {
                        exceptions.add(e);
                    }
                }
                latch.countDown();
            }
        };

        for (int i = 0; i < Math.min(maxConcurrency - 1, nbBatches - 1); i++) {
            try {
                executor.execute(worker);
            } catch (Exception e) {
                // Rejections are swallowed; the calling thread handles unclaimed batches
            }
        }
        worker.run();

        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            exceptions.add(e);
        }

        if (exceptions.isEmpty() == false) {
            final Iterator<Exception> it = exceptions.iterator();
            final IOException exception = new IOException("Failed to delete blobs", it.next());
            while (it.hasNext()) {
                exception.addSuppressed(it.next());
            }
            throw exception;
        }
    }
}
