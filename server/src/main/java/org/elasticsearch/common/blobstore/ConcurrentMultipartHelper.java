/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.blobstore;

import org.apache.lucene.search.CheckedIntConsumer;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

public class ConcurrentMultipartHelper {

    private ConcurrentMultipartHelper() {}

    /**
     * Callback invoked for each part of a multipart operation
     */
    @FunctionalInterface
    public interface PartConsumer {
        /**
         * @param partNum   0-based part index
         * @param offset    byte offset of this part within the blob
         * @param partSize  size in bytes of this part
         * @param lastPart  whether this is the final part
         */
        void accept(int partNum, long offset, long partSize, boolean lastPart) throws Exception;
    }

    /**
     * @param blobSize total size of the blob in bytes
     * @param partSize size of each part in bytes
     */
    public static int numberOfParts(long blobSize, long partSize) {
        return Math.toIntExact((blobSize + partSize - 1) / partSize);
    }

    /**
     * Executes a multipart operation concurrently. The calling thread also participates
     *
     * @param blobSize     total size of the blob in bytes
     * @param partSize     size of each part in bytes
     * @param executor     executor used to dispatch concurrent part operations
     * @param partConsumer callback invoked for each part, must be thread-safe
     */
    public static void runConcurrentParts(long blobSize, long partSize, Executor executor, PartConsumer partConsumer) throws IOException {
        final int nbParts = numberOfParts(blobSize, partSize);
        final long lastPartSize = blobSize - (long) (nbParts - 1) * partSize;
        runConcurrentTasks(nbParts, executor, partNum -> {
            final boolean lastPart = partNum == nbParts - 1;
            final long curPartSize = lastPart ? lastPartSize : partSize;
            final long offset = (long) partNum * partSize;
            partConsumer.accept(partNum, offset, curPartSize, lastPart);
        });
    }

    /**
     * Executes {@code tasks} independent tasks concurrently. The calling thread also participates
     *
     * @param tasks        number of tasks to execute
     * @param executor     executor used to dispatch concurrent tasks
     * @param taskConsumer callback invoked per task index, must be thread-safe
     */
    public static void runConcurrentTasks(int tasks, Executor executor, CheckedIntConsumer<Exception> taskConsumer) throws IOException {
        final AtomicInteger nextTask = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(tasks);
        final ConcurrentLinkedQueue<Exception> exceptions = new ConcurrentLinkedQueue<>();

        final Runnable worker = () -> {
            int i;
            while ((i = nextTask.getAndIncrement()) < tasks) {
                if (exceptions.isEmpty()) {
                    try {
                        taskConsumer.accept(i);
                    } catch (Exception e) {
                        exceptions.add(e);
                    }
                }
                latch.countDown();
            }
        };

        for (int i = 0; i < tasks - 1; i++) {
            try {
                executor.execute(worker);
            } catch (Exception e) {
                // Ignore rejections, the calling thread will process unclaimed parts
            }
        }
        // Calling thread also processes tasks
        worker.run();

        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            exceptions.add(e);
        }

        if (exceptions.isEmpty() == false) {
            final Iterator<Exception> it = exceptions.iterator();
            final Exception first = it.next();
            if (first instanceof RuntimeException re) {
                while (it.hasNext()) {
                    re.addSuppressed(it.next());
                }
                throw re;
            }
            final IOException exception = first instanceof IOException ioe
                ? ioe
                : new IOException("Concurrent multipart operation failed", first);
            while (it.hasNext()) {
                exception.addSuppressed(it.next());
            }
            throw exception;
        }
    }
}
