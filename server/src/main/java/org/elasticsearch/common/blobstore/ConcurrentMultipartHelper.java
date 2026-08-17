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
        final AtomicInteger nextPartNum = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(nbParts);
        final ConcurrentLinkedQueue<Exception> exceptions = new ConcurrentLinkedQueue<>();

        final Runnable worker = () -> {
            int partNum;
            while ((partNum = nextPartNum.getAndIncrement()) < nbParts) {
                if (exceptions.isEmpty()) {
                    final boolean lastPart = partNum == nbParts - 1;
                    final long curPartSize = lastPart ? lastPartSize : partSize;
                    final long offset = (long) partNum * partSize;
                    try {
                        partConsumer.accept(partNum, offset, curPartSize, lastPart);
                    } catch (Exception e) {
                        exceptions.add(e);
                    }
                }
                latch.countDown();
            }
        };

        for (int i = 0; i < nbParts - 1; i++) {
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
            final IOException exception = new IOException("Failed to upload parts", it.next());
            while (it.hasNext()) {
                exception.addSuppressed(it.next());
            }
            throw exception;
        }
    }
}
