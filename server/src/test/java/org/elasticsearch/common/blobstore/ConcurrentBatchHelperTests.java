/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.blobstore;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

public class ConcurrentBatchHelperTests extends ESTestCase {

    public void testEmptyBatchListIsNoOp() throws IOException {
        ConcurrentBatchHelper.runConcurrentBatches(List.of(), randomIntBetween(1, 10), command -> {
            throw new AssertionError("executor must not be called for empty input");
        }, batch -> { throw new AssertionError("consumer must not be called for empty input"); });
    }

    public void testAllBatchesExecutedWhenExecutorRejects() throws IOException {
        final int nbBatches = randomIntBetween(2, 20);
        final List<List<String>> batches = makeBatches(nbBatches);
        final AtomicInteger batchesExecuted = new AtomicInteger(0);

        ConcurrentBatchHelper.runConcurrentBatches(batches, randomIntBetween(2, 10), command -> {
            throw new RejectedExecutionException("executor is full");
        }, batch -> batchesExecuted.incrementAndGet());

        assertEquals(nbBatches, batchesExecuted.get());
    }

    public void testAllBatchesExecuted() throws IOException {
        final int nbBatches = randomIntBetween(1, 20);
        final List<List<String>> batches = makeBatches(nbBatches);
        final AtomicInteger batchesExecuted = new AtomicInteger(0);

        ConcurrentBatchHelper.runConcurrentBatches(
            batches,
            randomIntBetween(1, nbBatches + 1),
            Runnable::run,
            batch -> batchesExecuted.incrementAndGet()
        );

        assertEquals(nbBatches, batchesExecuted.get());
    }

    public void testExceptionFromBatchPropagatesAsIOException() {
        final List<List<String>> batches = makeBatches(randomIntBetween(1, 5));
        final RuntimeException cause = new RuntimeException("batch failed");

        final IOException e = expectThrows(
            IOException.class,
            () -> ConcurrentBatchHelper.runConcurrentBatches(batches, 1, Runnable::run, batch -> {
                throw cause;
            })
        );

        assertEquals("Failed to delete blobs", e.getMessage());
        assertSame(cause, e.getCause());
    }

    public void testSubsequentBatchesAreSkippedAfterFailure() {
        // With maxConcurrency=1 only the calling thread runs, so batches are processed
        // sequentially. Once the first batch throws, the worker sees a non-empty exception
        // queue and skips remaining batches (still counting them down so the latch drains).
        final List<List<String>> batches = makeBatches(randomIntBetween(2, 10));
        final RuntimeException cause = new RuntimeException("first batch failed");
        final AtomicInteger callCount = new AtomicInteger();

        final IOException e = expectThrows(
            IOException.class,
            () -> ConcurrentBatchHelper.runConcurrentBatches(batches, 1, Runnable::run, batch -> {
                callCount.incrementAndGet();
                throw cause;
            })
        );

        assertEquals("Failed to delete blobs", e.getMessage());
        assertSame(cause, e.getCause());
        // Only the first batch was actually processed; the rest were skipped.
        assertEquals(1, callCount.get());
    }

    public void testSingleBatchNeverUsesExecutor() throws IOException {
        // With nbBatches=1, min(maxConcurrency-1, nbBatches-1) = 0, so the executor is never called.
        final AtomicInteger batchesExecuted = new AtomicInteger();
        ConcurrentBatchHelper.runConcurrentBatches(makeBatches(1), randomIntBetween(1, 100), command -> {
            throw new AssertionError("executor must not be called for a single batch");
        }, batch -> batchesExecuted.incrementAndGet());
        assertEquals(1, batchesExecuted.get());
    }

    public void testMaxConcurrencyExceedsBatchCount() throws IOException {
        // Dispatcher caps at min(maxConcurrency-1, nbBatches-1) so over-large maxConcurrency
        // does not cause excess executor submissions or missed batches.
        final int nbBatches = randomIntBetween(2, 5);
        final AtomicInteger batchesExecuted = new AtomicInteger();
        ConcurrentBatchHelper.runConcurrentBatches(
            makeBatches(nbBatches),
            nbBatches * 10,
            Runnable::run,
            batch -> batchesExecuted.incrementAndGet()
        );
        assertEquals(nbBatches, batchesExecuted.get());
    }

    public void testBatchContentsArePassedCorrectly() throws IOException {
        final int nbBatches = randomIntBetween(1, 10);
        final List<List<String>> batches = makeBatches(nbBatches);
        final Set<String> received = Collections.synchronizedSet(new HashSet<>());

        ConcurrentBatchHelper.runConcurrentBatches(batches, randomIntBetween(1, nbBatches + 1), Runnable::run, received::addAll);

        final Set<String> expected = new HashSet<>();
        for (int i = 0; i < nbBatches; i++) {
            expected.add("item-" + i);
        }
        assertEquals(expected, received);
    }

    public void testCheckedIOExceptionPropagates() {
        // BatchConsumer.accept throws a checked IOException; it should surface as the cause
        // of the wrapper rather than being double-wrapped.
        final IOException cause = new IOException("IO error during batch");
        final IOException e = expectThrows(
            IOException.class,
            () -> ConcurrentBatchHelper.runConcurrentBatches(makeBatches(1), 1, Runnable::run, batch -> {
                throw cause;
            })
        );
        assertEquals("Failed to delete blobs", e.getMessage());
        assertSame(cause, e.getCause());
    }

    public void testPartialExecutorRejection() throws IOException {
        // Executor accepts the first few dispatches and rejects the rest.
        // The calling thread must handle unclaimed batches so the total still equals nbBatches.
        final int nbBatches = randomIntBetween(3, 10);
        final int acceptCount = randomIntBetween(1, nbBatches - 1);
        final AtomicInteger executorCalls = new AtomicInteger();
        final AtomicInteger batchesExecuted = new AtomicInteger();

        ConcurrentBatchHelper.runConcurrentBatches(makeBatches(nbBatches), nbBatches, command -> {
            if (executorCalls.getAndIncrement() < acceptCount) {
                command.run();
            } else {
                throw new RejectedExecutionException("pool full");
            }
        }, batch -> batchesExecuted.incrementAndGet());

        assertEquals(nbBatches, batchesExecuted.get());
    }

    private static List<List<String>> makeBatches(int count) {
        final List<List<String>> batches = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            batches.add(List.of("item-" + i));
        }
        return batches;
    }
}
