/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

public class SnapshotIndexCommitTests extends ESTestCase {

    public void testCompleteAndCloseCleanly() throws Exception {
        runCompleteTest(false);
    }

    public void testCompleteAndFailOnClose() throws Exception {
        runCompleteTest(true);
    }

    public void testAbortAndCloseCleanly() throws Exception {
        runAbortTest(false);
    }

    public void testAbortAndFailOnClose() throws Exception {
        runAbortTest(true);
    }

    public void testConcurrentAbortAndCompleteCleanly() throws Exception {
        runConcurrentTest(false);
    }

    public void testConcurrentAbortAndCompleteWithFailOnClose() throws Exception {
        runConcurrentTest(true);
    }

    private void runCompleteTest(boolean throwOnClose) throws Exception {
        final var isClosed = new AtomicBoolean();
        final var indexCommitRef = getSnapshotIndexCommit(throwOnClose, isClosed);

        assertFalse(isClosed.get());
        if (randomBoolean()) {
            assertTrue(indexCommitRef.tryIncRef());
            indexCommitRef.decRef();
        }

        assertOnCompletionBehaviour(throwOnClose, indexCommitRef);

        assertTrue(isClosed.get());
        assertFalse(indexCommitRef.tryIncRef());

        indexCommitRef.onAbort();
        assertFalse(indexCommitRef.tryIncRef());
    }

    private void runAbortTest(boolean throwOnClose) throws Exception {
        final var isClosed = new AtomicBoolean();
        final var indexCommitRef = getSnapshotIndexCommit(throwOnClose, isClosed);

        assertFalse(isClosed.get());
        assertTrue(indexCommitRef.tryIncRef());

        indexCommitRef.onAbort();
        assertFalse(isClosed.get());

        assertTrue(indexCommitRef.tryIncRef());
        indexCommitRef.decRef();
        indexCommitRef.decRef();

        assertTrue(isClosed.get());

        assertOnCompletionBehaviour(throwOnClose, indexCommitRef);
    }

    private void runConcurrentTest(boolean throwOnClose) throws Exception {
        final var isClosed = new AtomicBoolean();
        final var indexCommitRef = getSnapshotIndexCommit(throwOnClose, isClosed);

        final var completeFuture = new PlainActionFuture<Void>();
        final var barrier = new CyclicBarrier(2);
        final var completeThread = new Thread(() -> {
            safeAwait(barrier);
            indexCommitRef.onCompletion(completeFuture);
        });
        completeThread.start();

        final var abortThread = new Thread(() -> {
            safeAwait(barrier);
            indexCommitRef.onAbort();
        });
        abortThread.start();

        completeThread.join();
        abortThread.join();

        assertOnCompletionFuture(throwOnClose, completeFuture);
    }

    private SnapshotIndexCommit getSnapshotIndexCommit(boolean throwOnClose, AtomicBoolean isClosed) {
        return new SnapshotIndexCommit(new Engine.IndexCommitRef(null, () -> {
            assertTrue(isClosed.compareAndSet(false, true));
            if (throwOnClose) {
                throw new IOException("simulated");
            }
        }));
    }

    private void assertOnCompletionBehaviour(boolean throwOnClose, SnapshotIndexCommit indexCommitRef) throws Exception {
        final var future = new PlainActionFuture<Void>();
        indexCommitRef.onCompletion(future);
        assertOnCompletionFuture(throwOnClose, future);
    }

    private void assertOnCompletionFuture(boolean throwOnClose, PlainActionFuture<Void> completionFuture) throws Exception {
        assertTrue(completionFuture.isDone());
        if (throwOnClose) {
            assertEquals("simulated", expectThrows(ExecutionException.class, IOException.class, completionFuture::get).getMessage());
        } else {
            completionFuture.get();
        }
    }

}
