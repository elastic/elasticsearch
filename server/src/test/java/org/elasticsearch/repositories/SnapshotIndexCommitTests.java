/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

public class SnapshotIndexCommitTests extends ESTestCase {

    public void testComplete() throws Exception {
        runCompleteTest(false, null);
        runCompleteTest(true, null);
        runCompleteTest(false, new ElasticsearchException("outer"));
        runCompleteTest(true, new ElasticsearchException("outer"));
    }

    public void testAbort() throws Exception {
        runAbortTest(false, null);
        runAbortTest(true, null);
        runAbortTest(false, new ElasticsearchException("outer"));
        runAbortTest(true, new ElasticsearchException("outer"));
    }

    private void runCompleteTest(boolean throwOnClose, @Nullable Exception outerException) throws Exception {
        final var isClosed = new AtomicBoolean();
        final var indexCommitRef = getSnapshotIndexCommit(throwOnClose, isClosed);

        assertFalse(isClosed.get());
        if (randomBoolean()) {
            assertTrue(indexCommitRef.tryIncRef());
            indexCommitRef.decRef();
        }

        assertOnCompletionBehaviour(throwOnClose, outerException, indexCommitRef);

        assertTrue(isClosed.get());
        assertFalse(indexCommitRef.tryIncRef());

        indexCommitRef.onAbort();
        assertFalse(indexCommitRef.tryIncRef());
    }

    private void runAbortTest(boolean throwOnClose, @Nullable Exception outerException) throws Exception {
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

        assertOnCompletionBehaviour(throwOnClose, outerException, indexCommitRef);
    }

    private SnapshotIndexCommit getSnapshotIndexCommit(boolean throwOnClose, AtomicBoolean isClosed) {
        return new SnapshotIndexCommit(new Engine.IndexCommitRef(null, () -> {
            assertTrue(isClosed.compareAndSet(false, true));
            if (throwOnClose) {
                throw new IOException("simulated");
            }
        }));
    }

    private void assertOnCompletionBehaviour(boolean throwOnClose, @Nullable Exception outerException, SnapshotIndexCommit indexCommitRef)
        throws Exception {
        final var future = new PlainActionFuture<String>();
        if (outerException == null) {
            indexCommitRef.closingBefore(future).onResponse("success");
        } else {
            indexCommitRef.closingBefore(future).onFailure(outerException);
        }
        assertOnCompletionFuture(throwOnClose, outerException, future);
    }

    private void assertOnCompletionFuture(
        boolean throwOnClose,
        @Nullable Exception outerException,
        PlainActionFuture<String> completionFuture
    ) throws Exception {
        assertTrue(completionFuture.isDone());
        if (outerException == null) {
            if (throwOnClose) {
                assertEquals("simulated", expectThrows(ExecutionException.class, IOException.class, completionFuture::get).getMessage());
            } else {
                assertEquals("success", completionFuture.get());
            }
        } else {
            assertSame(outerException, expectThrows(ExecutionException.class, Exception.class, completionFuture::get));
            if (throwOnClose) {
                assertEquals(1, outerException.getSuppressed().length);
                assertEquals("simulated", outerException.getSuppressed()[0].getMessage());
            } else {
                assertEquals(0, outerException.getSuppressed().length);
            }
        }
    }

}
