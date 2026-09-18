/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch.chunk;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

public class ActiveFetchPhaseTasksTests extends ESTestCase {

    private static final ShardId TEST_SHARD_ID = new ShardId(new Index("test-index", "test-uuid"), 0);

    public void testAcquireRegisteredStream() {
        ActiveFetchPhaseTasks tasks = new ActiveFetchPhaseTasks();
        FetchPhaseResponseStream stream = new FetchPhaseResponseStream(0, 10, new NoopCircuitBreaker("test"));
        Releasable registration = tasks.registerResponseBuilder(123L, TEST_SHARD_ID, stream);

        try {
            FetchPhaseResponseStream acquired = tasks.acquireResponseStream(123L, TEST_SHARD_ID);
            assertSame(stream, acquired);
            assertTrue(acquired.hasReferences());
            acquired.decRef();
            registration.close();

            expectThrows(ResourceNotFoundException.class, () -> tasks.acquireResponseStream(123L, TEST_SHARD_ID));
        } finally {
            stream.decRef();
        }
    }

    public void testDuplicateRegisterThrows() {
        ActiveFetchPhaseTasks tasks = new ActiveFetchPhaseTasks();
        FetchPhaseResponseStream first = new FetchPhaseResponseStream(0, 10, new NoopCircuitBreaker("test"));
        FetchPhaseResponseStream second = new FetchPhaseResponseStream(0, 10, new NoopCircuitBreaker("test"));
        Releasable registration = tasks.registerResponseBuilder(123L, TEST_SHARD_ID, first);

        try {
            Exception e = expectThrows(IllegalStateException.class, () -> tasks.registerResponseBuilder(123L, TEST_SHARD_ID, second));
            assertEquals("already executing fetch task [123]", e.getMessage());
        } finally {
            registration.close();
            first.decRef();
            second.decRef();
        }
    }

    public void testCloseRegistrationRemovesTaskAndAllowsReregister() {
        ActiveFetchPhaseTasks tasks = new ActiveFetchPhaseTasks();
        FetchPhaseResponseStream stream = new FetchPhaseResponseStream(0, 10, new NoopCircuitBreaker("test"));
        Releasable registration = tasks.registerResponseBuilder(123L, TEST_SHARD_ID, stream);

        try {
            registration.close();
            expectThrows(ResourceNotFoundException.class, () -> tasks.acquireResponseStream(123L, TEST_SHARD_ID));

            FetchPhaseResponseStream replacement = new FetchPhaseResponseStream(0, 10, new NoopCircuitBreaker("test"));
            Releasable secondRegistration = tasks.registerResponseBuilder(123L, TEST_SHARD_ID, replacement);
            try {
                FetchPhaseResponseStream acquired = tasks.acquireResponseStream(123L, TEST_SHARD_ID);
                assertSame(replacement, acquired);
                acquired.decRef();
            } finally {
                secondRegistration.close();
                replacement.decRef();
            }
        } finally {
            stream.decRef();
        }
    }

    public void testAcquireMissingTaskThrowsResourceNotFound() {
        ActiveFetchPhaseTasks tasks = new ActiveFetchPhaseTasks();
        expectThrows(ResourceNotFoundException.class, () -> tasks.acquireResponseStream(999L, TEST_SHARD_ID));
    }

    public void testAcquireFailsWhenStreamAlreadyClosed() {
        ActiveFetchPhaseTasks tasks = new ActiveFetchPhaseTasks();
        FetchPhaseResponseStream stream = new FetchPhaseResponseStream(0, 10, new NoopCircuitBreaker("test"));
        Releasable registration = tasks.registerResponseBuilder(123L, TEST_SHARD_ID, stream);
        registration.close();

        try {
            expectThrows(ResourceNotFoundException.class, () -> tasks.acquireResponseStream(123L, TEST_SHARD_ID));
        } finally {
            stream.decRef();
        }
    }
}
