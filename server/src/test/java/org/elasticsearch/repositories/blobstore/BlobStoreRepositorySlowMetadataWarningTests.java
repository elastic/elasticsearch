/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.blobstore;

import org.apache.logging.log4j.Level;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.threadpool.Scheduler;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class BlobStoreRepositorySlowMetadataWarningTests extends ESTestCase {

    private final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue();
    private final ShardId shardId = new ShardId(randomIdentifier(), randomUUID(), 0);
    private final SnapshotId snapshotId = new SnapshotId(randomIdentifier(), randomUUID());
    private BlobStoreRepository repository;

    @Before
    public void createRepository() {
        repository = (BlobStoreRepository) TestUtils.createRepository(randomProjectIdOrDefault(), createTempDir(), xContentRegistry());
        // The repository's ThreadPool is a mock, which cannot schedule, so delegate scheduling to the deterministic task queue
        // so that tests can advance time without waiting.
        doAnswer(
            invocation -> deterministicTaskQueue.getThreadPool()
                .schedule(invocation.getArgument(0), invocation.getArgument(1), invocation.getArgument(2))
        ).when(repository.threadPool).schedule(any(Runnable.class), any(TimeValue.class), any(Executor.class));
    }

    @After
    public void closeRepository() {
        repository.close();
    }

    public void testWarnsWithHotThreadsWhenMetadataReadIsSlow() {
        try (var mockLog = MockLog.capture(BlobStoreRepository.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "slow metadata warning",
                    BlobStoreRepository.class.getCanonicalName(),
                    Level.WARN,
                    "[" + shardId + "][" + snapshotId + "] still reading file metadata after [2m]"
                )
            );
            mockLog.addExpectation(
                new MockLog.PatternSeenEventExpectation(
                    "hot threads",
                    BlobStoreRepository.class.getCanonicalName(),
                    Level.WARN,
                    Pattern.quote("hot threads while reading file metadata for shard snapshot [" + shardId + "][" + snapshotId + "]") + ".*"
                )
            );

            try (Releasable ignored = repository.warnIfReadingMetadataIsSlow(shardId, snapshotId)) {
                // not due immediately
                deterministicTaskQueue.runAllRunnableTasks();
                assertTrue(deterministicTaskQueue.hasDeferredTasks());
                assertFalse(deterministicTaskQueue.hasRunnableTasks());

                deterministicTaskQueue.runAllTasks();
                assertThat(deterministicTaskQueue.getCurrentTimeMillis(), equalTo(TimeValue.timeValueMinutes(2).millis()));
            }

            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testScheduledCallbackDoesNothingIfReadFinishedBeforeItRan() {
        // Capture the callback so it can be run after the read has finished, as happens when cancellation loses a race with the executor
        final var callback = new AtomicReference<Runnable>();
        doAnswer(invocation -> {
            callback.set(invocation.getArgument(0));
            return mock(Scheduler.ScheduledCancellable.class);
        }).when(repository.threadPool).schedule(any(Runnable.class), any(TimeValue.class), any(Executor.class));

        try (var mockLog = MockLog.capture(BlobStoreRepository.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "no warning",
                    BlobStoreRepository.class.getCanonicalName(),
                    Level.WARN,
                    "*still reading file metadata*"
                )
            );

            repository.warnIfReadingMetadataIsSlow(shardId, snapshotId).close();
            callback.get().run();

            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testDoesNotWarnWhenMetadataReadCompletesInTime() {
        try (var mockLog = MockLog.capture(BlobStoreRepository.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "no warning",
                    BlobStoreRepository.class.getCanonicalName(),
                    Level.WARN,
                    "*still reading file metadata*"
                )
            );

            repository.warnIfReadingMetadataIsSlow(shardId, snapshotId).close();
            deterministicTaskQueue.runAllTasks();

            mockLog.assertAllExpectationsMatched();
        }
    }
}
