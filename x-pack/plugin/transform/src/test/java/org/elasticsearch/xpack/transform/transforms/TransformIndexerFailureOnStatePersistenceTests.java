/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.exception.ElasticsearchTimeoutException;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.transform.transforms.SettingsConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpoint;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigTests;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerPosition;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformProgress;
import org.elasticsearch.xpack.core.transform.transforms.TransformState;
import org.elasticsearch.xpack.core.transform.transforms.TransformStoredDoc;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskState;
import org.elasticsearch.xpack.core.transform.transforms.persistence.TransformInternalIndexConstants;
import org.elasticsearch.xpack.transform.TransformExtension;
import org.elasticsearch.xpack.transform.TransformNode;
import org.elasticsearch.xpack.transform.TransformServices;
import org.elasticsearch.xpack.transform.checkpoint.CheckpointProvider;
import org.elasticsearch.xpack.transform.checkpoint.TransformCheckpointService;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.InMemoryTransformConfigManager;
import org.elasticsearch.xpack.transform.persistence.SeqNoPrimaryTermAndIndex;
import org.elasticsearch.xpack.transform.persistence.TransformConfigManager;
import org.elasticsearch.xpack.transform.persistence.TransformStatePersistenceException;
import org.elasticsearch.xpack.transform.transforms.scheduling.TransformScheduler;

import java.time.Clock;
import java.time.Instant;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isA;
import static org.mockito.Mockito.mock;

public class TransformIndexerFailureOnStatePersistenceTests extends ESTestCase {

    private static class MockClientTransformIndexer extends ClientTransformIndexer {

        MockClientTransformIndexer(
            ThreadPool threadPool,
            ClusterService clusterService,
            IndexNameExpressionResolver indexNameExpressionResolver,
            TransformExtension transformExtension,
            TransformServices transformServices,
            CheckpointProvider checkpointProvider,
            AtomicReference<IndexerState> initialState,
            TransformIndexerPosition initialPosition,
            ParentTaskAssigningClient client,
            TransformIndexerStats initialStats,
            TransformConfig transformConfig,
            TransformProgress transformProgress,
            TransformCheckpoint lastCheckpoint,
            TransformCheckpoint nextCheckpoint,
            SeqNoPrimaryTermAndIndex seqNoPrimaryTermAndIndex,
            TransformContext context,
            boolean shouldStopAtCheckpoint
        ) {
            super(
                threadPool,
                clusterService,
                indexNameExpressionResolver,
                transformExtension,
                transformServices,
                checkpointProvider,
                initialState,
                initialPosition,
                client,
                initialStats,
                transformConfig,
                transformProgress,
                lastCheckpoint,
                nextCheckpoint,
                seqNoPrimaryTermAndIndex,
                context,
                shouldStopAtCheckpoint
            );
        }

        protected boolean triggerSaveState() {
            // persist every iteration for testing
            return true;
        }

        public int getStatePersistenceFailures() {
            return context.getStatePersistenceFailureCount();
        }
    }

    private static class FailingToPutStoredDocTransformConfigManager extends InMemoryTransformConfigManager {

        private final Set<Integer> failAt;
        private final Exception exception;
        private int persistenceCallCount = 0;

        FailingToPutStoredDocTransformConfigManager(Set<Integer> failAt, Exception exception) {
            this.failAt = failAt;
            this.exception = exception;
        }

        @Override
        public void putOrUpdateTransformStoredDoc(
            TransformStoredDoc storedDoc,
            SeqNoPrimaryTermAndIndex seqNoPrimaryTermAndIndex,
            ActionListener<SeqNoPrimaryTermAndIndex> listener
        ) {
            if (failAt.contains(persistenceCallCount++)) {
                listener.onFailure(
                    new TransformStatePersistenceException(
                        "FailingToPutStoredDocTransformConfigManager.putOrUpdateTransformStoredDoc is intentionally throwing an exception",
                        exception
                    )
                );
            } else {
                super.putOrUpdateTransformStoredDoc(storedDoc, seqNoPrimaryTermAndIndex, listener);
            }
        }
    }

    private static class SeqNoCheckingTransformConfigManager extends InMemoryTransformConfigManager {

        private long seqNo = -1;
        private long primaryTerm = 0;

        SeqNoCheckingTransformConfigManager() {}

        @Override
        public void putOrUpdateTransformStoredDoc(
            TransformStoredDoc storedDoc,
            SeqNoPrimaryTermAndIndex seqNoPrimaryTermAndIndex,
            ActionListener<SeqNoPrimaryTermAndIndex> listener
        ) {
            if (seqNo != -1) {
                if (seqNoPrimaryTermAndIndex.getSeqNo() != seqNo || seqNoPrimaryTermAndIndex.getPrimaryTerm() != primaryTerm) {
                    listener.onFailure(
                        new TransformStatePersistenceException(
                            "SeqNoCheckingTransformConfigManager.putOrUpdateTransformStoredDoc is intentionally throwing an exception",
                            new VersionConflictEngineException(new ShardId("index", "indexUUID", 42), "some_id", 45L, 44L, 43L, 42L)
                        )
                    );
                    return;
                }
            }

            super.putOrUpdateTransformStoredDoc(storedDoc, seqNoPrimaryTermAndIndex, ActionListener.wrap(r -> {
                // always inc seqNo, primaryTerm at random
                if (randomBoolean()) {
                    primaryTerm++;
                }
                listener.onResponse(new SeqNoPrimaryTermAndIndex(++seqNo, primaryTerm, CURRENT_INDEX));
            }, listener::onFailure));
        }

        @Override
        public void getTransformStoredDoc(
            String transformId,
            boolean allowNoMatch,
            ActionListener<Tuple<TransformStoredDoc, SeqNoPrimaryTermAndIndex>> resultListener
        ) {
            super.getTransformStoredDoc(
                transformId,
                allowNoMatch,
                ActionListener.wrap(
                    r -> resultListener.onResponse(new Tuple<>(r.v1(), new SeqNoPrimaryTermAndIndex(seqNo, primaryTerm, CURRENT_INDEX))),
                    resultListener::onFailure
                )
            );
        }
    }

    public void testStatePersistenceErrorHandling() throws InterruptedException {
        TransformConfig config = TransformConfigTests.randomTransformConfigWithSettings(
            new SettingsConfig(
                randomBoolean() ? null : randomIntBetween(10, 10_000),
                randomBoolean() ? null : randomFloat(),
                randomBoolean(),
                randomBoolean(),
                randomBoolean(),
                randomBoolean(),
                2,
                false
            )
        );
        AtomicReference<TransformTaskState> state = new AtomicReference<>(TransformTaskState.STARTED);
        TransformContext.Listener contextListener = new TransformContext.Listener() {
            @Override
            public void shutdown() {}

            @Override
            public void failureCountChanged() {}

            @Override
            public void fail(Throwable exception, String failureMessage, ActionListener<Void> listener) {
                state.set(TransformTaskState.FAILED);
            }
        };

        {
            TransformContext context = new TransformContext(state.get(), null, 0, contextListener);
            Exception exceptionToThrow = randomBoolean()
                ? new VersionConflictEngineException(new ShardId("index", "indexUUID", 42), "some_id", 45L, 44L, 43L, 42L)
                : new ElasticsearchTimeoutException("timeout");
            TransformConfigManager configManager = new FailingToPutStoredDocTransformConfigManager(Set.of(0, 1, 2, 3), exceptionToThrow);
            try (var threadPool = createThreadPool()) {
                final var client = new NoOpClient(threadPool);

                MockClientTransformIndexer indexer = new MockClientTransformIndexer(
                    mock(ThreadPool.class),
                    mock(ClusterService.class),
                    mock(IndexNameExpressionResolver.class),
                    mock(TransformExtension.class),
                    new TransformServices(
                        configManager,
                        mock(TransformCheckpointService.class),
                        mock(TransformAuditor.class),
                        new TransformScheduler(Clock.systemUTC(), mock(ThreadPool.class), Settings.EMPTY, TimeValue.ZERO),
                        mock(TransformNode.class)
                    ),
                    mock(CheckpointProvider.class),
                    new AtomicReference<>(IndexerState.STOPPED),
                    null,
                    new ParentTaskAssigningClient(client, new TaskId("dummy-node:123456")),
                    mock(TransformIndexerStats.class),
                    config,
                    null,
                    new TransformCheckpoint(
                        "transform",
                        Instant.now().toEpochMilli(),
                        0L,
                        Collections.emptyMap(),
                        Instant.now().toEpochMilli()
                    ),
                    new TransformCheckpoint(
                        "transform",
                        Instant.now().toEpochMilli(),
                        2L,
                        Collections.emptyMap(),
                        Instant.now().toEpochMilli()
                    ),
                    new SeqNoPrimaryTermAndIndex(1, 1, TransformInternalIndexConstants.LATEST_INDEX_NAME),
                    context,
                    false
                );

                this.<Void>assertAsyncFailure(
                    listener -> indexer.persistState(
                        new TransformState(TransformTaskState.STARTED, IndexerState.INDEXING, null, 42, null, null, null, false, null),
                        listener
                    ),
                    e -> {
                        assertThat(ExceptionsHelper.unwrapCause(e), isA(exceptionToThrow.getClass()));
                        assertThat(state.get(), equalTo(TransformTaskState.STARTED));
                        assertThat(indexer.getStatePersistenceFailures(), equalTo(1));
                    }
                );

                this.<Void>assertAsyncFailure(
                    listener -> indexer.persistState(
                        new TransformState(TransformTaskState.STARTED, IndexerState.INDEXING, null, 42, null, null, null, false, null),
                        listener
                    ),
                    e -> {
                        assertThat(ExceptionsHelper.unwrapCause(e), isA(exceptionToThrow.getClass()));
                        assertThat(state.get(), equalTo(TransformTaskState.STARTED));
                        assertThat(indexer.getStatePersistenceFailures(), equalTo(2));
                    }
                );

                this.<Void>assertAsyncFailure(
                    listener -> indexer.persistState(
                        new TransformState(TransformTaskState.STARTED, IndexerState.INDEXING, null, 42, null, null, null, false, null),
                        listener
                    ),
                    e -> {
                        assertThat(ExceptionsHelper.unwrapCause(e), isA(exceptionToThrow.getClass()));
                        assertThat(state.get(), equalTo(TransformTaskState.FAILED));
                        assertThat(indexer.getStatePersistenceFailures(), equalTo(3));
                    }
                );
            }
        }

        // test reset on success
        {
            state.set(TransformTaskState.STARTED);
            TransformContext context = new TransformContext(state.get(), null, 0, contextListener);
            Exception exceptionToThrow = randomBoolean()
                ? new VersionConflictEngineException(new ShardId("index", "indexUUID", 42), "some_id", 45L, 44L, 43L, 42L)
                : new ElasticsearchTimeoutException("timeout");
            TransformConfigManager configManager = new FailingToPutStoredDocTransformConfigManager(Set.of(0, 2, 3, 4), exceptionToThrow);
            try (var threadPool = createThreadPool()) {
                final var client = new NoOpClient(threadPool);
                MockClientTransformIndexer indexer = new MockClientTransformIndexer(
                    mock(ThreadPool.class),
                    mock(ClusterService.class),
                    mock(IndexNameExpressionResolver.class),
                    mock(TransformExtension.class),
                    new TransformServices(
                        configManager,
                        mock(TransformCheckpointService.class),
                        mock(TransformAuditor.class),
                        new TransformScheduler(Clock.systemUTC(), mock(ThreadPool.class), Settings.EMPTY, TimeValue.ZERO),
                        mock(TransformNode.class)
                    ),
                    mock(CheckpointProvider.class),
                    new AtomicReference<>(IndexerState.STOPPED),
                    null,
                    new ParentTaskAssigningClient(client, new TaskId("dummy-node:123456")),
                    mock(TransformIndexerStats.class),
                    config,
                    null,
                    new TransformCheckpoint(
                        "transform",
                        Instant.now().toEpochMilli(),
                        0L,
                        Collections.emptyMap(),
                        Instant.now().toEpochMilli()
                    ),
                    new TransformCheckpoint(
                        "transform",
                        Instant.now().toEpochMilli(),
                        2L,
                        Collections.emptyMap(),
                        Instant.now().toEpochMilli()
                    ),
                    new SeqNoPrimaryTermAndIndex(1, 1, TransformInternalIndexConstants.LATEST_INDEX_NAME),
                    context,
                    false
                );

                this.<Void>assertAsyncFailure(
                    listener -> indexer.persistState(
                        new TransformState(TransformTaskState.STARTED, IndexerState.INDEXING, null, 42, null, null, null, false, null),
                        listener
                    ),
                    e -> {
                        assertThat(ExceptionsHelper.unwrapCause(e), isA(exceptionToThrow.getClass()));
                        assertThat(state.get(), equalTo(TransformTaskState.STARTED));
                        assertThat(indexer.getStatePersistenceFailures(), equalTo(1));
                    }
                );

                // succeed
                this.<Void>assertAsync(
                    listener -> indexer.persistState(
                        new TransformState(TransformTaskState.STARTED, IndexerState.INDEXING, null, 42, null, null, null, false, null),
                        listener
                    ),
                    r -> {
                        assertThat(state.get(), equalTo(TransformTaskState.STARTED));
                        assertThat(indexer.getStatePersistenceFailures(), equalTo(0));
                    }
                );

                // fail again
                this.<Void>assertAsyncFailure(
                    listener -> indexer.persistState(
                        new TransformState(TransformTaskState.STARTED, IndexerState.INDEXING, null, 42, null, null, null, false, null),
                        listener
                    ),
                    e -> {
                        assertThat(ExceptionsHelper.unwrapCause(e), isA(exceptionToThrow.getClass()));
                        assertThat(state.get(), equalTo(TransformTaskState.STARTED));
                        assertThat(indexer.getStatePersistenceFailures(), equalTo(1));
                    }
                );

                this.<Void>assertAsyncFailure(
                    listener -> indexer.persistState(
                        new TransformState(TransformTaskState.STARTED, IndexerState.INDEXING, null, 42, null, null, null, false, null),
                        listener
                    ),
                    e -> {
                        assertThat(ExceptionsHelper.unwrapCause(e), isA(exceptionToThrow.getClass()));
                        assertThat(state.get(), equalTo(TransformTaskState.STARTED));
                        assertThat(indexer.getStatePersistenceFailures(), equalTo(2));
                    }
                );

                this.<Void>assertAsyncFailure(
                    listener -> indexer.persistState(
                        new TransformState(TransformTaskState.STARTED, IndexerState.INDEXING, null, 42, null, null, null, false, null),
                        listener
                    ),
                    e -> {
                        assertThat(ExceptionsHelper.unwrapCause(e), isA(exceptionToThrow.getClass()));
                        assertThat(state.get(), equalTo(TransformTaskState.FAILED));
                        assertThat(indexer.getStatePersistenceFailures(), equalTo(3));
                    }
                );
            }

        }
    }

    public void testStatePersistenceRecovery() throws InterruptedException {
        TransformConfig config = TransformConfigTests.randomTransformConfigWithSettings(
            new SettingsConfig(
                randomBoolean() ? null : randomIntBetween(10, 10_000),
                randomBoolean() ? null : randomFloat(),
                randomBoolean(),
                randomBoolean(),
                randomBoolean(),
                randomBoolean(),
                2,
                false
            )
        );
        AtomicReference<TransformTaskState> state = new AtomicReference<>(TransformTaskState.STARTED);
        TransformContext.Listener contextListener = new TransformContext.Listener() {
            @Override
            public void shutdown() {}

            @Override
            public void failureCountChanged() {}

            @Override
            public void fail(Throwable exception, String failureMessage, ActionListener<Void> listener) {
                state.set(TransformTaskState.FAILED);
            }
        };

        TransformContext context = new TransformContext(state.get(), null, 0, contextListener);
        TransformConfigManager configManager = new SeqNoCheckingTransformConfigManager();

        try (var threadPool = createThreadPool()) {
            final var client = new NoOpClient(threadPool);
            MockClientTransformIndexer indexer = new MockClientTransformIndexer(
                mock(ThreadPool.class),
                mock(ClusterService.class),
                mock(IndexNameExpressionResolver.class),
                mock(TransformExtension.class),
                new TransformServices(
                    configManager,
                    mock(TransformCheckpointService.class),
                    mock(TransformAuditor.class),
                    new TransformScheduler(Clock.systemUTC(), mock(ThreadPool.class), Settings.EMPTY, TimeValue.ZERO),
                    mock(TransformNode.class)
                ),
                mock(CheckpointProvider.class),
                new AtomicReference<>(IndexerState.STOPPED),
                null,
                new ParentTaskAssigningClient(client, new TaskId("dummy-node:123456")),
                mock(TransformIndexerStats.class),
                config,
                null,
                new TransformCheckpoint(
                    "transform",
                    Instant.now().toEpochMilli(),
                    0L,
                    Collections.emptyMap(),
                    Instant.now().toEpochMilli()
                ),
                new TransformCheckpoint(
                    "transform",
                    Instant.now().toEpochMilli(),
                    2L,
                    Collections.emptyMap(),
                    Instant.now().toEpochMilli()
                ),
                new SeqNoPrimaryTermAndIndex(1, 1, TransformInternalIndexConstants.LATEST_INDEX_NAME),
                context,
                false
            );

            // succeed
            this.<Void>assertAsync(
                listener -> indexer.persistState(
                    new TransformState(TransformTaskState.STARTED, IndexerState.INDEXING, null, 42, null, null, null, false, null),
                    listener
                ),
                r -> {
                    assertThat(state.get(), equalTo(TransformTaskState.STARTED));
                    assertThat(indexer.getStatePersistenceFailures(), equalTo(0));
                }
            );

            // push a new state outside the indexer
            this.<SeqNoPrimaryTermAndIndex>assertAsync(
                listener -> configManager.putOrUpdateTransformStoredDoc(
                    new TransformStoredDoc(
                        config.getId(),
                        new TransformState(TransformTaskState.STARTED, IndexerState.INDEXING, null, 42, null, null, null, false, null),
                        indexer.getStats()
                    ),
                    indexer.getSeqNoPrimaryTermAndIndex(),
                    listener
                ),
                seqNoPrimaryTermAndIndex -> assertThat(
                    seqNoPrimaryTermAndIndex.getSeqNo(),
                    equalTo(indexer.getSeqNoPrimaryTermAndIndex().getSeqNo() + 1)
                )
            );

            // state persistence should fail with a version conflict
            this.<Void>assertAsyncFailure(
                listener -> indexer.persistState(
                    new TransformState(TransformTaskState.STARTED, IndexerState.INDEXING, null, 42, null, null, null, false, null),
                    listener
                ),
                e -> {
                    assertThat(ExceptionsHelper.unwrapCause(e), isA(VersionConflictEngineException.class));
                    assertThat(state.get(), equalTo(TransformTaskState.STARTED));
                    assertThat(indexer.getStatePersistenceFailures(), equalTo(1));
                }
            );

            // recovered
            this.<Void>assertAsync(
                listener -> indexer.persistState(
                    new TransformState(TransformTaskState.STARTED, IndexerState.INDEXING, null, 42, null, null, null, false, null),
                    listener
                ),
                r -> {
                    assertThat(indexer.getSeqNoPrimaryTermAndIndex().getSeqNo(), equalTo(2L));
                    assertThat(state.get(), equalTo(TransformTaskState.STARTED));
                    assertThat(indexer.getStatePersistenceFailures(), equalTo(0));
                }
            );

            // succeed
            this.<Void>assertAsync(
                listener -> indexer.persistState(
                    new TransformState(TransformTaskState.STARTED, IndexerState.INDEXING, null, 42, null, null, null, false, null),
                    listener
                ),
                r -> {
                    assertThat(indexer.getSeqNoPrimaryTermAndIndex().getSeqNo(), equalTo(3L));
                    assertThat(state.get(), equalTo(TransformTaskState.STARTED));
                    assertThat(indexer.getStatePersistenceFailures(), equalTo(0));
                }
            );
        }

    }

    private <T> void assertAsync(Consumer<ActionListener<T>> function, Consumer<T> furtherTests) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean listenerCalled = new AtomicBoolean(false);

        LatchedActionListener<T> listener = new LatchedActionListener<>(ActionListener.wrap(r -> {
            assertTrue("listener called more than once", listenerCalled.compareAndSet(false, true));
            furtherTests.accept(r);
        }, e -> {
            assertTrue("listener called more than once", listenerCalled.compareAndSet(false, true));
            fail("got unexpected exception: " + e);
        }), latch);

        function.accept(listener);
        assertTrue("timed out after 5s", latch.await(5, TimeUnit.SECONDS));
    }

    private <T> void assertAsyncFailure(Consumer<ActionListener<T>> function, Consumer<Exception> failureConsumer)
        throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean listenerCalled = new AtomicBoolean(false);

        LatchedActionListener<T> listener = new LatchedActionListener<>(ActionListener.wrap(r -> {
            assertTrue("listener called more than once", listenerCalled.compareAndSet(false, true));
            fail("got unexpected response: " + r);
        }, e -> {
            assertTrue("listener called more than once", listenerCalled.compareAndSet(false, true));
            failureConsumer.accept(e);
        }), latch);

        function.accept(listener);
        assertTrue("timed out after 5s", latch.await(5, TimeUnit.SECONDS));
    }
}
