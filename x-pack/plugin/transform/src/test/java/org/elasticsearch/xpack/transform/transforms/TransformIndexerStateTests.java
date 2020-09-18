/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.profile.SearchProfileShardResults;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.transform.transforms.TimeSyncConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpoint;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerPosition;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskState;
import org.elasticsearch.xpack.transform.checkpoint.CheckpointProvider;
import org.elasticsearch.xpack.transform.checkpoint.MockTimebasedCheckpointProvider;
import org.elasticsearch.xpack.transform.notifications.MockTransformAuditor;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.InMemoryTransformConfigManager;
import org.elasticsearch.xpack.transform.persistence.TransformConfigManager;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.core.transform.transforms.DestConfigTests.randomDestConfig;
import static org.elasticsearch.xpack.core.transform.transforms.SourceConfigTests.randomSourceConfig;
import static org.elasticsearch.xpack.core.transform.transforms.pivot.PivotConfigTests.randomPivotConfig;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.Mockito.mock;

public class TransformIndexerStateTests extends ESTestCase {

    private static final SearchResponse ONE_HIT_SEARCH_RESPONSE = new SearchResponse(
        new InternalSearchResponse(
            new SearchHits(new SearchHit[] { new SearchHit(1) }, new TotalHits(1L, TotalHits.Relation.EQUAL_TO), 1.0f),
            // Simulate completely null aggs
            null,
            new Suggest(Collections.emptyList()),
            new SearchProfileShardResults(Collections.emptyMap()),
            false,
            false,
            1
        ),
        "",
        1,
        1,
        0,
        0,
        ShardSearchFailure.EMPTY_ARRAY,
        SearchResponse.Clusters.EMPTY
    );

    private Client client;
    private ThreadPool threadPool;
    private TransformAuditor auditor;
    private TransformConfigManager transformConfigManager;

    class MockedTransformIndexer extends TransformIndexer {

        private int saveStateListenerCallCount = 0;
        // used for synchronizing with the test
        private CountDownLatch searchLatch;

        public MockedTransformIndexer(
            ThreadPool threadPool,
            String executorName,
            TransformConfigManager transformsConfigManager,
            CheckpointProvider checkpointProvider,
            TransformAuditor auditor,
            TransformConfig transformConfig,
            Map<String, String> fieldMappings,
            AtomicReference<IndexerState> initialState,
            TransformIndexerPosition initialPosition,
            TransformIndexerStats jobStats,
            TransformContext context
        ) {
            super(
                threadPool,
                executorName,
                transformsConfigManager,
                checkpointProvider,
                auditor,
                transformConfig,
                fieldMappings,
                initialState,
                initialPosition,
                jobStats,
                /* TransformProgress */ null,
                TransformCheckpoint.EMPTY,
                TransformCheckpoint.EMPTY,
                context
            );
        }

        public void initialize() {
            this.initializeFunction();
        }

        public CountDownLatch newSearchLatch(int count) {
            return searchLatch = new CountDownLatch(count);
        }

        @Override
        void doGetInitialProgress(SearchRequest request, ActionListener<SearchResponse> responseListener) {
            responseListener.onResponse(ONE_HIT_SEARCH_RESPONSE);
        }

        @Override
        protected void doNextSearch(long waitTimeInNanos, ActionListener<SearchResponse> nextPhase) {
            if (searchLatch != null) {
                try {
                    searchLatch.await();
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            }
            nextPhase.onResponse(ONE_HIT_SEARCH_RESPONSE);
        }

        @Override
        protected void doNextBulk(BulkRequest request, ActionListener<BulkResponse> nextPhase) {
            nextPhase.onResponse(new BulkResponse(new BulkItemResponse[0], 100));
        }

        @Override
        protected void doSaveState(IndexerState state, TransformIndexerPosition position, Runnable next) {
            Collection<ActionListener<Void>> saveStateListenersAtTheMomentOfCalling = saveStateListeners.getAndSet(null);
            try {
                if (saveStateListenersAtTheMomentOfCalling != null) {
                    saveStateListenerCallCount += saveStateListenersAtTheMomentOfCalling.size();
                    ActionListener.onResponse(saveStateListenersAtTheMomentOfCalling, null);
                }
            } catch (Exception onResponseException) {
                fail("failed to call save state listeners");
            } finally {
                next.run();
            }
        }

        public int getSaveStateListenerCallCount() {
            return saveStateListenerCallCount;
        }
    }

    @Before
    public void setUpMocks() {
        auditor = new MockTransformAuditor();
        transformConfigManager = new InMemoryTransformConfigManager();
        client = new NoOpClient(getTestName());
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void tearDownClient() {
        client.close();
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }

    public void testStopAtCheckpoint() throws Exception {
        TransformConfig config = new TransformConfig(
            randomAlphaOfLength(10),
            randomSourceConfig(),
            randomDestConfig(),
            null,
            new TimeSyncConfig("timestamp", TimeValue.timeValueSeconds(1)),
            null,
            randomPivotConfig(),
            randomBoolean() ? null : randomAlphaOfLengthBetween(1, 1000),
            null
        );

        for (IndexerState state : IndexerState.values()) {
            // skip indexing case, tested below
            if (IndexerState.INDEXING.equals(state)) {
                continue;
            }
            AtomicReference<IndexerState> stateRef = new AtomicReference<>(state);
            TransformContext context = new TransformContext(TransformTaskState.STARTED, "", 0, mock(TransformContext.Listener.class));
            final MockedTransformIndexer indexer = createMockIndexer(
                config,
                stateRef,
                null,
                threadPool,
                ThreadPool.Names.GENERIC,
                auditor,
                new TransformIndexerStats(),
                context
            );
            assertResponse(listener -> indexer.stopAtCheckpoint(true, listener));
            assertEquals(0, indexer.getSaveStateListenerCallCount());
        }

        // lets test a running indexer
        AtomicReference<IndexerState> state = new AtomicReference<>(IndexerState.STARTED);
        {
            TransformContext context = new TransformContext(TransformTaskState.STARTED, "", 0, mock(TransformContext.Listener.class));
            final MockedTransformIndexer indexer = createMockIndexer(
                config,
                state,
                null,
                threadPool,
                ThreadPool.Names.GENERIC,
                auditor,
                new TransformIndexerStats(),
                context
            );
            indexer.start();
            assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
            assertEquals(indexer.getState(), IndexerState.INDEXING);

            assertResponse(listener -> indexer.stopAtCheckpoint(true, listener));

            indexer.stop();
            assertBusy(() -> assertThat(indexer.getState(), equalTo(IndexerState.STOPPED)), 5, TimeUnit.SECONDS);

            // listener must have been called by the indexing thread
            assertEquals(1, indexer.getSaveStateListenerCallCount());

            // as the state is stopped it should go back to directly
            assertResponse(listener -> indexer.stopAtCheckpoint(true, listener));
            assertEquals(1, indexer.getSaveStateListenerCallCount());
        }

        // do another round
        {
            TransformContext context = new TransformContext(TransformTaskState.STARTED, "", 0, mock(TransformContext.Listener.class));
            final MockedTransformIndexer indexer = createMockIndexer(
                config,
                state,
                null,
                threadPool,
                ThreadPool.Names.GENERIC,
                auditor,
                new TransformIndexerStats(),
                context
            );

            indexer.start();
            assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
            assertEquals(indexer.getState(), IndexerState.INDEXING);

            // this time call it 3 times
            assertResponse(listener -> indexer.stopAtCheckpoint(true, listener));
            assertResponse(listener -> indexer.stopAtCheckpoint(true, listener));
            assertResponse(listener -> indexer.stopAtCheckpoint(true, listener));

            indexer.stop();
            assertBusy(() -> assertThat(indexer.getState(), equalTo(IndexerState.STOPPED)), 5, TimeUnit.SECONDS);

            // listener must have been called by the indexing thread between 1 and 3 times
            assertThat(indexer.getSaveStateListenerCallCount(), greaterThanOrEqualTo(1));
            assertThat(indexer.getSaveStateListenerCallCount(), lessThanOrEqualTo(3));
        }

        // 3rd round with some back and forth
        {
            TransformContext context = new TransformContext(TransformTaskState.STARTED, "", 0, mock(TransformContext.Listener.class));
            final MockedTransformIndexer indexer = createMockIndexer(
                config,
                state,
                null,
                threadPool,
                ThreadPool.Names.GENERIC,
                auditor,
                new TransformIndexerStats(),
                context
            );
            indexer.start();
            assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
            assertEquals(indexer.getState(), IndexerState.INDEXING);

            // slow down the indexer
            CountDownLatch searchLatch = indexer.newSearchLatch(1);

            // this time call 5 times and change stopAtCheckpoint every time
            List<CountDownLatch> responseLatches = new ArrayList<>();
            for (int i = 0; i < 5; ++i) {
                CountDownLatch latch = new CountDownLatch(1);
                boolean stopAtCheckpoint = i % 2 == 0;
                countResponse(listener -> indexer.stopAtCheckpoint(stopAtCheckpoint, listener), latch);
                responseLatches.add(latch);
            }

            // now let the indexer run again
            searchLatch.countDown();

            indexer.stop();
            assertBusy(() -> assertThat(indexer.getState(), equalTo(IndexerState.STOPPED)), 5, TimeUnit.SECONDS);

            // wait for all listeners
            for (CountDownLatch l : responseLatches) {
                assertTrue("timed out after 5s", l.await(5, TimeUnit.SECONDS));
            }

            // listener must have been called 5 times, because the value changed every time and we slowed down the indexer
            assertThat(indexer.getSaveStateListenerCallCount(), equalTo(5));
        }

        // 4th round: go wild
        {
            TransformContext context = new TransformContext(TransformTaskState.STARTED, "", 0, mock(TransformContext.Listener.class));
            final MockedTransformIndexer indexer = createMockIndexer(
                config,
                state,
                null,
                threadPool,
                ThreadPool.Names.GENERIC,
                auditor,
                new TransformIndexerStats(),
                context
            );
            indexer.start();
            assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
            assertEquals(indexer.getState(), IndexerState.INDEXING);

            // slow down the indexer
            CountDownLatch searchLatch = indexer.newSearchLatch(1);

            List<CountDownLatch> responseLatches = new ArrayList<>();
            for (int i = 0; i < 3; ++i) {
                CountDownLatch latch = new CountDownLatch(1);
                boolean stopAtCheckpoint = randomBoolean();
                countResponse(listener -> indexer.stopAtCheckpoint(stopAtCheckpoint, listener), latch);
                responseLatches.add(latch);
            }

            // now let the indexer run again
            searchLatch.countDown();

            // this time call it 3 times
            assertResponse(listener -> indexer.stopAtCheckpoint(randomBoolean(), listener));
            assertResponse(listener -> indexer.stopAtCheckpoint(randomBoolean(), listener));
            assertResponse(listener -> indexer.stopAtCheckpoint(randomBoolean(), listener));

            indexer.stop();
            assertBusy(() -> assertThat(indexer.getState(), equalTo(IndexerState.STOPPED)), 5, TimeUnit.SECONDS);

            // wait for all listeners
            for (CountDownLatch l : responseLatches) {
                assertTrue("timed out after 5s", l.await(5, TimeUnit.SECONDS));
            }

            // listener must have been called by the indexing thread between 1 and 6 times
            assertThat(indexer.getSaveStateListenerCallCount(), greaterThanOrEqualTo(1));
            assertThat(indexer.getSaveStateListenerCallCount(), lessThanOrEqualTo(6));
        }
    }

    private void assertResponse(Consumer<ActionListener<Void>> function) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        countResponse(function, latch);
        assertTrue("timed out after 5s", latch.await(5, TimeUnit.SECONDS));
    }

    private void countResponse(Consumer<ActionListener<Void>> function, CountDownLatch latch) throws InterruptedException {
        LatchedActionListener<Void> listener = new LatchedActionListener<>(
            ActionListener.wrap(
                r -> { assertEquals("listener called more than once", 1, latch.getCount()); },
                e -> { fail("got unexpected exception: " + e.getMessage()); }
            ),
            latch
        );

        function.accept(listener);
    }

    private MockedTransformIndexer createMockIndexer(
        TransformConfig config,
        AtomicReference<IndexerState> state,
        Consumer<String> failureConsumer,
        ThreadPool threadPool,
        String executorName,
        TransformAuditor auditor,
        TransformIndexerStats jobStats,
        TransformContext context
    ) {
        CheckpointProvider checkpointProvider = new MockTimebasedCheckpointProvider(config);
        transformConfigManager.putTransformConfiguration(config, ActionListener.wrap(r -> {}, e -> {}));

        MockedTransformIndexer indexer = new MockedTransformIndexer(
            threadPool,
            executorName,
            transformConfigManager,
            checkpointProvider,
            auditor,
            config,
            Collections.emptyMap(),
            state,
            null,
            jobStats,
            context
        );

        indexer.initialize();
        return indexer;
    }
}
