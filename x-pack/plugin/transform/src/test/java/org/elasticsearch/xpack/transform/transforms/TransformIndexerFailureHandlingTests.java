/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.breaker.CircuitBreaker.Durability;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.profile.SearchProfileShardResults;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.common.notifications.Level;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.indexing.IterationResult;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;
import org.elasticsearch.xpack.core.transform.transforms.SettingsConfig;
import org.elasticsearch.xpack.core.transform.transforms.TimeRetentionPolicyConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpoint;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerPosition;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskState;
import org.elasticsearch.xpack.transform.Transform;
import org.elasticsearch.xpack.transform.TransformServices;
import org.elasticsearch.xpack.transform.checkpoint.CheckpointProvider;
import org.elasticsearch.xpack.transform.checkpoint.TransformCheckpointService;
import org.elasticsearch.xpack.transform.notifications.MockTransformAuditor;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.IndexBasedTransformConfigManager;
import org.elasticsearch.xpack.transform.persistence.SeqNoPrimaryTermAndIndex;
import org.junit.After;
import org.junit.Before;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.core.transform.transforms.DestConfigTests.randomDestConfig;
import static org.elasticsearch.xpack.core.transform.transforms.SourceConfigTests.randomSourceConfig;
import static org.elasticsearch.xpack.core.transform.transforms.pivot.PivotConfigTests.randomPivotConfig;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.matchesRegex;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.matches;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TransformIndexerFailureHandlingTests extends ESTestCase {

    private Client client;
    private ThreadPool threadPool;

    static class MockedTransformIndexer extends ClientTransformIndexer {

        private final Function<SearchRequest, SearchResponse> searchFunction;
        private final Function<BulkRequest, BulkResponse> bulkFunction;
        private final Function<DeleteByQueryRequest, BulkByScrollResponse> deleteByQueryFunction;

        private final Consumer<String> failureConsumer;

        // used for synchronizing with the test
        private CountDownLatch latch;

        MockedTransformIndexer(
            ThreadPool threadPool,
            String executorName,
            IndexBasedTransformConfigManager transformsConfigManager,
            CheckpointProvider checkpointProvider,
            TransformConfig transformConfig,
            TransformAuditor auditor,
            AtomicReference<IndexerState> initialState,
            TransformIndexerPosition initialPosition,
            TransformIndexerStats jobStats,
            TransformContext context,
            Function<SearchRequest, SearchResponse> searchFunction,
            Function<BulkRequest, BulkResponse> bulkFunction,
            Function<DeleteByQueryRequest, BulkByScrollResponse> deleteByQueryFunction,
            Consumer<String> failureConsumer
        ) {
            super(
                threadPool,
                new TransformServices(
                    transformsConfigManager,
                    mock(TransformCheckpointService.class),
                    auditor,
                    mock(SchedulerEngine.class)
                ),
                checkpointProvider,
                initialState,
                initialPosition,
                mock(Client.class),
                jobStats,
                transformConfig,
                /* TransformProgress */ null,
                TransformCheckpoint.EMPTY,
                TransformCheckpoint.EMPTY,
                new SeqNoPrimaryTermAndIndex(1, 1, "foo"),
                context,
                false
            );
            this.searchFunction = searchFunction;
            this.bulkFunction = bulkFunction;
            this.deleteByQueryFunction = deleteByQueryFunction;
            this.failureConsumer = failureConsumer;
        }

        public void initialize() {
            this.initializeFunction();
        }

        public CountDownLatch newLatch(int count) {
            return latch = new CountDownLatch(count);
        }

        @Override
        protected void createCheckpoint(ActionListener<TransformCheckpoint> listener) {
            final long timestamp = System.currentTimeMillis();
            listener.onResponse(new TransformCheckpoint(getJobId(), timestamp, 1, Collections.emptyMap(), timestamp));
        }

        @Override
        protected String getJobId() {
            return transformConfig.getId();
        }

        @Override
        protected void doNextSearch(long waitTimeInNanos, ActionListener<SearchResponse> nextPhase) {
            assert latch != null;
            try {
                latch.await();
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }

            try {
                SearchResponse response = searchFunction.apply(buildSearchRequest());
                nextPhase.onResponse(response);
            } catch (Exception e) {
                nextPhase.onFailure(e);
            }
        }

        @Override
        protected void doNextBulk(BulkRequest request, ActionListener<BulkResponse> nextPhase) {
            assert latch != null;
            try {
                latch.await();
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }

            try {
                BulkResponse response = bulkFunction.apply(request);
                super.handleBulkResponse(response, nextPhase);
            } catch (Exception e) {
                nextPhase.onFailure(e);
            }
        }

        @Override
        protected void doSaveState(IndexerState state, TransformIndexerPosition position, Runnable next) {
            assert state == IndexerState.STARTED || state == IndexerState.INDEXING || state == IndexerState.STOPPED;
            next.run();
        }

        @Override
        protected void onFailure(Exception exc) {
            try {
                super.onFailure(exc);
            } catch (Exception e) {
                final StringWriter sw = new StringWriter();
                final PrintWriter pw = new PrintWriter(sw, true);
                e.printStackTrace(pw);
                fail("Unexpected failure: " + e.getMessage() + " Trace: " + sw.getBuffer().toString());
            }
        }

        @Override
        protected void onAbort() {
            fail("onAbort should not be called");
        }

        @Override
        protected void failIndexer(String message) {
            if (failureConsumer != null) {
                failureConsumer.accept(message);
                super.failIndexer(message);
            } else {
                fail("failIndexer should not be called, received error: " + message);
            }
        }

        @Override
        void doGetInitialProgress(SearchRequest request, ActionListener<SearchResponse> responseListener) {
            responseListener.onResponse(
                new SearchResponse(
                    new InternalSearchResponse(
                        new SearchHits(new SearchHit[0], new TotalHits(0L, TotalHits.Relation.EQUAL_TO), 0.0f),
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
                )
            );
        }

        @Override
        protected void doDeleteByQuery(DeleteByQueryRequest deleteByQueryRequest, ActionListener<BulkByScrollResponse> responseListener) {
            try {
                BulkByScrollResponse response = deleteByQueryFunction.apply(deleteByQueryRequest);
                responseListener.onResponse(response);
            } catch (Exception e) {
                responseListener.onFailure(e);
            }
        }

        @Override
        protected void refreshDestinationIndex(ActionListener<RefreshResponse> responseListener) {
            responseListener.onResponse(new RefreshResponse(1, 1, 0, Collections.emptyList()));
        }

        @Override
        void doGetFieldMappings(ActionListener<Map<String, String>> fieldMappingsListener) {
            fieldMappingsListener.onResponse(Collections.emptyMap());
        }
    }

    @Before
    public void setUpMocks() {
        client = new NoOpClient(getTestName());
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void tearDownClient() {
        client.close();
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }

    public void testPageSizeAdapt() throws Exception {
        Integer pageSize = randomBoolean() ? null : randomIntBetween(500, 10_000);
        TransformConfig config = new TransformConfig(
            randomAlphaOfLength(10),
            randomSourceConfig(),
            randomDestConfig(),
            null,
            null,
            null,
            randomPivotConfig(),
            null,
            randomBoolean() ? null : randomAlphaOfLengthBetween(1, 1000),
            new SettingsConfig(pageSize, null, (Boolean) null),
            null,
            null,
            null
        );
        AtomicReference<IndexerState> state = new AtomicReference<>(IndexerState.STOPPED);
        final long initialPageSize = pageSize == null ? Transform.DEFAULT_INITIAL_MAX_PAGE_SEARCH_SIZE : pageSize;
        Function<SearchRequest, SearchResponse> searchFunction = searchRequest -> {
            throw new SearchPhaseExecutionException(
                "query",
                "Partial shards failure",
                new ShardSearchFailure[] {
                    new ShardSearchFailure(new CircuitBreakingException("to much memory", 110, 100, Durability.TRANSIENT)) }
            );
        };

        Function<BulkRequest, BulkResponse> bulkFunction = bulkRequest -> new BulkResponse(new BulkItemResponse[0], 100);

        TransformAuditor auditor = MockTransformAuditor.createMockAuditor();
        TransformContext context = new TransformContext(TransformTaskState.STARTED, "", 0, mock(TransformContext.Listener.class));

        MockedTransformIndexer indexer = createMockIndexer(
            config,
            state,
            searchFunction,
            bulkFunction,
            null,
            null,
            threadPool,
            ThreadPool.Names.GENERIC,
            auditor,
            context
        );
        final CountDownLatch latch = indexer.newLatch(1);
        indexer.start();
        assertThat(indexer.getState(), equalTo(IndexerState.STARTED));
        assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
        assertThat(indexer.getState(), equalTo(IndexerState.INDEXING));

        latch.countDown();
        assertBusy(() -> assertThat(indexer.getState(), equalTo(IndexerState.STARTED)), 10, TimeUnit.MINUTES);
        long pageSizeAfterFirstReduction = indexer.getPageSize();
        assertThat(initialPageSize, greaterThan(pageSizeAfterFirstReduction));
        assertThat(pageSizeAfterFirstReduction, greaterThan((long) TransformIndexer.MINIMUM_PAGE_SIZE));

        // run indexer a 2nd time
        final CountDownLatch secondRunLatch = indexer.newLatch(1);
        assertEquals(pageSizeAfterFirstReduction, indexer.getPageSize());
        assertThat(indexer.getState(), equalTo(IndexerState.STARTED));

        // when the indexer thread shuts down, it ignores the trigger, we might have to call it again
        assertBusy(() -> assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis())));
        assertThat(indexer.getState(), equalTo(IndexerState.INDEXING));

        secondRunLatch.countDown();
        assertBusy(() -> assertThat(indexer.getState(), equalTo(IndexerState.STARTED)));

        // assert that page size has been reduced again
        assertThat(pageSizeAfterFirstReduction, greaterThan((long) indexer.getPageSize()));
        assertThat(pageSizeAfterFirstReduction, greaterThan((long) TransformIndexer.MINIMUM_PAGE_SIZE));
    }

    public void testDoProcessAggNullCheck() {
        Integer pageSize = randomBoolean() ? null : randomIntBetween(500, 10_000);
        TransformConfig config = new TransformConfig(
            randomAlphaOfLength(10),
            randomSourceConfig(),
            randomDestConfig(),
            null,
            null,
            null,
            randomPivotConfig(),
            null,
            randomBoolean() ? null : randomAlphaOfLengthBetween(1, 1000),
            new SettingsConfig(pageSize, null, (Boolean) null),
            null,
            null,
            null
        );
        SearchResponse searchResponse = new SearchResponse(
            new InternalSearchResponse(
                new SearchHits(new SearchHit[0], new TotalHits(0L, TotalHits.Relation.EQUAL_TO), 0.0f),
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
        AtomicReference<IndexerState> state = new AtomicReference<>(IndexerState.STOPPED);
        Function<SearchRequest, SearchResponse> searchFunction = searchRequest -> searchResponse;
        Function<BulkRequest, BulkResponse> bulkFunction = bulkRequest -> new BulkResponse(new BulkItemResponse[0], 100);

        TransformAuditor auditor = mock(TransformAuditor.class);
        TransformContext context = new TransformContext(TransformTaskState.STARTED, "", 0, mock(TransformContext.Listener.class));

        MockedTransformIndexer indexer = createMockIndexer(
            config,
            state,
            searchFunction,
            bulkFunction,
            null,
            null,
            threadPool,
            ThreadPool.Names.GENERIC,
            auditor,
            context
        );

        IterationResult<TransformIndexerPosition> newPosition = indexer.doProcess(searchResponse);
        assertThat(newPosition.getToIndex().collect(Collectors.toList()), is(empty()));
        assertThat(newPosition.getPosition(), is(nullValue()));
        assertThat(newPosition.isDone(), is(true));
    }

    public void testScriptError() throws Exception {
        Integer pageSize = randomBoolean() ? null : randomIntBetween(500, 10_000);
        String transformId = randomAlphaOfLength(10);
        TransformConfig config = new TransformConfig(
            transformId,
            randomSourceConfig(),
            randomDestConfig(),
            null,
            null,
            null,
            randomPivotConfig(),
            null,
            randomBoolean() ? null : randomAlphaOfLengthBetween(1, 1000),
            new SettingsConfig(pageSize, null, (Boolean) null),
            null,
            null,
            null
        );
        AtomicReference<IndexerState> state = new AtomicReference<>(IndexerState.STOPPED);
        Function<SearchRequest, SearchResponse> searchFunction = searchRequest -> {
            throw new SearchPhaseExecutionException(
                "query",
                "Partial shards failure",
                new ShardSearchFailure[] {
                    new ShardSearchFailure(
                        new ScriptException(
                            "runtime error",
                            new ArithmeticException("/ by zero"),
                            singletonList("stack"),
                            "test",
                            "painless"
                        )
                    ) }

            );
        };

        Function<BulkRequest, BulkResponse> bulkFunction = bulkRequest -> new BulkResponse(new BulkItemResponse[0], 100);

        final AtomicBoolean failIndexerCalled = new AtomicBoolean(false);
        final AtomicReference<String> failureMessage = new AtomicReference<>();
        Consumer<String> failureConsumer = message -> {
            failIndexerCalled.compareAndSet(false, true);
            failureMessage.compareAndSet(null, message);
        };

        MockTransformAuditor auditor = MockTransformAuditor.createMockAuditor();
        TransformContext.Listener contextListener = mock(TransformContext.Listener.class);
        TransformContext context = new TransformContext(TransformTaskState.STARTED, "", 0, contextListener);

        MockedTransformIndexer indexer = createMockIndexer(
            config,
            state,
            searchFunction,
            bulkFunction,
            null,
            failureConsumer,
            threadPool,
            ThreadPool.Names.GENERIC,
            auditor,
            context
        );

        final CountDownLatch latch = indexer.newLatch(1);

        indexer.start();
        assertThat(indexer.getState(), equalTo(IndexerState.STARTED));
        assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
        assertThat(indexer.getState(), equalTo(IndexerState.INDEXING));

        latch.countDown();
        assertBusy(() -> assertThat(indexer.getState(), equalTo(IndexerState.STARTED)), 10, TimeUnit.SECONDS);
        assertTrue(failIndexerCalled.get());
        verify(contextListener, times(1)).fail(
            matches("Failed to execute script with error: \\[.*ArithmeticException: / by zero\\], stack trace: \\[stack\\]"),
            any()
        );

        assertThat(
            failureMessage.get(),
            matchesRegex("Failed to execute script with error: \\[.*ArithmeticException: / by zero\\], stack trace: \\[stack\\]")
        );
    }

    public void testRetentionPolicyDeleteByQueryThrowsIrrecoverable() throws Exception {
        String transformId = randomAlphaOfLength(10);
        TransformConfig config = new TransformConfig(
            transformId,
            randomSourceConfig(),
            randomDestConfig(),
            null,
            null,
            null,
            randomPivotConfig(),
            null,
            randomBoolean() ? null : randomAlphaOfLengthBetween(1, 1000),
            null,
            new TimeRetentionPolicyConfig(randomAlphaOfLength(10), TimeValue.timeValueSeconds(10)),
            null,
            null
        );

        final SearchResponse searchResponse = new SearchResponse(
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

        AtomicReference<IndexerState> state = new AtomicReference<>(IndexerState.STOPPED);
        Function<SearchRequest, SearchResponse> searchFunction = searchRequest -> searchResponse;

        Function<BulkRequest, BulkResponse> bulkFunction = bulkRequest -> new BulkResponse(new BulkItemResponse[0], 100);

        Function<DeleteByQueryRequest, BulkByScrollResponse> deleteByQueryFunction = deleteByQueryRequest -> {
            throw new SearchPhaseExecutionException(
                "query",
                "Partial shards failure",
                new ShardSearchFailure[] {
                    new ShardSearchFailure(
                        new ElasticsearchParseException("failed to parse date field", new IllegalArgumentException("illegal format"))
                    ) }
            );
        };

        final AtomicBoolean failIndexerCalled = new AtomicBoolean(false);
        final AtomicReference<String> failureMessage = new AtomicReference<>();
        Consumer<String> failureConsumer = message -> {
            failIndexerCalled.compareAndSet(false, true);
            failureMessage.compareAndSet(null, message);
        };

        MockTransformAuditor auditor = MockTransformAuditor.createMockAuditor();
        TransformContext.Listener contextListener = mock(TransformContext.Listener.class);
        TransformContext context = new TransformContext(TransformTaskState.STARTED, "", 0, contextListener);

        MockedTransformIndexer indexer = createMockIndexer(
            config,
            state,
            searchFunction,
            bulkFunction,
            deleteByQueryFunction,
            failureConsumer,
            threadPool,
            ThreadPool.Names.GENERIC,
            auditor,
            context
        );

        final CountDownLatch latch = indexer.newLatch(1);

        indexer.start();
        assertThat(indexer.getState(), equalTo(IndexerState.STARTED));
        assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
        assertThat(indexer.getState(), equalTo(IndexerState.INDEXING));

        latch.countDown();
        assertBusy(() -> assertThat(indexer.getState(), equalTo(IndexerState.STARTED)), 10, TimeUnit.SECONDS);
        assertTrue(failIndexerCalled.get());
        verify(contextListener, times(1)).fail(
            matches("task encountered irrecoverable failure: ElasticsearchParseException\\[failed to parse date field\\].*"),
            any()
        );

        assertThat(
            failureMessage.get(),
            matchesRegex(
                "task encountered irrecoverable failure: ElasticsearchParseException\\[failed to parse date field\\].*"
            )
        );
    }

    public void testRetentionPolicyDeleteByQueryThrowsTemporaryProblem() throws Exception {
        String transformId = randomAlphaOfLength(10);
        TransformConfig config = new TransformConfig(
            transformId,
            randomSourceConfig(),
            randomDestConfig(),
            null,
            null,
            null,
            randomPivotConfig(),
            null,
            randomBoolean() ? null : randomAlphaOfLengthBetween(1, 1000),
            null,
            new TimeRetentionPolicyConfig(randomAlphaOfLength(10), TimeValue.timeValueSeconds(10)),
            null,
            null
        );

        final SearchResponse searchResponse = new SearchResponse(
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

        AtomicReference<IndexerState> state = new AtomicReference<>(IndexerState.STOPPED);
        Function<SearchRequest, SearchResponse> searchFunction = searchRequest -> searchResponse;

        Function<BulkRequest, BulkResponse> bulkFunction = bulkRequest -> new BulkResponse(new BulkItemResponse[0], 100);

        Function<DeleteByQueryRequest, BulkByScrollResponse> deleteByQueryFunction = deleteByQueryRequest -> {
            throw new SearchPhaseExecutionException(
                "query",
                "Partial shards failure",
                new ShardSearchFailure[] { new ShardSearchFailure(new ElasticsearchTimeoutException("timed out during dbq")) }
            );
        };

        final AtomicBoolean failIndexerCalled = new AtomicBoolean(false);
        final AtomicReference<String> failureMessage = new AtomicReference<>();
        Consumer<String> failureConsumer = message -> {
            failIndexerCalled.compareAndSet(false, true);
            failureMessage.compareAndSet(null, message);
        };

        MockTransformAuditor auditor = MockTransformAuditor.createMockAuditor();
        auditor.addExpectation(
            new MockTransformAuditor.SeenAuditExpectation(
                "timed out during dbq",
                Level.WARNING,
                transformId,
                "Transform encountered an exception: org.elasticsearch.ElasticsearchTimeoutException: timed out during dbq;"
                    + " Will attempt again at next scheduled trigger."
            )
        );
        TransformContext.Listener contextListener = mock(TransformContext.Listener.class);
        TransformContext context = new TransformContext(TransformTaskState.STARTED, "", 0, contextListener);

        MockedTransformIndexer indexer = createMockIndexer(
            config,
            state,
            searchFunction,
            bulkFunction,
            deleteByQueryFunction,
            failureConsumer,
            threadPool,
            ThreadPool.Names.GENERIC,
            auditor,
            context
        );

        final CountDownLatch latch = indexer.newLatch(1);

        indexer.start();
        assertThat(indexer.getState(), equalTo(IndexerState.STARTED));
        assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
        assertThat(indexer.getState(), equalTo(IndexerState.INDEXING));

        latch.countDown();
        assertBusy(() -> assertThat(indexer.getState(), equalTo(IndexerState.STARTED)), 10, TimeUnit.SECONDS);
        assertFalse(failIndexerCalled.get());
        assertThat(indexer.getState(), equalTo(IndexerState.STARTED));
        auditor.assertAllExpectationsMatched();
        assertEquals(1, context.getFailureCount());
    }

    public void testFailureCounterIsResetOnSuccess() throws Exception {
        String transformId = randomAlphaOfLength(10);
        TransformConfig config = new TransformConfig(
            transformId,
            randomSourceConfig(),
            randomDestConfig(),
            null,
            null,
            null,
            randomPivotConfig(),
            null,
            randomBoolean() ? null : randomAlphaOfLengthBetween(1, 1000),
            null,
            null,
            null,
            null
        );

        final SearchResponse searchResponse = new SearchResponse(
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

        AtomicReference<IndexerState> state = new AtomicReference<>(IndexerState.STOPPED);
        Function<SearchRequest, SearchResponse> searchFunction = new Function<>() {
            final AtomicInteger calls = new AtomicInteger(0);

            @Override
            public SearchResponse apply(SearchRequest searchRequest) {
                int call = calls.getAndIncrement();
                if (call == 0) {
                    throw new SearchPhaseExecutionException(
                        "query",
                        "Partial shards failure",
                        new ShardSearchFailure[] { new ShardSearchFailure(new Exception()) }
                    );
                }
                return searchResponse;
            }
        };

        Function<BulkRequest, BulkResponse> bulkFunction = request -> new BulkResponse(new BulkItemResponse[0], 1);

        final AtomicBoolean failIndexerCalled = new AtomicBoolean(false);
        final AtomicReference<String> failureMessage = new AtomicReference<>();
        Consumer<String> failureConsumer = message -> {
            failIndexerCalled.compareAndSet(false, true);
            failureMessage.compareAndSet(null, message);
        };

        MockTransformAuditor auditor = MockTransformAuditor.createMockAuditor();
        TransformContext.Listener contextListener = mock(TransformContext.Listener.class);
        TransformContext context = new TransformContext(TransformTaskState.STARTED, "", 0, contextListener);

        MockedTransformIndexer indexer = createMockIndexer(
            config,
            state,
            searchFunction,
            bulkFunction,
            null,
            failureConsumer,
            threadPool,
            ThreadPool.Names.GENERIC,
            auditor,
            context
        );

        final CountDownLatch latch = indexer.newLatch(1);

        indexer.start();
        assertThat(indexer.getState(), equalTo(IndexerState.STARTED));
        assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
        assertThat(indexer.getState(), equalTo(IndexerState.INDEXING));

        latch.countDown();
        assertBusy(() -> assertThat(indexer.getState(), equalTo(IndexerState.STARTED)), 10, TimeUnit.SECONDS);
        assertFalse(failIndexerCalled.get());
        assertThat(indexer.getState(), equalTo(IndexerState.STARTED));
        assertEquals(1, context.getFailureCount());

        final CountDownLatch secondLatch = indexer.newLatch(1);

        indexer.start();
        assertThat(indexer.getState(), equalTo(IndexerState.STARTED));
        assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
        assertThat(indexer.getState(), equalTo(IndexerState.INDEXING));

        secondLatch.countDown();
        assertBusy(() -> assertThat(indexer.getState(), equalTo(IndexerState.STARTED)), 10, TimeUnit.SECONDS);
        assertFalse(failIndexerCalled.get());
        assertThat(indexer.getState(), equalTo(IndexerState.STARTED));
        auditor.assertAllExpectationsMatched();
        assertEquals(0, context.getFailureCount());
    }

    private MockedTransformIndexer createMockIndexer(
        TransformConfig config,
        AtomicReference<IndexerState> state,
        Function<SearchRequest, SearchResponse> searchFunction,
        Function<BulkRequest, BulkResponse> bulkFunction,
        Function<DeleteByQueryRequest, BulkByScrollResponse> deleteByQueryFunction,
        Consumer<String> failureConsumer,
        ThreadPool threadPool,
        String executorName,
        TransformAuditor auditor,
        TransformContext context
    ) {
        IndexBasedTransformConfigManager transformConfigManager = mock(IndexBasedTransformConfigManager.class);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<TransformConfig> listener = (ActionListener<TransformConfig>) invocationOnMock.getArguments()[1];
            listener.onResponse(config);
            return null;
        }).when(transformConfigManager).getTransformConfiguration(any(), any());
        MockedTransformIndexer indexer = new MockedTransformIndexer(
            threadPool,
            executorName,
            transformConfigManager,
            mock(CheckpointProvider.class),
            config,
            auditor,
            state,
            null,
            new TransformIndexerStats(),
            context,
            searchFunction,
            bulkFunction,
            deleteByQueryFunction,
            failureConsumer
        );

        indexer.initialize();
        return indexer;
    }

}
