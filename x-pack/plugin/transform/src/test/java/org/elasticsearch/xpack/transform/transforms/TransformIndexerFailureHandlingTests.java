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
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.breaker.CircuitBreaker.Durability;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.profile.SearchProfileResults;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.common.notifications.Level;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.indexing.IterationResult;
import org.elasticsearch.xpack.core.transform.transforms.SettingsConfig;
import org.elasticsearch.xpack.core.transform.transforms.TimeRetentionPolicyConfig;
import org.elasticsearch.xpack.core.transform.transforms.TimeSyncConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpoint;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerPosition;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformState;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskState;
import org.elasticsearch.xpack.transform.Transform;
import org.elasticsearch.xpack.transform.TransformServices;
import org.elasticsearch.xpack.transform.checkpoint.CheckpointProvider;
import org.elasticsearch.xpack.transform.checkpoint.TransformCheckpointService;
import org.elasticsearch.xpack.transform.notifications.MockTransformAuditor;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.IndexBasedTransformConfigManager;
import org.elasticsearch.xpack.transform.persistence.SeqNoPrimaryTermAndIndex;
import org.elasticsearch.xpack.transform.transforms.scheduling.TransformScheduler;
import org.junit.After;
import org.junit.Before;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Clock;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * Tests various indexer failure cases.
 * <p>
 * Legacy Warning: These tests have been written before {@link TransformFailureHandler} has been created,
 * potentially a lot of these tests can be rewritten. For new test cases use {@link TransformFailureHandlerTests}
 * if possible.
 */
public class TransformIndexerFailureHandlingTests extends ESTestCase {

    private Client client;
    private ThreadPool threadPool;

    static class MockedTransformIndexer extends ClientTransformIndexer {

        private final Function<SearchRequest, SearchResponse> searchFunction;
        private final Function<BulkRequest, BulkResponse> bulkFunction;
        private final Function<DeleteByQueryRequest, BulkByScrollResponse> deleteByQueryFunction;

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
            Function<DeleteByQueryRequest, BulkByScrollResponse> deleteByQueryFunction
        ) {
            super(
                threadPool,
                new TransformServices(
                    transformsConfigManager,
                    mock(TransformCheckpointService.class),
                    auditor,
                    new TransformScheduler(Clock.systemUTC(), threadPool, Settings.EMPTY)
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
                SearchResponse response = searchFunction.apply(buildSearchRequest().v2());
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
            super.doSaveState(state, position, next);
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
        void doGetInitialProgress(SearchRequest request, ActionListener<SearchResponse> responseListener) {
            responseListener.onResponse(
                new SearchResponse(
                    new InternalSearchResponse(
                        new SearchHits(new SearchHit[0], new TotalHits(0L, TotalHits.Relation.EQUAL_TO), 0.0f),
                        // Simulate completely null aggs
                        null,
                        new Suggest(Collections.emptyList()),
                        new SearchProfileResults(Collections.emptyMap()),
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

        @Override
        protected void persistState(TransformState state, ActionListener<Void> listener) {
            listener.onResponse(null);
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
            new SettingsConfig.Builder().setMaxPageSearchSize(pageSize).build(),
            null,
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
        long pageSizeAfterFirstReduction = context.getPageSize();
        assertThat(initialPageSize, greaterThan(pageSizeAfterFirstReduction));
        assertThat(pageSizeAfterFirstReduction, greaterThan((long) TransformIndexer.MINIMUM_PAGE_SIZE));

        // run indexer a 2nd time
        final CountDownLatch secondRunLatch = indexer.newLatch(1);
        assertEquals(pageSizeAfterFirstReduction, context.getPageSize());
        assertThat(indexer.getState(), equalTo(IndexerState.STARTED));

        // when the indexer thread shuts down, it ignores the trigger, we might have to call it again
        assertBusy(() -> assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis())));
        assertThat(indexer.getState(), equalTo(IndexerState.INDEXING));

        secondRunLatch.countDown();
        assertBusy(() -> assertThat(indexer.getState(), equalTo(IndexerState.STARTED)));

        // assert that page size has been reduced again
        assertThat(pageSizeAfterFirstReduction, greaterThan((long) context.getPageSize()));
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
            new SettingsConfig.Builder().setMaxPageSearchSize(pageSize).build(),
            null,
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
                new SearchProfileResults(Collections.emptyMap()),
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
            new SettingsConfig.Builder().setMaxPageSearchSize(pageSize).build(),
            null,
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

        MockTransformAuditor auditor = MockTransformAuditor.createMockAuditor();
        TransformContext.Listener contextListener = createContextListener(failIndexerCalled, failureMessage);
        TransformContext context = new TransformContext(TransformTaskState.STARTED, "", 0, contextListener);

        MockedTransformIndexer indexer = createMockIndexer(
            config,
            state,
            searchFunction,
            bulkFunction,
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
        assertBusy(() -> assertThat(indexer.getState(), equalTo(IndexerState.STARTED)), 10, TimeUnit.SECONDS);
        assertTrue(failIndexerCalled.get());
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
                new SearchProfileResults(Collections.emptyMap()),
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

        MockTransformAuditor auditor = MockTransformAuditor.createMockAuditor();
        TransformContext.Listener contextListener = createContextListener(failIndexerCalled, failureMessage);
        TransformContext context = new TransformContext(TransformTaskState.STARTED, "", 0, contextListener);

        MockedTransformIndexer indexer = createMockIndexer(
            config,
            state,
            searchFunction,
            bulkFunction,
            deleteByQueryFunction,
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
        assertThat(
            failureMessage.get(),
            matchesRegex(
                "task encountered irrecoverable failure: org.elasticsearch.ElasticsearchParseException: failed to parse date field;.*"
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
                new SearchProfileResults(Collections.emptyMap()),
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

        MockTransformAuditor auditor = MockTransformAuditor.createMockAuditor();
        auditor.addExpectation(
            new MockTransformAuditor.SeenAuditExpectation(
                "timed out during dbq",
                Level.WARNING,
                transformId,
                "Transform encountered an exception: [org.elasticsearch.ElasticsearchTimeoutException: timed out during dbq];"
                    + " Will automatically retry [1/10]"
            )
        );
        TransformContext.Listener contextListener = createContextListener(failIndexerCalled, failureMessage);
        TransformContext context = new TransformContext(TransformTaskState.STARTED, "", 0, contextListener);

        MockedTransformIndexer indexer = createMockIndexer(
            config,
            state,
            searchFunction,
            bulkFunction,
            deleteByQueryFunction,
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
            new TimeSyncConfig("time", TimeSyncConfig.DEFAULT_DELAY),
            null,
            randomPivotConfig(),
            null,
            randomBoolean() ? null : randomAlphaOfLengthBetween(1, 1000),
            null,
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
                new SearchProfileResults(Collections.emptyMap()),
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

        MockTransformAuditor auditor = MockTransformAuditor.createMockAuditor();
        TransformContext.Listener contextListener = createContextListener(failIndexerCalled, failureMessage);
        TransformContext context = new TransformContext(TransformTaskState.STARTED, "", 0, contextListener);

        MockedTransformIndexer indexer = createMockIndexer(
            config,
            state,
            searchFunction,
            bulkFunction,
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
        assertBusy(() -> assertThat(indexer.getState(), equalTo(IndexerState.STARTED)), 10, TimeUnit.SECONDS);
        assertFalse(failIndexerCalled.get());
        assertThat(indexer.getState(), equalTo(IndexerState.STARTED));
        assertEquals(1, context.getFailureCount());

        final CountDownLatch secondLatch = indexer.newLatch(1);

        indexer.start();
        assertThat(indexer.getState(), equalTo(IndexerState.STARTED));
        assertBusy(() -> assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis())));
        assertThat(indexer.getState(), equalTo(IndexerState.INDEXING));

        secondLatch.countDown();
        assertBusy(() -> assertThat(indexer.getState(), equalTo(IndexerState.STARTED)), 10, TimeUnit.SECONDS);
        assertFalse(failIndexerCalled.get());
        assertThat(indexer.getState(), equalTo(IndexerState.STARTED));
        auditor.assertAllExpectationsMatched();
        assertEquals(0, context.getFailureCount());
    }

    // tests throttling of audits on logs based on repeated exception types
    public void testHandleFailureAuditing() {
        String transformId = randomAlphaOfLength(10);
        TransformConfig config = new TransformConfig.Builder().setId(transformId)
            .setSource(randomSourceConfig())
            .setDest(randomDestConfig())
            .setSyncConfig(new TimeSyncConfig("time", TimeSyncConfig.DEFAULT_DELAY))
            .setPivotConfig(randomPivotConfig())
            .build();

        AtomicReference<IndexerState> state = new AtomicReference<>(IndexerState.STOPPED);
        Function<SearchRequest, SearchResponse> searchFunction = request -> mock(SearchResponse.class);
        Function<BulkRequest, BulkResponse> bulkFunction = request -> mock(BulkResponse.class);

        final AtomicBoolean failIndexerCalled = new AtomicBoolean(false);
        final AtomicReference<String> failureMessage = new AtomicReference<>();

        MockTransformAuditor auditor = MockTransformAuditor.createMockAuditor();
        TransformContext.Listener contextListener = createContextListener(failIndexerCalled, failureMessage);
        TransformContext context = new TransformContext(TransformTaskState.STARTED, "", 0, contextListener);

        auditor.addExpectation(
            new MockTransformAuditor.SeenAuditExpectation(
                "timeout_exception_1",
                Level.WARNING,
                transformId,
                "Transform encountered an exception: [*ElasticsearchTimeoutException: timeout_1]; Will automatically retry [1/"
                    + Transform.DEFAULT_FAILURE_RETRIES
                    + "]"
            )
        );

        auditor.addExpectation(
            new MockTransformAuditor.SeenAuditExpectation(
                "bulk_exception_1",
                Level.WARNING,
                transformId,
                "Transform encountered an exception: [*BulkIndexingException: bulk_exception_1*]; Will automatically retry [2/"
                    + Transform.DEFAULT_FAILURE_RETRIES
                    + "]"
            )
        );

        auditor.addExpectation(
            new MockTransformAuditor.SeenAuditExpectation(
                "timeout_exception_2",
                Level.WARNING,
                transformId,
                "Transform encountered an exception: [*ElasticsearchTimeoutException: timeout_2]; Will automatically retry [3/"
                    + Transform.DEFAULT_FAILURE_RETRIES
                    + "]"
            )
        );

        auditor.addExpectation(
            new MockTransformAuditor.SeenAuditExpectation(
                "bulk_exception_2",
                Level.WARNING,
                transformId,
                "Transform encountered an exception: [*BulkIndexingException: bulk_exception_2*]; Will automatically retry [6/"
                    + Transform.DEFAULT_FAILURE_RETRIES
                    + "]"
            )
        );

        MockedTransformIndexer indexer = createMockIndexer(
            config,
            state,
            searchFunction,
            bulkFunction,
            null,
            threadPool,
            ThreadPool.Names.GENERIC,
            auditor,
            context
        );

        indexer.handleFailure(
            new SearchPhaseExecutionException(
                "query",
                "Partial shards failure",
                new ShardSearchFailure[] { new ShardSearchFailure(new ElasticsearchTimeoutException("timeout_1")) }
            )
        );

        indexer.handleFailure(
            new SearchPhaseExecutionException(
                "query",
                "Partial shards failure",
                new ShardSearchFailure[] {
                    new ShardSearchFailure(
                        new BulkIndexingException("bulk_exception_1", new EsRejectedExecutionException("full queue"), false)
                    ) }
            )
        );

        indexer.handleFailure(
            new SearchPhaseExecutionException(
                "query",
                "Partial shards failure",
                new ShardSearchFailure[] { new ShardSearchFailure(new ElasticsearchTimeoutException("timeout_2")) }
            )
        );

        // not logged
        indexer.handleFailure(
            new SearchPhaseExecutionException(
                "query",
                "Partial shards failure",
                new ShardSearchFailure[] { new ShardSearchFailure(new ElasticsearchTimeoutException("timeout_2")) }
            )
        );

        // not logged
        indexer.handleFailure(
            new SearchPhaseExecutionException(
                "query",
                "Partial shards failure",
                new ShardSearchFailure[] { new ShardSearchFailure(new ElasticsearchTimeoutException("timeout_2")) }
            )
        );

        indexer.handleFailure(
            new SearchPhaseExecutionException(
                "query",
                "Partial shards failure",
                new ShardSearchFailure[] {
                    new ShardSearchFailure(
                        new BulkIndexingException("bulk_exception_2", new EsRejectedExecutionException("full queue"), false)
                    ) }
            )
        );

        auditor.assertAllExpectationsMatched();
    }

    public void testHandleFailure() {
        testHandleFailure(0, 5, 0, 0);
        testHandleFailure(5, 0, 5, 2);
        testHandleFailure(3, 5, 3, 2);
        testHandleFailure(5, 3, 5, 2);
        testHandleFailure(0, null, 0, 0);
        testHandleFailure(3, null, 3, 2);
        testHandleFailure(5, null, 5, 2);
        testHandleFailure(7, null, 7, 2);
        testHandleFailure(Transform.DEFAULT_FAILURE_RETRIES, null, Transform.DEFAULT_FAILURE_RETRIES, 2);
        testHandleFailure(null, 0, 0, 0);
        testHandleFailure(null, 3, 3, 2);
        testHandleFailure(null, 5, 5, 2);
        testHandleFailure(null, 7, 7, 2);
        testHandleFailure(null, Transform.DEFAULT_FAILURE_RETRIES, Transform.DEFAULT_FAILURE_RETRIES, 2);
        testHandleFailure(null, null, Transform.DEFAULT_FAILURE_RETRIES, 2);
    }

    private void testHandleFailure(
        Integer configNumFailureRetries,
        Integer contextNumFailureRetries,
        int expectedEffectiveNumFailureRetries,
        int expecedNumberOfRetryAudits
    ) {
        String transformId = randomAlphaOfLength(10);
        TransformConfig config = new TransformConfig.Builder().setId(transformId)
            .setSource(randomSourceConfig())
            .setDest(randomDestConfig())
            .setSyncConfig(new TimeSyncConfig("time", TimeSyncConfig.DEFAULT_DELAY))
            .setPivotConfig(randomPivotConfig())
            .setSettings(new SettingsConfig.Builder().setNumFailureRetries(configNumFailureRetries).build())
            .build();

        AtomicReference<IndexerState> state = new AtomicReference<>(IndexerState.STOPPED);
        Function<SearchRequest, SearchResponse> searchFunction = request -> mock(SearchResponse.class);
        Function<BulkRequest, BulkResponse> bulkFunction = request -> mock(BulkResponse.class);

        final AtomicBoolean failIndexerCalled = new AtomicBoolean(false);
        final AtomicReference<String> failureMessage = new AtomicReference<>();

        MockTransformAuditor auditor = MockTransformAuditor.createMockAuditor();
        TransformContext.Listener contextListener = createContextListener(failIndexerCalled, failureMessage);
        TransformContext context = new TransformContext(TransformTaskState.STARTED, "", 0, contextListener);
        if (contextNumFailureRetries != null) {
            context.setNumFailureRetries(contextNumFailureRetries);
        }

        int indexerRetries = configNumFailureRetries != null ? configNumFailureRetries
            : contextNumFailureRetries != null ? contextNumFailureRetries
            : Transform.DEFAULT_FAILURE_RETRIES;
        auditor.addExpectation(
            new MockTransformAuditor.SeenAuditExpectation(
                getTestName(),
                Level.WARNING,
                transformId,
                "Transform encountered an exception: [*]; Will automatically retry [*/" + indexerRetries + "]",
                expecedNumberOfRetryAudits
            )
        );

        MockedTransformIndexer indexer = createMockIndexer(
            config,
            state,
            searchFunction,
            bulkFunction,
            null,
            threadPool,
            ThreadPool.Names.GENERIC,
            auditor,
            context
        );

        for (int i = 0; i < expectedEffectiveNumFailureRetries; ++i) {
            indexer.handleFailure(new Exception("exception no. " + (i + 1)));
            assertFalse(failIndexerCalled.get());
            assertThat(failureMessage.get(), is(nullValue()));
            assertThat(context.getFailureCount(), is(equalTo(i + 1)));
        }
        indexer.handleFailure(new Exception("exception no. " + (expectedEffectiveNumFailureRetries + 1)));
        assertTrue(failIndexerCalled.get());
        assertThat(
            failureMessage.get(),
            is(
                equalTo(
                    "task encountered more than "
                        + expectedEffectiveNumFailureRetries
                        + " failures; latest failure: exception no. "
                        + (expectedEffectiveNumFailureRetries + 1)
                )
            )
        );
        assertThat(context.getFailureCount(), is(equalTo(expectedEffectiveNumFailureRetries + 1)));

        auditor.assertAllExpectationsMatched();
    }

    private MockedTransformIndexer createMockIndexer(
        TransformConfig config,
        AtomicReference<IndexerState> state,
        Function<SearchRequest, SearchResponse> searchFunction,
        Function<BulkRequest, BulkResponse> bulkFunction,
        Function<DeleteByQueryRequest, BulkByScrollResponse> deleteByQueryFunction,
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
            deleteByQueryFunction
        );

        indexer.initialize();
        return indexer;
    }

    private TransformContext.Listener createContextListener(
        final AtomicBoolean failIndexerCalled,
        final AtomicReference<String> failureMessage
    ) {
        return new TransformContext.Listener() {
            @Override
            public void shutdown() {}

            @Override
            public void failureCountChanged() {}

            @Override
            public void fail(String message, ActionListener<Void> listener) {
                assertTrue(failIndexerCalled.compareAndSet(false, true));
                assertTrue(failureMessage.compareAndSet(null, message));
            }
        };
    }
}
