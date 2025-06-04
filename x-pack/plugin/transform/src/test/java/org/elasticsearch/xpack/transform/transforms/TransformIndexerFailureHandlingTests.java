/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker.Durability;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.exception.ElasticsearchTimeoutException;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.test.ESTestCase;
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
import org.elasticsearch.xpack.transform.TransformExtension;
import org.elasticsearch.xpack.transform.TransformNode;
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
import java.util.stream.Stream;

import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.core.transform.transforms.DestConfigTests.randomDestConfig;
import static org.elasticsearch.xpack.core.transform.transforms.SourceConfigTests.randomSourceConfig;
import static org.elasticsearch.xpack.core.transform.transforms.pivot.PivotConfigTests.randomPivotConfig;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
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

    private ThreadPool threadPool;
    private static final Function<BulkRequest, BulkResponse> EMPTY_BULK_RESPONSE = bulkRequest -> new BulkResponse(
        new BulkItemResponse[0],
        100
    );

    static class MockedTransformIndexer extends ClientTransformIndexer {

        private final Function<SearchRequest, SearchResponse> searchFunction;
        private final Function<BulkRequest, BulkResponse> bulkFunction;
        private final Function<DeleteByQueryRequest, BulkByScrollResponse> deleteByQueryFunction;

        // used for synchronizing with the test
        private CountDownLatch latch;
        private int doProcessCount;

        MockedTransformIndexer(
            ThreadPool threadPool,
            ClusterService clusterService,
            IndexNameExpressionResolver indexNameExpressionResolver,
            TransformExtension transformExtension,
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
            int doProcessCount
        ) {
            super(
                threadPool,
                clusterService,
                indexNameExpressionResolver,
                transformExtension,
                new TransformServices(
                    transformsConfigManager,
                    mock(TransformCheckpointService.class),
                    auditor,
                    new TransformScheduler(Clock.systemUTC(), threadPool, Settings.EMPTY, TimeValue.ZERO),
                    mock(TransformNode.class)
                ),
                checkpointProvider,
                initialState,
                initialPosition,
                mock(ParentTaskAssigningClient.class),
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
            this.doProcessCount = doProcessCount;
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
        protected void doNextSearch(long waitTimeInNanos, ActionListener<SearchResponse> nextPhase) {
            assert latch != null;
            try {
                latch.await();
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }

            ActionListener.run(nextPhase, l -> ActionListener.respondAndRelease(l, searchFunction.apply(buildSearchRequest().v2())));
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
            ActionListener.respondAndRelease(responseListener, SearchResponseUtils.successfulResponse(SearchHits.EMPTY_WITH_TOTAL_HITS));
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
        protected void refreshDestinationIndex(ActionListener<Void> responseListener) {
            responseListener.onResponse(null);
        }

        @Override
        void doGetFieldMappings(ActionListener<Map<String, String>> fieldMappingsListener) {
            fieldMappingsListener.onResponse(Collections.emptyMap());
        }

        @Override
        protected void persistState(TransformState state, ActionListener<Void> listener) {
            listener.onResponse(null);
        }

        @Override
        protected IterationResult<TransformIndexerPosition> doProcess(SearchResponse searchResponse) {
            if (doProcessCount > 0) {
                doProcessCount -= 1;
                // pretend that we processed 10k documents for each call
                getStats().incrementNumDocuments(10_000);
                return new IterationResult<>(Stream.of(new IndexRequest()), new TransformIndexerPosition(null, null), false);
            }
            return super.doProcess(searchResponse);
        }
    }

    @Before
    public void setUpMocks() {
        threadPool = createThreadPool();
    }

    @After
    public void tearDownClient() {
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

        MockedTransformIndexer indexer = createMockIndexer(config, state, searchFunction, bulkFunction, null, threadPool, auditor, context);
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
        SearchResponse searchResponse = SearchResponseUtils.successfulResponse(SearchHits.EMPTY_WITH_TOTAL_HITS);
        try {
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
                auditor,
                context
            );

            IterationResult<TransformIndexerPosition> newPosition = indexer.doProcess(searchResponse);
            assertThat(newPosition.getToIndex().collect(Collectors.toList()), is(empty()));
            assertThat(newPosition.getPosition(), is(nullValue()));
            assertThat(newPosition.isDone(), is(true));
        } finally {
            searchResponse.decRef();
        }
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

        MockedTransformIndexer indexer = createMockIndexer(config, state, searchFunction, bulkFunction, null, threadPool, auditor, context);

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

        final SearchResponse searchResponse = SearchResponseUtils.successfulResponse(
            SearchHits.unpooled(new SearchHit[] { SearchHit.unpooled(1) }, new TotalHits(1L, TotalHits.Relation.EQUAL_TO), 1.0f)
        );
        try {
            AtomicReference<IndexerState> state = new AtomicReference<>(IndexerState.STOPPED);
            Function<SearchRequest, SearchResponse> searchFunction = searchRequest -> {
                searchResponse.mustIncRef();
                return searchResponse;
            };

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
        } finally {
            searchResponse.decRef();
        }
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

        final SearchResponse searchResponse = SearchResponseUtils.successfulResponse(
            SearchHits.unpooled(new SearchHit[] { SearchHit.unpooled(1) }, new TotalHits(1L, TotalHits.Relation.EQUAL_TO), 1.0f)
        );
        try {
            AtomicReference<IndexerState> state = new AtomicReference<>(IndexerState.STOPPED);
            Function<SearchRequest, SearchResponse> searchFunction = searchRequest -> {
                searchResponse.mustIncRef();
                return searchResponse;
            };

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
                    "Transform encountered an exception: [org.elasticsearch.exception.ElasticsearchTimeoutException: timed out during dbq];"
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
        } finally {
            searchResponse.decRef();
        }
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

        final SearchResponse searchResponse = SearchResponseUtils.successfulResponse(
            SearchHits.unpooled(new SearchHit[] { SearchHit.unpooled(1) }, new TotalHits(1L, TotalHits.Relation.EQUAL_TO), 1.0f)
        );
        try {
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
                    searchResponse.mustIncRef();
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
        } finally {
            searchResponse.decRef();
        }
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

        MockedTransformIndexer indexer = createMockIndexer(config, state, searchFunction, bulkFunction, null, threadPool, auditor, context);

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

    /**
     * Given no bulk upload errors
     * When we run the indexer
     * Then we should not fail or recreate the destination index
     */
    public void testHandleBulkResponseWithNoFailures() throws Exception {
        var indexer = runIndexer(createMockIndexer(returnHit(), EMPTY_BULK_RESPONSE));
        assertThat(indexer.getStats().getIndexFailures(), is(0L));
        assertFalse(indexer.context.shouldRecreateDestinationIndex());
        assertNull(indexer.context.getLastFailure());
    }

    private static TransformIndexer runIndexer(MockedTransformIndexer indexer) throws Exception {
        var latch = indexer.newLatch(1);
        indexer.start();
        assertThat(indexer.getState(), equalTo(IndexerState.STARTED));
        assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
        assertThat(indexer.getState(), equalTo(IndexerState.INDEXING));
        latch.countDown();
        assertBusy(() -> assertThat(indexer.getState(), equalTo(IndexerState.STARTED)), 10, TimeUnit.SECONDS);
        return indexer;
    }

    private MockedTransformIndexer createMockIndexer(
        Function<SearchRequest, SearchResponse> searchFunction,
        Function<BulkRequest, BulkResponse> bulkFunction
    ) {
        return createMockIndexer(searchFunction, bulkFunction, mock(TransformContext.Listener.class));
    }

    private static Function<SearchRequest, SearchResponse> returnHit() {
        return request -> SearchResponseUtils.successfulResponse(
            SearchHits.unpooled(new SearchHit[] { SearchHit.unpooled(1) }, new TotalHits(1L, TotalHits.Relation.EQUAL_TO), 1.0f)
        );
    }

    /**
     * Given an irrecoverable bulk upload error
     * When we run the indexer
     * Then we should fail without retries and not recreate the destination index
     */
    public void testHandleBulkResponseWithIrrecoverableFailures() throws Exception {
        var failCalled = new AtomicBoolean();
        var indexer = runIndexer(
            createMockIndexer(
                returnHit(),
                bulkResponseWithError(new ResourceNotFoundException("resource not found error")),
                createContextListener(failCalled, new AtomicReference<>())
            )
        );
        assertThat(indexer.getStats().getIndexFailures(), is(1L));
        assertFalse(indexer.context.shouldRecreateDestinationIndex());
        assertTrue(failCalled.get());
    }

    private MockedTransformIndexer createMockIndexer(
        Function<SearchRequest, SearchResponse> searchFunction,
        Function<BulkRequest, BulkResponse> bulkFunction,
        TransformContext.Listener listener
    ) {
        return createMockIndexer(
            new TransformConfig(
                randomAlphaOfLength(10),
                randomSourceConfig(),
                randomDestConfig(),
                null,
                null,
                null,
                randomPivotConfig(),
                null,
                randomBoolean() ? null : randomAlphaOfLengthBetween(1, 1000),
                new SettingsConfig.Builder().setMaxPageSearchSize(randomBoolean() ? null : randomIntBetween(500, 10_000)).build(),
                null,
                null,
                null,
                null
            ),
            new AtomicReference<>(IndexerState.STOPPED),
            searchFunction,
            bulkFunction,
            null,
            threadPool,
            mock(TransformAuditor.class),
            new TransformContext(TransformTaskState.STARTED, "", 0, listener),
            1
        );
    }

    private static Function<BulkRequest, BulkResponse> bulkResponseWithError(Exception e) {
        return bulkRequest -> new BulkResponse(
            new BulkItemResponse[] {
                BulkItemResponse.failure(1, DocWriteRequest.OpType.INDEX, new BulkItemResponse.Failure("the_index", "id", e)) },
            100
        );
    }

    /**
     * Given an IndexNotFound bulk upload error
     * When we run the indexer
     * Then we should fail with retries and recreate the destination index
     */
    public void testHandleBulkResponseWithIndexNotFound() throws Exception {
        var indexer = runIndexerWithBulkResponseError(new IndexNotFoundException("Some Error"));
        assertThat(indexer.getStats().getIndexFailures(), is(1L));
        assertTrue(indexer.context.shouldRecreateDestinationIndex());
        assertFalse(bulkIndexingException(indexer).isIrrecoverable());
    }

    private TransformIndexer runIndexerWithBulkResponseError(Exception e) throws Exception {
        return runIndexer(createMockIndexer(returnHit(), bulkResponseWithError(e)));
    }

    private static BulkIndexingException bulkIndexingException(TransformIndexer indexer) {
        var lastFailure = indexer.context.getLastFailure();
        assertNotNull(lastFailure);
        assertThat(lastFailure, instanceOf(BulkIndexingException.class));
        return (BulkIndexingException) lastFailure;
    }

    /**
     * Given a recoverable bulk upload error
     * When we run the indexer
     * Then we should fail with retries and not recreate the destination index
     */
    public void testHandleBulkResponseWithNoIrrecoverableFailures() throws Exception {
        var indexer = runIndexerWithBulkResponseError(new EsRejectedExecutionException("es rejected execution"));
        assertThat(indexer.getStats().getIndexFailures(), is(1L));
        assertFalse(indexer.context.shouldRecreateDestinationIndex());
        assertFalse(bulkIndexingException(indexer).isIrrecoverable());
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

        MockedTransformIndexer indexer = createMockIndexer(config, state, searchFunction, bulkFunction, null, threadPool, auditor, context);

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
        TransformAuditor auditor,
        TransformContext context
    ) {
        return createMockIndexer(config, state, searchFunction, bulkFunction, deleteByQueryFunction, threadPool, auditor, context, 0);
    }

    private MockedTransformIndexer createMockIndexer(
        TransformConfig config,
        AtomicReference<IndexerState> state,
        Function<SearchRequest, SearchResponse> searchFunction,
        Function<BulkRequest, BulkResponse> bulkFunction,
        Function<DeleteByQueryRequest, BulkByScrollResponse> deleteByQueryFunction,
        ThreadPool threadPool,
        TransformAuditor auditor,
        TransformContext context,
        int doProcessCount
    ) {
        IndexBasedTransformConfigManager transformConfigManager = mock(IndexBasedTransformConfigManager.class);
        doAnswer(invocationOnMock -> {
            ActionListener<TransformConfig> listener = invocationOnMock.getArgument(1);
            listener.onResponse(config);
            return null;
        }).when(transformConfigManager).getTransformConfiguration(any(), any());
        MockedTransformIndexer indexer = new MockedTransformIndexer(
            threadPool,
            mock(ClusterService.class),
            mock(IndexNameExpressionResolver.class),
            mock(TransformExtension.class),
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
            doProcessCount
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
            public void fail(Throwable exception, String message, ActionListener<Void> listener) {
                assertTrue(failIndexerCalled.compareAndSet(false, true));
                assertTrue(failureMessage.compareAndSet(null, message));
            }
        };
    }
}
