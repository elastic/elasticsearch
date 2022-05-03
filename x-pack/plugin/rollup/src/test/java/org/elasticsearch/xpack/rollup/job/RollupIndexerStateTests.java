/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.rollup.job;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.rollup.ConfigTestHelpers;
import org.elasticsearch.xpack.core.rollup.RollupField;
import org.elasticsearch.xpack.core.rollup.job.RollupIndexerJobStats;
import org.elasticsearch.xpack.core.rollup.job.RollupJob;
import org.elasticsearch.xpack.core.rollup.job.RollupJobConfig;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

public class RollupIndexerStateTests extends ESTestCase {
    private static class EmptyRollupIndexer extends RollupIndexer {
        EmptyRollupIndexer(
            ThreadPool threadPool,
            RollupJob job,
            AtomicReference<IndexerState> initialState,
            Map<String, Object> initialPosition,
            RollupIndexerJobStats stats
        ) {
            super(threadPool, job, initialState, initialPosition, stats);
        }

        EmptyRollupIndexer(
            ThreadPool threadPool,
            RollupJob job,
            AtomicReference<IndexerState> initialState,
            Map<String, Object> initialPosition
        ) {
            super(threadPool, job, initialState, initialPosition);
        }

        @Override
        protected void doNextSearch(long waitTimeInNanos, ActionListener<SearchResponse> nextPhase) {
            // TODO Should use InternalComposite constructor but it is package protected in core.
            Aggregations aggs = new Aggregations(Collections.singletonList(new CompositeAggregation() {
                @Override
                public List<? extends Bucket> getBuckets() {
                    return Collections.emptyList();
                }

                @Override
                public Map<String, Object> afterKey() {
                    return null;
                }

                @Override
                public String getName() {
                    return AGGREGATION_NAME;
                }

                @Override
                public String getType() {
                    return null;
                }

                @Override
                public Map<String, Object> getMetadata() {
                    return null;
                }

                @Override
                public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                    return null;
                }
            }));
            final SearchResponseSections sections = new SearchResponseSections(
                new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0),
                aggs,
                null,
                false,
                null,
                null,
                1
            );
            final SearchResponse response = new SearchResponse(sections, null, 1, 1, 0, 0, new ShardSearchFailure[0], null);
            nextPhase.onResponse(response);
        }

        @Override
        protected void doNextBulk(BulkRequest request, ActionListener<BulkResponse> nextPhase) {
            assert false : "doNextBulk should not be called";
        }

        @Override
        protected void doSaveState(IndexerState state, Map<String, Object> position, Runnable next) {
            assert state == IndexerState.STARTED || state == IndexerState.INDEXING || state == IndexerState.STOPPED;
            next.run();
        }

        @Override
        protected void onAbort() {
            assert false : "onAbort should not be called";
        }

        @Override
        protected void onFailure(Exception exc) {
            throw new AssertionError("failed with " + exc);
        }

        @Override
        protected void onFinish(ActionListener<Void> listener) {
            listener.onResponse(null);
        }
    }

    private static class DelayedEmptyRollupIndexer extends EmptyRollupIndexer {
        protected CountDownLatch latch;

        DelayedEmptyRollupIndexer(
            ThreadPool threadPool,
            RollupJob job,
            AtomicReference<IndexerState> initialState,
            Map<String, Object> initialPosition
        ) {
            super(threadPool, job, initialState, initialPosition);
        }

        DelayedEmptyRollupIndexer(
            ThreadPool threadPool,
            RollupJob job,
            AtomicReference<IndexerState> initialState,
            Map<String, Object> initialPosition,
            RollupIndexerJobStats stats
        ) {
            super(threadPool, job, initialState, initialPosition, stats);
        }

        private CountDownLatch newLatch() {
            return latch = new CountDownLatch(1);
        }

        @Override
        protected void doNextSearch(long waitTimeInNanos, ActionListener<SearchResponse> nextPhase) {
            assert latch != null;
            try {
                latch.await();
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
            super.doNextSearch(waitTimeInNanos, nextPhase);
        }
    }

    private static class NonEmptyRollupIndexer extends RollupIndexer {
        final Function<SearchRequest, SearchResponse> searchFunction;
        final Function<BulkRequest, BulkResponse> bulkFunction;
        final Consumer<Exception> failureConsumer;
        final BiConsumer<IndexerState, Map<String, Object>> saveStateCheck;
        private CountDownLatch latch;

        NonEmptyRollupIndexer(
            ThreadPool threadPool,
            RollupJob job,
            AtomicReference<IndexerState> initialState,
            Map<String, Object> initialPosition,
            Function<SearchRequest, SearchResponse> searchFunction,
            Function<BulkRequest, BulkResponse> bulkFunction,
            Consumer<Exception> failureConsumer
        ) {
            this(threadPool, job, initialState, initialPosition, searchFunction, bulkFunction, failureConsumer, (i, m) -> {});
        }

        NonEmptyRollupIndexer(
            ThreadPool threadPool,
            RollupJob job,
            AtomicReference<IndexerState> initialState,
            Map<String, Object> initialPosition,
            Function<SearchRequest, SearchResponse> searchFunction,
            Function<BulkRequest, BulkResponse> bulkFunction,
            Consumer<Exception> failureConsumer,
            BiConsumer<IndexerState, Map<String, Object>> saveStateCheck
        ) {
            super(threadPool, job, initialState, initialPosition);
            this.searchFunction = searchFunction;
            this.bulkFunction = bulkFunction;
            this.failureConsumer = failureConsumer;
            this.saveStateCheck = saveStateCheck;
        }

        private CountDownLatch newLatch(int count) {
            return latch = new CountDownLatch(count);
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
            nextPhase.onResponse(bulkFunction.apply(request));
        }

        @Override
        protected void doSaveState(IndexerState state, Map<String, Object> position, Runnable next) {
            assert state == IndexerState.STARTED || state == IndexerState.INDEXING || state == IndexerState.STOPPED;
            saveStateCheck.accept(state, position);
            next.run();
        }

        @Override
        protected void onAbort() {
            assert false : "onAbort should not be called";
        }

        @Override
        protected void onFailure(Exception exc) {
            failureConsumer.accept(exc);
        }

        @Override
        protected void onFinish(ActionListener<Void> listener) {
            listener.onResponse(null);
        }
    }

    public void testStarted() throws Exception {
        RollupJob job = new RollupJob(ConfigTestHelpers.randomRollupJobConfig(random()), Collections.emptyMap());
        AtomicReference<IndexerState> state = new AtomicReference<>(IndexerState.STOPPED);
        final ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            RollupIndexer indexer = new EmptyRollupIndexer(threadPool, job, state, null);
            indexer.start();
            assertThat(indexer.getState(), equalTo(IndexerState.STARTED));
            assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
            assertBusy(() -> assertThat(indexer.getState(), equalTo(IndexerState.STARTED)));
            assertThat(indexer.getStats().getNumInvocations(), equalTo(1L));
            assertThat(indexer.getStats().getNumPages(), equalTo(1L));
            assertThat(indexer.getStats().getIndexFailures(), equalTo(0L));
            assertThat(indexer.getStats().getSearchFailures(), equalTo(0L));
            assertThat(indexer.getStats().getSearchTotal(), equalTo(1L));
            assertThat(indexer.getStats().getIndexTotal(), equalTo(0L));
            assertTrue(indexer.abort());
        } finally {
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        }
    }

    public void testIndexing() throws Exception {
        RollupJob job = new RollupJob(ConfigTestHelpers.randomRollupJobConfig(random()), Collections.emptyMap());
        AtomicReference<IndexerState> state = new AtomicReference<>(IndexerState.STOPPED);
        final ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            AtomicBoolean isFinished = new AtomicBoolean(false);
            DelayedEmptyRollupIndexer indexer = new DelayedEmptyRollupIndexer(threadPool, job, state, null) {
                @Override
                protected void onFinish(ActionListener<Void> listener) {
                    super.onFinish(ActionListener.wrap(r -> { listener.onResponse(r); }, listener::onFailure));
                }

                @Override
                protected void doSaveState(IndexerState state, Map<String, Object> position, Runnable next) {
                    super.doSaveState(state, position, () -> {
                        if (state == IndexerState.STARTED) {
                            isFinished.set(true);
                        }
                        next.run();
                    });
                }
            };
            final CountDownLatch latch = indexer.newLatch();
            indexer.start();
            assertThat(indexer.getState(), equalTo(IndexerState.STARTED));
            assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
            assertThat(indexer.getState(), equalTo(IndexerState.INDEXING));
            latch.countDown();
            assertBusy(() -> assertTrue(isFinished.get()));
            assertThat(indexer.getState(), equalTo(IndexerState.STARTED));
            assertThat(indexer.getStats().getNumInvocations(), equalTo(1L));
            assertThat(indexer.getStats().getNumPages(), equalTo(1L));
            assertThat(indexer.getStats().getIndexFailures(), equalTo(0L));
            assertThat(indexer.getStats().getSearchFailures(), equalTo(0L));
            assertThat(indexer.getStats().getSearchTotal(), equalTo(1L));
            assertTrue(indexer.abort());
        } finally {
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        }
    }

    public void testStateChangeMidTrigger() {
        AtomicReference<IndexerState> state = new AtomicReference<>(IndexerState.STOPPED);

        RollupIndexerJobStats stats = new RollupIndexerJobStats();
        RollupIndexerJobStats spyStats = spy(stats);
        RollupJobConfig config = ConfigTestHelpers.randomRollupJobConfig(random());

        // We call stats before a final state check, so this allows us to flip the state
        // and make sure the appropriate error is thrown
        Answer<?> forwardAndChangeState = invocation -> {
            invocation.callRealMethod();
            state.set(IndexerState.STOPPED);
            return null;
        };

        doAnswer(forwardAndChangeState).when(spyStats).incrementNumInvocations(1L);
        RollupJob job = new RollupJob(config, Collections.emptyMap());

        final ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            AtomicBoolean isFinished = new AtomicBoolean(false);
            DelayedEmptyRollupIndexer indexer = new DelayedEmptyRollupIndexer(threadPool, job, state, null, spyStats) {
                @Override
                protected void onFinish(ActionListener<Void> listener) {
                    super.onFinish(ActionListener.wrap(r -> {
                        listener.onResponse(r);
                        isFinished.set(true);
                    }, listener::onFailure));
                }
            };
            final CountDownLatch latch = indexer.newLatch();
            indexer.start();
            assertThat(indexer.getState(), equalTo(IndexerState.STARTED));
            assertFalse(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
            assertThat(indexer.getState(), equalTo(IndexerState.STOPPED));
            latch.countDown();
            assertThat(indexer.getState(), equalTo(IndexerState.STOPPED));
            assertThat(indexer.getStats().getNumInvocations(), equalTo(1L));
            assertThat(indexer.getStats().getNumPages(), equalTo(0L));
            assertTrue(indexer.abort());
        } finally {
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        }
    }

    public void testAbortDuringSearch() throws Exception {
        final AtomicBoolean aborted = new AtomicBoolean(false);
        RollupJob job = new RollupJob(ConfigTestHelpers.randomRollupJobConfig(random()), Collections.emptyMap());
        AtomicReference<IndexerState> state = new AtomicReference<>(IndexerState.STOPPED);
        final ThreadPool threadPool = new TestThreadPool(getTestName());
        final CountDownLatch latch = new CountDownLatch(1);
        try {
            EmptyRollupIndexer indexer = new EmptyRollupIndexer(threadPool, job, state, null) {
                @Override
                protected void onFinish(ActionListener<Void> listener) {
                    fail("Should not have called onFinish");
                }

                @Override
                protected void doNextSearch(long waitTimeInNanos, ActionListener<SearchResponse> nextPhase) {
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        throw new IllegalStateException(e);
                    }
                    state.set(IndexerState.ABORTING);   // <-- Set to aborting right before we return the (empty) search response
                    super.doNextSearch(waitTimeInNanos, nextPhase);
                }

                @Override
                protected void onAbort() {
                    aborted.set(true);
                }
            };

            indexer.start();
            assertThat(indexer.getState(), equalTo(IndexerState.STARTED));
            assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
            assertThat(indexer.getState(), equalTo(IndexerState.INDEXING));
            latch.countDown();
            assertBusy(() -> assertTrue(aborted.get()));
            assertThat(indexer.getState(), equalTo(IndexerState.ABORTING));
            assertThat(indexer.getStats().getNumInvocations(), equalTo(1L));
            assertThat(indexer.getStats().getNumPages(), equalTo(0L));
            assertThat(indexer.getStats().getSearchFailures(), equalTo(0L));
        } finally {
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        }
    }

    public void testAbortAfterCompletion() throws Exception {
        final AtomicBoolean aborted = new AtomicBoolean(false);
        RollupJob job = new RollupJob(ConfigTestHelpers.randomRollupJobConfig(random()), Collections.emptyMap());
        AtomicReference<IndexerState> state = new AtomicReference<>(IndexerState.STOPPED);
        final ThreadPool threadPool = new TestThreadPool(getTestName());

        // Don't use the indexer's latch because we completely change doNextSearch()
        final CountDownLatch doNextSearchLatch = new CountDownLatch(1);

        try {
            DelayedEmptyRollupIndexer indexer = new DelayedEmptyRollupIndexer(threadPool, job, state, null) {
                @Override
                protected void onAbort() {
                    aborted.set(true);
                }

                @Override
                protected void doNextSearch(long waitTimeInNanos, ActionListener<SearchResponse> nextPhase) {
                    try {
                        doNextSearchLatch.await();
                    } catch (InterruptedException e) {
                        throw new IllegalStateException(e);
                    }
                    // TODO Should use InternalComposite constructor but it is package protected in core.
                    Aggregations aggs = new Aggregations(Collections.singletonList(new CompositeAggregation() {
                        @Override
                        public List<? extends Bucket> getBuckets() {
                            // Abort immediately before we are attempting to finish the job because the response
                            // was empty
                            state.set(IndexerState.ABORTING);
                            return Collections.emptyList();
                        }

                        @Override
                        public Map<String, Object> afterKey() {
                            return null;
                        }

                        @Override
                        public String getName() {
                            return AGGREGATION_NAME;
                        }

                        @Override
                        public String getType() {
                            return null;
                        }

                        @Override
                        public Map<String, Object> getMetadata() {
                            return null;
                        }

                        @Override
                        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                            return null;
                        }
                    }));
                    final SearchResponseSections sections = new SearchResponseSections(
                        new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0),
                        aggs,
                        null,
                        false,
                        null,
                        null,
                        1
                    );
                    final SearchResponse response = new SearchResponse(sections, null, 1, 1, 0, 0, ShardSearchFailure.EMPTY_ARRAY, null);
                    nextPhase.onResponse(response);
                }

                @Override
                protected void doSaveState(IndexerState state, Map<String, Object> position, Runnable next) {
                    assertTrue(state.equals(IndexerState.ABORTING));
                    next.run();
                }
            };

            indexer.start();
            assertThat(indexer.getState(), equalTo(IndexerState.STARTED));
            assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
            assertThat(indexer.getState(), equalTo(IndexerState.INDEXING));
            doNextSearchLatch.countDown();
            assertBusy(() -> assertTrue(aborted.get()));
            assertThat(indexer.getState(), equalTo(IndexerState.ABORTING));
            assertThat(indexer.getStats().getNumInvocations(), equalTo(1L));
            assertThat(indexer.getStats().getNumPages(), equalTo(1L));
        } finally {
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        }
    }

    public void testStopIndexing() throws Exception {
        RollupJob job = new RollupJob(ConfigTestHelpers.randomRollupJobConfig(random()), Collections.emptyMap());
        AtomicReference<IndexerState> state = new AtomicReference<>(IndexerState.STOPPED);
        final ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            DelayedEmptyRollupIndexer indexer = new DelayedEmptyRollupIndexer(threadPool, job, state, null);
            final CountDownLatch latch = indexer.newLatch();
            assertFalse(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
            assertThat(indexer.getState(), equalTo(IndexerState.STOPPED));
            assertThat(indexer.stop(), equalTo(IndexerState.STOPPED));
            assertThat(indexer.start(), equalTo(IndexerState.STARTED));
            assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
            assertThat(indexer.getState(), equalTo(IndexerState.INDEXING));
            assertThat(indexer.stop(), equalTo(IndexerState.STOPPING));
            latch.countDown();
            assertBusy(() -> assertThat(indexer.getState(), equalTo(IndexerState.STOPPED)));
            assertTrue(indexer.abort());
        } finally {
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        }
    }

    public void testAbortIndexing() throws Exception {
        RollupJob job = new RollupJob(ConfigTestHelpers.randomRollupJobConfig(random()), Collections.emptyMap());
        AtomicReference<IndexerState> state = new AtomicReference<>(IndexerState.STOPPED);
        final ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            final AtomicBoolean isAborted = new AtomicBoolean(false);
            DelayedEmptyRollupIndexer indexer = new DelayedEmptyRollupIndexer(threadPool, job, state, null) {
                @Override
                protected void onAbort() {
                    isAborted.set(true);
                }
            };
            final CountDownLatch latch = indexer.newLatch();
            indexer.start();
            assertThat(indexer.getState(), equalTo(IndexerState.STARTED));
            assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
            assertThat(indexer.getState(), equalTo(IndexerState.INDEXING));
            assertFalse(indexer.abort());
            assertThat(indexer.getState(), equalTo(IndexerState.ABORTING));
            latch.countDown();
            assertBusy(() -> assertTrue(isAborted.get()));
            assertFalse(indexer.abort());
        } finally {
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        }
    }

    public void testAbortStarted() {
        RollupJob job = new RollupJob(ConfigTestHelpers.randomRollupJobConfig(random()), Collections.emptyMap());
        AtomicReference<IndexerState> state = new AtomicReference<>(IndexerState.STOPPED);
        final ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            final AtomicBoolean isAborted = new AtomicBoolean(false);
            DelayedEmptyRollupIndexer indexer = new DelayedEmptyRollupIndexer(threadPool, job, state, null) {
                @Override
                protected void onAbort() {
                    isAborted.set(true);
                }
            };
            indexer.newLatch();
            indexer.start();
            assertThat(indexer.getState(), equalTo(IndexerState.STARTED));
            assertTrue(indexer.abort());
            assertThat(indexer.getState(), equalTo(IndexerState.ABORTING));
            assertFalse(isAborted.get());
            assertThat(indexer.getStats().getNumInvocations(), equalTo(0L));
            assertThat(indexer.getStats().getNumPages(), equalTo(0L));
            assertFalse(indexer.abort());
        } finally {
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        }
    }

    public void testMultipleJobTriggering() throws Exception {
        RollupJob job = new RollupJob(ConfigTestHelpers.randomRollupJobConfig(random()), Collections.emptyMap());
        AtomicReference<IndexerState> state = new AtomicReference<>(IndexerState.STOPPED);
        final ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            final AtomicBoolean isAborted = new AtomicBoolean(false);
            DelayedEmptyRollupIndexer indexer = new DelayedEmptyRollupIndexer(threadPool, job, state, null) {
                @Override
                protected void onAbort() {
                    isAborted.set(true);
                }
            };
            indexer.start();
            for (int i = 0; i < 5; i++) {
                final CountDownLatch latch = indexer.newLatch();
                assertThat(indexer.getState(), equalTo(IndexerState.STARTED));
                assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
                assertThat(indexer.getState(), equalTo(IndexerState.INDEXING));
                assertFalse(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
                assertThat(indexer.getState(), equalTo(IndexerState.INDEXING));
                latch.countDown();
                assertBusy(() -> assertThat(indexer.getState(), equalTo(IndexerState.STARTED)));
                assertThat(indexer.getStats().getNumInvocations(), equalTo((long) i + 1));
                assertThat(indexer.getStats().getNumPages(), equalTo((long) i + 1));
            }
            final CountDownLatch latch = indexer.newLatch();
            assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
            assertThat(indexer.stop(), equalTo(IndexerState.STOPPING));
            assertThat(indexer.getState(), equalTo(IndexerState.STOPPING));
            latch.countDown();
            assertBusy(() -> assertThat(indexer.getState(), equalTo(IndexerState.STOPPED)));
            assertTrue(indexer.abort());
        } finally {
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        }
    }

    // Tests how we handle unknown keys that come back from composite agg, e.g. if we add support for new types but don't
    // deal with it everyhwere
    public void testUnknownKey() throws Exception {
        AtomicBoolean isFinished = new AtomicBoolean(false);
        RollupJob job = new RollupJob(ConfigTestHelpers.randomRollupJobConfig(random()), Collections.emptyMap());
        AtomicReference<IndexerState> state = new AtomicReference<>(IndexerState.STOPPED);
        Function<SearchRequest, SearchResponse> searchFunction = searchRequest -> {
            Aggregations aggs = new Aggregations(Collections.singletonList(new CompositeAggregation() {
                @Override
                public List<? extends Bucket> getBuckets() {
                    Bucket b = new Bucket() {
                        @Override
                        public Map<String, Object> getKey() {
                            return Collections.singletonMap("foo", "bar");
                        }

                        @Override
                        public String getKeyAsString() {
                            return null;
                        }

                        @Override
                        public long getDocCount() {
                            return 1;
                        }

                        @Override
                        public Aggregations getAggregations() {
                            return InternalAggregations.EMPTY;
                        }

                        @Override
                        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                            return null;
                        }
                    };

                    return Collections.singletonList(b);
                }

                @Override
                public Map<String, Object> afterKey() {
                    return null;
                }

                @Override
                public String getName() {
                    return RollupField.NAME;
                }

                @Override
                public String getType() {
                    return null;
                }

                @Override
                public Map<String, Object> getMetadata() {
                    return null;
                }

                @Override
                public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                    return null;
                }
            }));
            final SearchResponseSections sections = new SearchResponseSections(
                new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0),
                aggs,
                null,
                false,
                null,
                null,
                1
            );
            return new SearchResponse(sections, null, 1, 1, 0, 0, ShardSearchFailure.EMPTY_ARRAY, null);
        };

        Function<BulkRequest, BulkResponse> bulkFunction = bulkRequest -> new BulkResponse(new BulkItemResponse[0], 100);

        Consumer<Exception> failureConsumer = e -> { assertThat(e.getMessage(), equalTo("Could not identify key in agg [foo]")); };
        BiConsumer<IndexerState, Map<String, Object>> stateCheck = (i, p) -> {
            if (i == IndexerState.STARTED) {
                isFinished.set(true);
            }
        };

        final ThreadPool threadPool = new TestThreadPool(getTestName());
        try {

            NonEmptyRollupIndexer indexer = new NonEmptyRollupIndexer(
                threadPool,
                job,
                state,
                null,
                searchFunction,
                bulkFunction,
                failureConsumer,
                stateCheck
            );
            final CountDownLatch latch = indexer.newLatch(1);
            indexer.start();
            assertThat(indexer.getState(), equalTo(IndexerState.STARTED));
            assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
            assertThat(indexer.getState(), equalTo(IndexerState.INDEXING));
            latch.countDown();
            assertBusy(() -> assertTrue(isFinished.get()));

            // Despite failure in bulk, we should move back to STARTED and wait to try again on next trigger
            assertThat(indexer.getState(), equalTo(IndexerState.STARTED));
            assertThat(indexer.getStats().getNumInvocations(), equalTo(1L));
            assertThat(indexer.getStats().getNumPages(), equalTo(1L));

            // There should be one recorded failure
            assertThat(indexer.getStats().getSearchFailures(), equalTo(1L));

            // Note: no docs were indexed
            assertThat(indexer.getStats().getOutputDocuments(), equalTo(0L));
            assertTrue(indexer.abort());
        } finally {
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        }
    }

    // Tests to make sure that errors in search do not interfere with shutdown procedure
    public void testFailureWhileStopping() throws Exception {
        AtomicBoolean isFinished = new AtomicBoolean(false);

        RollupJob job = new RollupJob(ConfigTestHelpers.randomRollupJobConfig(random()), Collections.emptyMap());
        AtomicReference<IndexerState> state = new AtomicReference<>(IndexerState.STOPPED);
        Function<SearchRequest, SearchResponse> searchFunction = searchRequest -> {
            Aggregations aggs = new Aggregations(Collections.singletonList(new CompositeAggregation() {
                @Override
                public List<? extends Bucket> getBuckets() {
                    Bucket b = new Bucket() {
                        @Override
                        public Map<String, Object> getKey() {
                            state.set(IndexerState.STOPPING); // <- Force a stop so we can see how error + non-INDEXING state is handled
                            return Collections.singletonMap("foo", "bar");  // This will throw an exception
                        }

                        @Override
                        public String getKeyAsString() {
                            return null;
                        }

                        @Override
                        public long getDocCount() {
                            return 1;
                        }

                        @Override
                        public Aggregations getAggregations() {
                            return InternalAggregations.EMPTY;
                        }

                        @Override
                        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                            return null;
                        }
                    };

                    return Collections.singletonList(b);
                }

                @Override
                public Map<String, Object> afterKey() {
                    return null;
                }

                @Override
                public String getName() {
                    return RollupField.NAME;
                }

                @Override
                public String getType() {
                    return null;
                }

                @Override
                public Map<String, Object> getMetadata() {
                    return null;
                }

                @Override
                public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                    return null;
                }
            }));
            final SearchResponseSections sections = new SearchResponseSections(
                new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0),
                aggs,
                null,
                false,
                null,
                null,
                1
            );
            return new SearchResponse(sections, null, 1, 1, 0, 0, ShardSearchFailure.EMPTY_ARRAY, null);
        };

        Function<BulkRequest, BulkResponse> bulkFunction = bulkRequest -> new BulkResponse(new BulkItemResponse[0], 100);

        Consumer<Exception> failureConsumer = e -> { assertThat(e.getMessage(), equalTo("Could not identify key in agg [foo]")); };

        BiConsumer<IndexerState, Map<String, Object>> doSaveStateCheck = (indexerState, position) -> { isFinished.set(true); };

        final ThreadPool threadPool = new TestThreadPool(getTestName());
        try {

            NonEmptyRollupIndexer indexer = new NonEmptyRollupIndexer(
                threadPool,
                job,
                state,
                null,
                searchFunction,
                bulkFunction,
                failureConsumer,
                doSaveStateCheck
            );
            final CountDownLatch latch = indexer.newLatch(1);
            indexer.start();
            assertThat(indexer.getState(), equalTo(IndexerState.STARTED));
            assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
            assertThat(indexer.getState(), equalTo(IndexerState.INDEXING));
            latch.countDown();
            assertBusy(() -> assertTrue(isFinished.get()));
            // Despite failure in processing keys, we should continue moving to STOPPED
            assertThat(indexer.getState(), equalTo(IndexerState.STOPPED));
            assertThat(indexer.getStats().getNumInvocations(), equalTo(1L));
            assertThat(indexer.getStats().getNumPages(), equalTo(1L));

            // There should be one recorded failure
            assertThat(indexer.getStats().getSearchFailures(), equalTo(1L));

            // Note: no docs were indexed
            assertThat(indexer.getStats().getOutputDocuments(), equalTo(0L));
            assertTrue(indexer.abort());
        } finally {
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        }
    }

    public void testSearchShardFailure() throws Exception {
        AtomicBoolean isFinished = new AtomicBoolean(false);
        RollupJob job = new RollupJob(ConfigTestHelpers.randomRollupJobConfig(random()), Collections.emptyMap());
        AtomicReference<IndexerState> state = new AtomicReference<>(IndexerState.STOPPED);
        Function<SearchRequest, SearchResponse> searchFunction = searchRequest -> {
            throw new SearchPhaseExecutionException(
                "query",
                "Partial shards failure",
                new ShardSearchFailure[] { new ShardSearchFailure(new RuntimeException("failed")) }
            );
        };

        Function<BulkRequest, BulkResponse> bulkFunction = bulkRequest -> new BulkResponse(new BulkItemResponse[0], 100);

        Consumer<Exception> failureConsumer = e -> { assertThat(e.getMessage(), startsWith("Partial shards failure")); };
        BiConsumer<IndexerState, Map<String, Object>> stateCheck = (i, p) -> {
            if (i == IndexerState.STARTED) {
                isFinished.set(true);
            }
        };

        final ThreadPool threadPool = new TestThreadPool(getTestName());
        try {

            NonEmptyRollupIndexer indexer = new NonEmptyRollupIndexer(
                threadPool,
                job,
                state,
                null,
                searchFunction,
                bulkFunction,
                failureConsumer,
                stateCheck
            );
            final CountDownLatch latch = indexer.newLatch(1);
            indexer.start();
            assertThat(indexer.getState(), equalTo(IndexerState.STARTED));
            assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
            assertThat(indexer.getState(), equalTo(IndexerState.INDEXING));
            latch.countDown();
            assertBusy(() -> assertTrue(isFinished.get()));

            // Despite failure in bulk, we should move back to STARTED and wait to try again on next trigger
            assertThat(indexer.getState(), equalTo(IndexerState.STARTED));
            assertThat(indexer.getStats().getNumInvocations(), equalTo(1L));

            // There should be one recorded failure
            assertThat(indexer.getStats().getSearchFailures(), equalTo(1L));

            // Note: no pages processed, no docs were indexed
            assertThat(indexer.getStats().getNumPages(), equalTo(0L));
            assertThat(indexer.getStats().getOutputDocuments(), equalTo(0L));
            assertTrue(indexer.abort());
        } finally {
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        }
    }

    public void testBulkFailure() throws Exception {
        AtomicBoolean isFinished = new AtomicBoolean(false);
        RollupJob job = new RollupJob(ConfigTestHelpers.randomRollupJobConfig(random()), Collections.emptyMap());
        AtomicReference<IndexerState> state = new AtomicReference<>(IndexerState.STOPPED);
        Function<SearchRequest, SearchResponse> searchFunction = searchRequest -> {
            Aggregations aggs = new Aggregations(Collections.singletonList(new CompositeAggregation() {
                @Override
                public List<? extends Bucket> getBuckets() {
                    Bucket b = new Bucket() {
                        @Override
                        public Map<String, Object> getKey() {
                            return Collections.singletonMap("foo.terms", "bar");
                        }

                        @Override
                        public String getKeyAsString() {
                            return null;
                        }

                        @Override
                        public long getDocCount() {
                            return 1;
                        }

                        @Override
                        public Aggregations getAggregations() {
                            return InternalAggregations.EMPTY;
                        }

                        @Override
                        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                            return null;
                        }
                    };

                    return Collections.singletonList(b);
                }

                @Override
                public Map<String, Object> afterKey() {
                    return null;
                }

                @Override
                public String getName() {
                    return RollupField.NAME;
                }

                @Override
                public String getType() {
                    return null;
                }

                @Override
                public Map<String, Object> getMetadata() {
                    return null;
                }

                @Override
                public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                    return null;
                }
            }));
            final SearchResponseSections sections = new SearchResponseSections(
                new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0),
                aggs,
                null,
                false,
                null,
                null,
                1
            );
            return new SearchResponse(sections, null, 1, 1, 0, 0, ShardSearchFailure.EMPTY_ARRAY, null);
        };

        Function<BulkRequest, BulkResponse> bulkFunction = bulkRequest -> {
            fail("Should not have reached bulk function");
            return null;
        };

        Consumer<Exception> failureConsumer = e -> { assertThat(e.getMessage(), equalTo("failed")); };
        BiConsumer<IndexerState, Map<String, Object>> stateCheck = (i, p) -> {
            if (i == IndexerState.STARTED) {
                isFinished.set(true);
            }
        };

        final ThreadPool threadPool = new TestThreadPool(getTestName());
        try {

            NonEmptyRollupIndexer indexer = new NonEmptyRollupIndexer(
                threadPool,
                job,
                state,
                null,
                searchFunction,
                bulkFunction,
                failureConsumer,
                stateCheck
            ) {
                @Override
                protected void doNextBulk(BulkRequest request, ActionListener<BulkResponse> nextPhase) {
                    nextPhase.onFailure(new RuntimeException("failed"));
                }
            };
            final CountDownLatch latch = indexer.newLatch(1);
            indexer.start();
            assertThat(indexer.getState(), equalTo(IndexerState.STARTED));
            assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
            assertThat(indexer.getState(), equalTo(IndexerState.INDEXING));
            latch.countDown();
            assertBusy(() -> assertTrue(isFinished.get()));

            // Despite failure in bulk, we should move back to STARTED and wait to try again on next trigger
            assertThat(indexer.getState(), equalTo(IndexerState.STARTED));
            assertThat(indexer.getStats().getNumInvocations(), equalTo(1L));
            assertThat(indexer.getStats().getNumPages(), equalTo(1L));

            // There should be one recorded failure
            assertThat(indexer.getStats().getIndexFailures(), equalTo(1L));

            // Note: no docs were indexed
            assertThat(indexer.getStats().getOutputDocuments(), equalTo(0L));
            assertTrue(indexer.abort());
        } finally {
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        }
    }
}
