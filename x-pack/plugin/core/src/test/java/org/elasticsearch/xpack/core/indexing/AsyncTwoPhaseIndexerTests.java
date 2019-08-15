/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.indexing;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;

public class AsyncTwoPhaseIndexerTests extends ESTestCase {

    AtomicBoolean isFinished = new AtomicBoolean(false);
    AtomicBoolean isStopped = new AtomicBoolean(false);

    @Before
    public void reset() {
        isFinished.set(false);
        isStopped.set(false);
    }

    private class MockIndexer extends AsyncTwoPhaseIndexer<Integer, MockJobStats> {

        private final CountDownLatch latch;
        // test the execution order
        private volatile int step;
        private final boolean stoppedBeforeFinished;

        protected MockIndexer(Executor executor, AtomicReference<IndexerState> initialState, Integer initialPosition,
                              CountDownLatch latch, boolean stoppedBeforeFinished) {
            super(executor, initialState, initialPosition, new MockJobStats());
            this.latch = latch;
            this.stoppedBeforeFinished = stoppedBeforeFinished;
        }

        @Override
        protected String getJobId() {
            return "mock";
        }

        @Override
        protected IterationResult<Integer> doProcess(SearchResponse searchResponse) {
            assertFalse("should not be called as stoppedBeforeFinished is false", stoppedBeforeFinished);
            assertThat(step, equalTo(3));
            ++step;
            return new IterationResult<>(Collections.emptyList(), 3, true);
        }

        private void awaitForLatch() {
            try {
                latch.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        protected SearchRequest buildSearchRequest() {
            assertThat(step, equalTo(1));
            ++step;
            return new SearchRequest();
        }

        @Override
        protected void onStart(long now, ActionListener<Boolean> listener) {
            assertThat(step, equalTo(0));
            ++step;
            listener.onResponse(true);
        }

        @Override
        protected void doNextSearch(SearchRequest request, ActionListener<SearchResponse> nextPhase) {
            assertThat(step, equalTo(2));
            ++step;
            final SearchResponseSections sections = new SearchResponseSections(
                new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0), null,
                null, false, null, null, 1);

            // block till latch has been counted down, simulating network latency
            awaitForLatch();
            nextPhase.onResponse(new SearchResponse(sections, null, 1, 1, 0, 0, ShardSearchFailure.EMPTY_ARRAY, null));
        }

        @Override
        protected void doNextBulk(BulkRequest request, ActionListener<BulkResponse> nextPhase) {
            fail("should not be called");
        }

        @Override
        protected void doSaveState(IndexerState state, Integer position, Runnable next) {
            int expectedStep = stoppedBeforeFinished ? 3 : 5;
            assertThat(step, equalTo(expectedStep));
            ++step;
            next.run();
        }

        @Override
        protected void onFailure(Exception exc) {
            fail(exc.getMessage());
        }

        @Override
        protected void onFinish(ActionListener<Void> listener) {
            assertThat(step, equalTo(4));
            ++step;
            listener.onResponse(null);
            assertTrue(isFinished.compareAndSet(false, true));
        }

        @Override
        protected void onStop() {
            assertTrue(isStopped.compareAndSet(false, true));
        }

        @Override
        protected void onAbort() {
        }

        public int getStep() {
            return step;
        }

    }

    private class MockIndexerFiveRuns extends AsyncTwoPhaseIndexer<Integer, MockJobStats> {

        // counters
        private volatile boolean started = false;
        private volatile int searchRequests = 0;
        private volatile int searchOps = 0;
        private volatile int processOps = 0;
        private volatile int bulkOps = 0;

        protected MockIndexerFiveRuns(Executor executor, AtomicReference<IndexerState> initialState, Integer initialPosition) {
            super(executor, initialState, initialPosition, new MockJobStats());
        }

        @Override
        protected String getJobId() {
            return "mock_5_runs";
        }

        @Override
        protected IterationResult<Integer> doProcess(SearchResponse searchResponse) {
            ++processOps;
            if (processOps == 5) {
                return new IterationResult<>(Collections.singletonList(new IndexRequest()), processOps, true);
            }
            else if (processOps % 2 == 0) {
                return new IterationResult<>(Collections.emptyList(), processOps, false);
            }

            return new IterationResult<>(Collections.singletonList(new IndexRequest()), processOps, false);
        }

        @Override
        protected SearchRequest buildSearchRequest() {
            ++searchRequests;
            return new SearchRequest();
        }

        @Override
        protected void onStart(long now, ActionListener<Boolean> listener) {
            started = true;
            listener.onResponse(true);
        }

        @Override
        protected void doNextSearch(SearchRequest request, ActionListener<SearchResponse> nextPhase) {
            ++searchOps;
            final SearchResponseSections sections = new SearchResponseSections(
                new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0), null,
                null, false, null, null, 1);

            nextPhase.onResponse(new SearchResponse(sections, null, 1, 1, 0, 0, ShardSearchFailure.EMPTY_ARRAY, null));
        }

        @Override
        protected void doNextBulk(BulkRequest request, ActionListener<BulkResponse> nextPhase) {
            ++bulkOps;
            nextPhase.onResponse(new BulkResponse(new BulkItemResponse[0], 100));
        }

        @Override
        protected void doSaveState(IndexerState state, Integer position, Runnable next) {
            next.run();
        }

        @Override
        protected void onFailure(Exception exc) {
            fail(exc.getMessage());
        }

        @Override
        protected void onFinish(ActionListener<Void> listener) {
            assertTrue(isFinished.compareAndSet(false, true));
            listener.onResponse(null);
        }

        @Override
        protected void onStop() {
            assertTrue(isStopped.compareAndSet(false, true));
        }

        @Override
        protected void onAbort() {
        }

        public void assertCounters() {
            assertTrue(started);
            assertEquals(5L, searchRequests);
            assertEquals(5L, searchOps);
            assertEquals(5L, processOps);
            assertEquals(2L, bulkOps);
        }

    }

    private class MockIndexerThrowsFirstSearch extends AsyncTwoPhaseIndexer<Integer, MockJobStats> {

        // test the execution order
        private int step;

        protected MockIndexerThrowsFirstSearch(Executor executor, AtomicReference<IndexerState> initialState, Integer initialPosition) {
            super(executor, initialState, initialPosition, new MockJobStats());
        }

        @Override
        protected String getJobId() {
            return "mock";
        }

        @Override
        protected IterationResult<Integer> doProcess(SearchResponse searchResponse) {
            fail("should not be called");
            return null;
        }

        @Override
        protected SearchRequest buildSearchRequest() {
            assertThat(step, equalTo(1));
            ++step;
            return new SearchRequest();
        }

        @Override
        protected void onStart(long now, ActionListener<Boolean> listener) {
            assertThat(step, equalTo(0));
            ++step;
            listener.onResponse(true);
        }

        @Override
        protected void doNextSearch(SearchRequest request, ActionListener<SearchResponse> nextPhase) {
            throw new RuntimeException("Failed to build search request");
        }

        @Override
        protected void doNextBulk(BulkRequest request, ActionListener<BulkResponse> nextPhase) {
            fail("should not be called");
        }

        @Override
        protected void doSaveState(IndexerState state, Integer position, Runnable next) {
            fail("should not be called");
        }

        @Override
        protected void onFailure(Exception exc) {
            assertThat(step, equalTo(2));
            ++step;
            assertTrue(isFinished.compareAndSet(false, true));
        }

        @Override
        protected void onFinish(ActionListener<Void> listener) {
            fail("should not be called");
        }

        @Override
        protected void onAbort() {
            fail("should not be called");
        }

        public int getStep() {
            return step;
        }
    }

    private static class MockJobStats extends IndexerJobStats {

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return null;
        }
    }

    public void testStateMachine() throws Exception {
        AtomicReference<IndexerState> state = new AtomicReference<>(IndexerState.STOPPED);
        final ExecutorService executor = Executors.newFixedThreadPool(1);
        try {
            CountDownLatch countDownLatch = new CountDownLatch(1);
            MockIndexer indexer = new MockIndexer(executor, state, 2, countDownLatch, false);
            indexer.start();
            assertThat(indexer.getState(), equalTo(IndexerState.STARTED));
            assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
            assertThat(indexer.getState(), equalTo(IndexerState.INDEXING));
            assertTrue(awaitBusy(() -> indexer.getPosition() == 2));
            countDownLatch.countDown();
            assertTrue(awaitBusy(() -> isFinished.get()));
            assertThat(indexer.getPosition(), equalTo(3));

            assertFalse(isStopped.get());
            assertThat(indexer.getStep(), equalTo(6));
            assertThat(indexer.getStats().getNumInvocations(), equalTo(1L));
            assertThat(indexer.getStats().getNumPages(), equalTo(1L));
            assertThat(indexer.getStats().getOutputDocuments(), equalTo(0L));
            assertTrue(indexer.abort());
        } finally {
            executor.shutdownNow();
        }
    }

    public void testStateMachineBrokenSearch() throws InterruptedException {
        AtomicReference<IndexerState> state = new AtomicReference<>(IndexerState.STOPPED);
        final ExecutorService executor = Executors.newFixedThreadPool(1);
        try {

            MockIndexerThrowsFirstSearch indexer = new MockIndexerThrowsFirstSearch(executor, state, 2);
            indexer.start();
            assertThat(indexer.getState(), equalTo(IndexerState.STARTED));
            assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
            assertTrue(awaitBusy(() -> isFinished.get(), 10000, TimeUnit.SECONDS));
            assertThat(indexer.getStep(), equalTo(3));

        } finally {
            executor.shutdownNow();
        }
    }

    public void testStop_WhileIndexing() throws InterruptedException {
        AtomicReference<IndexerState> state = new AtomicReference<>(IndexerState.STOPPED);
        final ExecutorService executor = Executors.newFixedThreadPool(1);
        try {
            CountDownLatch countDownLatch = new CountDownLatch(1);
            MockIndexer indexer = new MockIndexer(executor, state, 2, countDownLatch, true);
            indexer.start();
            assertThat(indexer.getState(), equalTo(IndexerState.STARTED));
            assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
            assertThat(indexer.getState(), equalTo(IndexerState.INDEXING));
            indexer.stop();
            countDownLatch.countDown();

            assertThat(indexer.getPosition(), equalTo(2));
            assertTrue(awaitBusy(() -> isStopped.get()));
            assertFalse(isFinished.get());
        } finally {
            executor.shutdownNow();
        }
    }

    public void testFiveRuns() throws InterruptedException {
        AtomicReference<IndexerState> state = new AtomicReference<>(IndexerState.STOPPED);
        final ExecutorService executor = Executors.newFixedThreadPool(1);
        try {
            MockIndexerFiveRuns indexer = new MockIndexerFiveRuns (executor, state, 2);
            indexer.start();
            assertThat(indexer.getState(), equalTo(IndexerState.STARTED));
            assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
            assertTrue(awaitBusy(() -> isFinished.get()));
            indexer.assertCounters();
        } finally {
            executor.shutdownNow();
        }
    }
}
