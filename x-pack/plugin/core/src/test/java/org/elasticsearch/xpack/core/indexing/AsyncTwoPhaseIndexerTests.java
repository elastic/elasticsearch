/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.indexing;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ESTestCase;

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

    private class MockIndexer extends AsyncTwoPhaseIndexer<Integer, MockJobStats> {

        private final CountDownLatch latch;
        // test the execution order
        private volatile int step;

        protected MockIndexer(Executor executor, AtomicReference<IndexerState> initialState, Integer initialPosition,
                              CountDownLatch latch) {
            super(executor, initialState, initialPosition, new MockJobStats());
            this.latch = latch;
        }

        @Override
        protected String getJobId() {
            return "mock";
        }

        @Override
        protected IterationResult<Integer> doProcess(SearchResponse searchResponse) {
            awaitForLatch();
            assertThat(step, equalTo(3));
            ++step;
            return new IterationResult<Integer>(Collections.emptyList(), 3, true);
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
        protected void onStart(long now, ActionListener<Void> listener) {
            assertThat(step, equalTo(0));
            ++step;
            listener.onResponse(null);
        }

        @Override
        protected void doNextSearch(SearchRequest request, ActionListener<SearchResponse> nextPhase) {
            assertThat(step, equalTo(2));
            ++step;
            final SearchResponseSections sections = new SearchResponseSections(
                new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0), null,
                null, false, null, null, 1);
            nextPhase.onResponse(new SearchResponse(sections, null, 1, 1, 0, 0, ShardSearchFailure.EMPTY_ARRAY, null));
        }

        @Override
        protected void doNextBulk(BulkRequest request, ActionListener<BulkResponse> nextPhase) {
            fail("should not be called");
        }

        @Override
        protected void doSaveState(IndexerState state, Integer position, Runnable next) {
            assertThat(step, equalTo(5));
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
            isFinished.set(true);
        }

        @Override
        protected void onAbort() {
        }

        public int getStep() {
            return step;
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
        protected void onStart(long now, ActionListener<Void> listener) {
            assertThat(step, equalTo(0));
            ++step;
            listener.onResponse(null);
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
            isFinished.set(true);
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
        isFinished.set(false);
        try {
            CountDownLatch countDownLatch = new CountDownLatch(1);
            MockIndexer indexer = new MockIndexer(executor, state, 2, countDownLatch);
            indexer.start();
            assertThat(indexer.getState(), equalTo(IndexerState.STARTED));
            assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
            assertThat(indexer.getState(), equalTo(IndexerState.INDEXING));
            countDownLatch.countDown();

            assertThat(indexer.getPosition(), equalTo(2));
            ESTestCase.awaitBusy(() -> isFinished.get());
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
        isFinished.set(false);
        try {

            MockIndexerThrowsFirstSearch indexer = new MockIndexerThrowsFirstSearch(executor, state, 2);
            indexer.start();
            assertThat(indexer.getState(), equalTo(IndexerState.STARTED));
            assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
            assertTrue(ESTestCase.awaitBusy(() -> isFinished.get(), 10000, TimeUnit.SECONDS));
            assertThat(indexer.getStep(), equalTo(3));

        } finally {
            executor.shutdownNow();
        }
    }
}
