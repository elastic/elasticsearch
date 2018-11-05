/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.indexing;

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

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;

public class AsyncTwoPhaseIndexerTests extends ESTestCase {

    AtomicBoolean isFinished = new AtomicBoolean(false);

    private class MockIndexer extends AsyncTwoPhaseIndexer<Integer, MockJobStats> {

        // test the execution order
        private int step;

        protected MockIndexer(Executor executor, AtomicReference<IndexerState> initialState, Integer initialPosition) {
            super(executor, initialState, initialPosition, new MockJobStats());
        }

        @Override
        protected String getJobId() {
            return "mock";
        }

        @Override
        protected IterationResult<Integer> doProcess(SearchResponse searchResponse) {
            assertThat(step, equalTo(3));
            ++step;
            return new IterationResult<Integer>(Collections.emptyList(), 3, true);
        }

        @Override
        protected SearchRequest buildSearchRequest() {
            assertThat(step, equalTo(1));
            ++step;
            return null;
        }

        @Override
        protected void onStartJob(long now) {
            assertThat(step, equalTo(0));
            ++step;
        }

        @Override
        protected void doNextSearch(SearchRequest request, ActionListener<SearchResponse> nextPhase) {
            assertThat(step, equalTo(2));
            ++step;
            final SearchResponseSections sections = new SearchResponseSections(new SearchHits(new SearchHit[0], 0, 0), null, null, false,
                    null, null, 1);

            nextPhase.onResponse(new SearchResponse(sections, null, 1, 1, 0, 0, ShardSearchFailure.EMPTY_ARRAY, null));
        }

        @Override
        protected void doNextBulk(BulkRequest request, ActionListener<BulkResponse> nextPhase) {
            fail("should not be called");
        }

        @Override
        protected void doSaveState(IndexerState state, Integer position, Runnable next) {
            assertThat(step, equalTo(4));
            ++step;
            next.run();
        }

        @Override
        protected void onFailure(Exception exc) {
            fail(exc.getMessage());
        }

        @Override
        protected void onFinish() {
            assertThat(step, equalTo(5));
            ++step;
            isFinished.set(true);
        }

        @Override
        protected void onAbort() {
        }

        public int getStep() {
            return step;
        }

    }

    private class MockIndexerThrowsOnSaveState extends AsyncTwoPhaseIndexer<Integer, MockJobStats> {

        // test the execution order
        private int step;
        private int failures;

        protected MockIndexerThrowsOnSaveState(Executor executor, AtomicReference<IndexerState> initialState, Integer initialPosition) {
            super(executor, initialState, initialPosition, new MockJobStats());
        }

        @Override
        protected String getJobId() {
            return "mock";
        }

        @Override
        protected IterationResult<Integer> doProcess(SearchResponse searchResponse) {
            if (step == 3) {
                ++step;
                return new IterationResult<Integer>(Collections.singletonList(new IndexRequest()), 3, false);
            } else if (step == 7) {
                ++step;
                return new IterationResult<Integer>(Collections.singletonList(new IndexRequest()), 3, true);
            }
            fail("number of steps mismatch: " + step + "expected 3 or 7");
            return null;
        }

        @Override
        protected SearchRequest buildSearchRequest() {
            if (step == 1 || step == 5) {
                ++step;
            } else {
                fail("number of steps mismatch: " + step + "expected 1 or 5");
            }
            return null;
        }

        @Override
        protected void onStartJob(long now) {
            assertThat(step, equalTo(0));
            ++step;
        }

        @Override
        protected void doNextSearch(SearchRequest request, ActionListener<SearchResponse> nextPhase) {
            if (step == 2 || step == 6) {
                ++step;
                final SearchResponseSections sections = new SearchResponseSections(new SearchHits(new SearchHit[0], 0, 0), null, null,
                        false, null, null, 1);

                nextPhase.onResponse(new SearchResponse(sections, null, 1, 1, 0, 0, ShardSearchFailure.EMPTY_ARRAY, null));
            } else {
                fail("number of steps mismatch: " + step + " expected 2 or 6");
            }
        }

        @Override
        protected void doNextBulk(BulkRequest request, ActionListener<BulkResponse> nextPhase) {
            assertThat(step, equalTo(4));
            ++step;
            nextPhase.onResponse(new BulkResponse(new BulkItemResponse[0], 0L));
        }

        @Override
        protected void doSaveState(IndexerState state, Integer position, Runnable next) {
            assertThat(step, equalTo(8));
            ++step;
            throw new RuntimeException("Failed to save state");
        }

        @Override
        protected void onFailure(Exception exc) {
            ++failures;
            if (getFailures() == 2) {
                isFinished.set(true);
            }
        }

        @Override
        protected void onFinish() {
            fail("should not be called");
        }

        @Override
        protected void onAbort() {
            fail("should not be called");
        }

        public int getStep() {
            return step;
        }

        public int getFailures() {
            return failures;
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
            return null;
        }

        @Override
        protected void onStartJob(long now) {
            assertThat(step, equalTo(0));
            ++step;
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
            assertThat(step, equalTo(2));
            ++step;
            next.run();
        }

        @Override
        protected void onFailure(Exception exc) {
            assertThat(step, equalTo(3));
            ++step;
            isFinished.set(true);
        }

        @Override
        protected void onFinish() {
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

    public void testStateMachine() throws InterruptedException {
        AtomicReference<IndexerState> state = new AtomicReference<>(IndexerState.STOPPED);
        final ExecutorService executor = Executors.newFixedThreadPool(1);
        isFinished.set(false);
        try {

            MockIndexer indexer = new MockIndexer(executor, state, 2);
            indexer.start();
            assertThat(indexer.getState(), equalTo(IndexerState.STARTED));
            assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
            assertThat(indexer.getState(), equalTo(IndexerState.INDEXING));
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

    public void testStateMachineBrokenSave() throws InterruptedException {
        AtomicReference<IndexerState> state = new AtomicReference<>(IndexerState.STOPPED);
        final ExecutorService executor = Executors.newFixedThreadPool(1);
        isFinished.set(false);
        try {

            MockIndexerThrowsOnSaveState indexer = new MockIndexerThrowsOnSaveState(executor, state, 2);
            indexer.start();
            assertThat(indexer.getState(), equalTo(IndexerState.STARTED));
            assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
            assertThat(indexer.getState(), equalTo(IndexerState.INDEXING));
            assertThat(indexer.getPosition(), equalTo(2));
            assertTrue(ESTestCase.awaitBusy(() -> isFinished.get()));
            assertThat(indexer.getStep(), equalTo(9));
            assertThat(indexer.getFailures(), equalTo(2));
            assertThat(indexer.getStats().getNumInvocations(), equalTo(1L));
            assertThat(indexer.getStats().getNumPages(), equalTo(2L));
            assertThat(indexer.getStats().getOutputDocuments(), equalTo(0L));

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
            assertTrue(ESTestCase.awaitBusy(() -> isFinished.get()));
            assertThat(indexer.getStep(), equalTo(4));

        } finally {
            executor.shutdownNow();
        }
    }
}
