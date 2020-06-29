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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
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

        protected MockIndexer(ThreadPool threadPool, String executorName, AtomicReference<IndexerState> initialState,
                              Integer initialPosition, CountDownLatch latch, boolean stoppedBeforeFinished) {
            super(threadPool, executorName, initialState, initialPosition, new MockJobStats());
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
        protected SearchRequest buildSearchRequest(long waitTimeInNanos) {
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
            // for stop before finished we do not know if its stopped before are after the search
            if (stoppedBeforeFinished == false) {
                assertThat(step, equalTo(5));
            }
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

        private final long startTime;
        private final CountDownLatch latch;
        private volatile float maxDocsPerSecond;

        // counters
        private volatile boolean started = false;
        private volatile boolean waitingForLatch = false;
        private volatile int searchRequests = 0;
        private volatile int searchOps = 0;
        private volatile int processOps = 0;
        private volatile int bulkOps = 0;

        protected MockIndexerFiveRuns(ThreadPool threadPool, String executorName, AtomicReference<IndexerState> initialState,
                Integer initialPosition, float maxDocsPerSecond, CountDownLatch latch) {
            super(threadPool, executorName, initialState, initialPosition, new MockJobStats());
            startTime = System.nanoTime();
            this.latch = latch;
            this.maxDocsPerSecond = maxDocsPerSecond;
        }

        public void rethrottle(float maxDocsPerSecond) {
            this.maxDocsPerSecond = maxDocsPerSecond;
            rethrottle();
        }

        @Override
        protected String getJobId() {
            return "mock_5_runs";
        }

        @Override
        protected float getMaxDocsPerSecond() {
            return maxDocsPerSecond;
        }

        @Override
        protected IterationResult<Integer> doProcess(SearchResponse searchResponse) {
            // increment doc count for throttling
            getStats().incrementNumDocuments(1000);

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
        protected SearchRequest buildSearchRequest(long waitTimeInNanos) {
            ++searchRequests;
            return new SearchRequest();
        }

        @Override
        protected void onStart(long now, ActionListener<Boolean> listener) {
            started = true;
            listener.onResponse(true);
        }

        private void awaitForLatch() {
            if (latch == null) {
                return;
            }
            try {
                waitingForLatch = true;
                latch.await(10, TimeUnit.SECONDS);
                waitingForLatch = false;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        public boolean waitingForLatchCountDown() {
            return waitingForLatch;
        }

        @Override
        protected void doNextSearch(SearchRequest request, ActionListener<SearchResponse> nextPhase) {
            ++searchOps;
            final SearchResponseSections sections = new SearchResponseSections(
                new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0), null,
                null, false, null, null, 1);

            if (processOps == 3) {
                awaitForLatch();
            }

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

        @Override
        protected long getTimeNanos() {
            return startTime + searchOps * 50_000_000L;
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

        protected MockIndexerThrowsFirstSearch(ThreadPool threadPool, String executorName, AtomicReference<IndexerState> initialState,
                                               Integer initialPosition) {
            super(threadPool, executorName, initialState, initialPosition, new MockJobStats());
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
        protected SearchRequest buildSearchRequest(long waitTimeInNanos) {
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

    private class MockThreadPool extends TestThreadPool {

        private List<TimeValue> delays = new ArrayList<>();

        MockThreadPool(String name, ExecutorBuilder<?>... customBuilders) {
            super(name, Settings.EMPTY, customBuilders);
        }

        @Override
        public ScheduledCancellable schedule(Runnable command, TimeValue delay, String executor) {
            delays.add(delay);

            return super.schedule(command, TimeValue.ZERO, executor);
        }

        public void assertCountersAndDelay(Collection<TimeValue> expectedDelays) {
            assertThat(delays, equalTo(expectedDelays));
        }
    }

    public void testStateMachine() throws Exception {
        AtomicReference<IndexerState> state = new AtomicReference<>(IndexerState.STOPPED);
        final ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            CountDownLatch countDownLatch = new CountDownLatch(1);
            MockIndexer indexer = new MockIndexer(threadPool, ThreadPool.Names.GENERIC, state, 2, countDownLatch, false);
            indexer.start();
            assertThat(indexer.getState(), equalTo(IndexerState.STARTED));
            assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
            assertThat(indexer.getState(), equalTo(IndexerState.INDEXING));
            assertBusy(() -> assertThat(indexer.getPosition(), equalTo(2)));

            countDownLatch.countDown();
            assertBusy(() -> assertTrue(isFinished.get()));
            assertThat(indexer.getPosition(), equalTo(3));

            assertFalse(isStopped.get());
            assertThat(indexer.getStep(), equalTo(6));
            assertThat(indexer.getStats().getNumInvocations(), equalTo(1L));
            assertThat(indexer.getStats().getNumPages(), equalTo(1L));
            assertThat(indexer.getStats().getOutputDocuments(), equalTo(0L));
            assertTrue(indexer.abort());
        } finally {
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        }
    }

    public void testStateMachineBrokenSearch() throws Exception {
        AtomicReference<IndexerState> state = new AtomicReference<>(IndexerState.STOPPED);
        final ThreadPool threadPool = new TestThreadPool(getTestName());

        try {
            MockIndexerThrowsFirstSearch indexer = new MockIndexerThrowsFirstSearch(threadPool, ThreadPool.Names.GENERIC, state, 2);
            indexer.start();
            assertThat(indexer.getState(), equalTo(IndexerState.STARTED));
            assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
            assertBusy(() -> assertTrue(isFinished.get()), 10000, TimeUnit.SECONDS);
            assertThat(indexer.getStep(), equalTo(3));
        } finally {
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        }
    }

    public void testStop_WhileIndexing() throws Exception {
        AtomicReference<IndexerState> state = new AtomicReference<>(IndexerState.STOPPED);
        final ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            CountDownLatch countDownLatch = new CountDownLatch(1);
            MockIndexer indexer = new MockIndexer(threadPool, ThreadPool.Names.GENERIC, state, 2, countDownLatch, true);
            indexer.start();
            assertThat(indexer.getState(), equalTo(IndexerState.STARTED));
            assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
            assertThat(indexer.getState(), equalTo(IndexerState.INDEXING));
            indexer.stop();
            countDownLatch.countDown();

            assertThat(indexer.getPosition(), equalTo(2));
            assertBusy(() -> assertTrue(isStopped.get()));
            assertFalse(isFinished.get());
        } finally {
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        }
    }

    public void testFiveRuns() throws Exception {
        doTestFiveRuns(-1, Collections.emptyList());
    }

    public void testFiveRunsThrottled100() throws Exception {
        // expect throttling to kick in
        doTestFiveRuns(100, timeValueCollectionFromMilliseconds(9950L, 9950L, 9950L, 9950L));
    }

    public void testFiveRunsThrottled1000() throws Exception {
        // expect throttling to kick in
        doTestFiveRuns(1_000, timeValueCollectionFromMilliseconds(950L, 950L, 950L, 950L));
    }

    public void testFiveRunsThrottled18000() throws Exception {
        // expect throttling to not kick in due to min wait time
        doTestFiveRuns(18_000, Collections.emptyList());
    }

    public void testFiveRunsThrottled1000000() throws Exception {
        // docs per seconds is set high, so throttling does not kick in
        doTestFiveRuns(1_000_000, Collections.emptyList());
    }

    public void doTestFiveRuns(float docsPerSecond, Collection<TimeValue> expectedDelays) throws Exception {
        AtomicReference<IndexerState> state = new AtomicReference<>(IndexerState.STOPPED);
        final MockThreadPool threadPool = new MockThreadPool(getTestName());
        try {
            MockIndexerFiveRuns indexer = new MockIndexerFiveRuns (threadPool, ThreadPool.Names.GENERIC, state, 2, docsPerSecond,
                null);
            indexer.start();
            assertThat(indexer.getState(), equalTo(IndexerState.STARTED));
            assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
            assertBusy(() -> assertTrue(isFinished.get()));
            indexer.assertCounters();
            threadPool.assertCountersAndDelay(expectedDelays);
        } finally {
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        }
    }

    public void testFiveRunsRethrottle0_100() throws Exception {
        doTestFiveRunsRethrottle(-1, 100, timeValueCollectionFromMilliseconds(9950L));
    }

    public void testFiveRunsRethrottle100_0() throws Exception {
        doTestFiveRunsRethrottle(100, 0, timeValueCollectionFromMilliseconds(9950L, 9950L, 9950L));
    }

    public void testFiveRunsRethrottle100_1000() throws Exception {
        doTestFiveRunsRethrottle(100, 1000, timeValueCollectionFromMilliseconds(9950L, 9950L, 9950L, 950L));
    }

    public void testFiveRunsRethrottle1000_100() throws Exception {
        doTestFiveRunsRethrottle(1000, 100, timeValueCollectionFromMilliseconds(950L, 950L, 950L, 9950L));
    }

    public void doTestFiveRunsRethrottle(
        float docsPerSecond,
        float docsPerSecondRethrottle,
        Collection<TimeValue> expectedDelays
    ) throws Exception {
        AtomicReference<IndexerState> state = new AtomicReference<>(IndexerState.STOPPED);

        final MockThreadPool threadPool = new MockThreadPool(getTestName());
        try {
            CountDownLatch latch = new CountDownLatch(1);
            MockIndexerFiveRuns indexer = new MockIndexerFiveRuns (threadPool, ThreadPool.Names.GENERIC, state, 2, docsPerSecond,
                latch);
            indexer.start();
            assertThat(indexer.getState(), equalTo(IndexerState.STARTED));
            assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
            // wait until the indexer starts waiting on the latch
            assertBusy(() -> assertTrue(indexer.waitingForLatchCountDown()));
            // rethrottle
            indexer.rethrottle(docsPerSecondRethrottle);
            latch.countDown();
            // let it finish
            assertBusy(() -> assertTrue(isFinished.get()));
            indexer.assertCounters();
            threadPool.assertCountersAndDelay(expectedDelays);
        } finally {
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        }
    }

    public void testCalculateThrottlingDelay() {
        // negative docs per second, throttling turned off
        assertThat(AsyncTwoPhaseIndexer.calculateThrottlingDelay(-100, 100, 1_000, 1_000), equalTo(TimeValue.ZERO));

        // negative docs per second, throttling turned off
        assertThat(AsyncTwoPhaseIndexer.calculateThrottlingDelay(0, 100, 1_000, 1_000), equalTo(TimeValue.ZERO));

        // 100 docs/s with 100 docs -> 1s delay
        assertThat(AsyncTwoPhaseIndexer.calculateThrottlingDelay(100, 100, 1_000_000, 1_000_000), equalTo(TimeValue.timeValueSeconds(1)));

        // 100 docs/s with 100 docs, 200ms passed -> 800ms delay
        assertThat(
            AsyncTwoPhaseIndexer.calculateThrottlingDelay(100, 100, 1_000_000_000L, 1_200_000_000L),
            equalTo(TimeValue.timeValueMillis(800))
        );

        // 100 docs/s with 100 docs done, time passed -> no delay
        assertThat(AsyncTwoPhaseIndexer.calculateThrottlingDelay(100, 100, 1_000_000_000L, 5_000_000_000L), equalTo(TimeValue.ZERO));

        // 1_000_000 docs/s with 1 doc done, time passed -> no delay
        assertThat(AsyncTwoPhaseIndexer.calculateThrottlingDelay(1_000_000, 1, 1_000_000_000L, 1_000_000_000L), equalTo(TimeValue.ZERO));

        // max: 1 docs/s with 1_000_000 docs done, time passed -> no delay
        assertThat(
            AsyncTwoPhaseIndexer.calculateThrottlingDelay(1, 1_000_000, 1_000_000_000L, 1_000_000_000L),
            equalTo(TimeValue.timeValueHours(1))
        );

        // min: 100 docs/s with 100 docs, 995ms passed -> no delay, because minimum not reached
        assertThat(AsyncTwoPhaseIndexer.calculateThrottlingDelay(100, 100, 1_000_000_000L, 1_995_000_000L), equalTo(TimeValue.ZERO));

    }

    private static Collection<TimeValue> timeValueCollectionFromMilliseconds(Long... milliseconds) {
        List<TimeValue> timeValues = new ArrayList<>();
        for (Long m: milliseconds) {
            timeValues.add(TimeValue.timeValueMillis(m));
        }

        return timeValues;
    }
}
