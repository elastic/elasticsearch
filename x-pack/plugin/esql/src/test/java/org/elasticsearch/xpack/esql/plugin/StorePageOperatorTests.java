/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.test.AnyOperatorTestCase;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.hamcrest.Matcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class StorePageOperatorTests extends AnyOperatorTestCase {

    private static TestThreadPool threadPool;

    @BeforeClass
    public static void createThreadPool() {
        threadPool = new TestThreadPool("StorePageOperatorTests");
    }

    @AfterClass
    public static void shutdownThreadPool() {
        terminate(threadPool);
        threadPool = null;
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return new StorePageOperator.StorePageOperatorFactory(createTestProvider(3), p -> p);
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("StorePageOperator[pageSize=3]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return expectedDescriptionOfSimple();
    }

    @Override
    protected void assertStatus(Map<String, Object> map, List<Page> input, List<Page> output) {
        assertThat(map, nullValue());
    }

    public void testSinglePageFitsInPageSize() {
        var recorder = new StorageRecorder();
        var provider = createTestProvider(10, recorder);
        try (var op = new StorePageOperator(provider, p -> p)) {
            op.addInput(buildPage(0, 5));
            assertFalse(op.isFinished());
            op.finish();
            drainBlocked(op);
            assertTrue(op.isFinished());

            assertThat(provider.totalPages(), equalTo(1));
            assertThat(provider.totalRows(), equalTo(5));
            assertThat(provider.firstPage(), notNullValue());
            assertThat(recorder.storedPages.size(), equalTo(1));
            assertThat(recorder.metadataCreated, equalTo(true));
            assertThat(recorder.metadataFinalized, equalTo(true));

            int firstPageRows = provider.firstPage().stream().mapToInt(Page::getPositionCount).sum();
            assertThat(firstPageRows, equalTo(5));

            releaseFirstPage(provider);
        }
    }

    public void testMultiplePagesExactFit() {
        var recorder = new StorageRecorder();
        var provider = createTestProvider(3, recorder);
        try (var op = new StorePageOperator(provider, p -> p)) {
            op.addInput(buildPage(0, 3));
            op.addInput(buildPage(3, 3));
            op.addInput(buildPage(6, 3));
            op.finish();
            drainBlocked(op);

            assertThat(provider.totalPages(), equalTo(3));
            assertThat(provider.totalRows(), equalTo(9));
            assertThat(recorder.storedPages.size(), equalTo(3));
            assertThat(recorder.metadataFinalized, equalTo(true));

            assertStoredPageRows(recorder, 0, 3);
            assertStoredPageRows(recorder, 1, 3);
            assertStoredPageRows(recorder, 2, 3);

            releaseFirstPage(provider);
        }
    }

    public void testMultiplePagesWithRemainder() {
        var recorder = new StorageRecorder();
        var provider = createTestProvider(3, recorder);
        try (var op = new StorePageOperator(provider, p -> p)) {
            op.addInput(buildPage(0, 5));
            op.addInput(buildPage(5, 3));
            op.finish();
            drainBlocked(op);

            assertThat(provider.totalPages(), equalTo(3));
            assertThat(provider.totalRows(), equalTo(8));
            assertThat(recorder.storedPages.size(), equalTo(3));

            assertStoredPageRows(recorder, 0, 3);
            assertStoredPageRows(recorder, 1, 3);
            assertStoredPageRows(recorder, 2, 2);

            releaseFirstPage(provider);
        }
    }

    public void testLargeInputSplitIntoManyPages() {
        int pageSize = 4;
        int totalRows = 25;
        var recorder = new StorageRecorder();
        var provider = createTestProvider(pageSize, recorder);
        try (var op = new StorePageOperator(provider, p -> p)) {
            op.addInput(buildPage(0, totalRows));
            op.finish();
            drainBlocked(op);

            int expectedPages = (totalRows + pageSize - 1) / pageSize;
            assertThat(provider.totalPages(), equalTo(expectedPages));
            assertThat(provider.totalRows(), equalTo(totalRows));
            assertThat(recorder.storedPages.size(), equalTo(expectedPages));

            for (int i = 0; i < expectedPages - 1; i++) {
                assertStoredPageRows(recorder, i, pageSize);
            }
            assertStoredPageRows(recorder, expectedPages - 1, totalRows % pageSize == 0 ? pageSize : totalRows % pageSize);

            releaseFirstPage(provider);
        }
    }

    public void testFirstPageIsHeldForResponse() {
        var recorder = new StorageRecorder();
        var provider = createTestProvider(3, recorder);
        try (var op = new StorePageOperator(provider, p -> p)) {
            op.addInput(buildPage(0, 5));

            // First page is captured immediately, before any store completes
            assertThat(provider.firstPage(), notNullValue());
            int firstPageRows = provider.firstPage().stream().mapToInt(Page::getPositionCount).sum();
            assertThat(firstPageRows, equalTo(3));

            op.finish();
            drainBlocked(op);
            releaseFirstPage(provider);
        }
    }

    public void testBackpressureAtConcurrencyLimit() {
        var recorder = new StorageRecorder();
        recorder.asyncMode = true;
        var provider = createTestProvider(1, recorder);
        try (var op = new StorePageOperator(provider, p -> p)) {
            // With pageSize=1, each row produces a page store.
            // Feed MAX_IN_FLIGHT_STORES rows — should not block yet (stores dispatched but under limit)
            for (int i = 0; i < StorePageOperator.MAX_IN_FLIGHT_STORES; i++) {
                op.addInput(buildPage(i, 1));
            }
            // Metadata store is also in-flight, so we may already be at the limit
            // Feed one more to ensure we hit the limit
            op.addInput(buildPage(StorePageOperator.MAX_IN_FLIGHT_STORES, 1));

            assertFalse("should be blocked at concurrency limit", op.needsInput());

            // Complete some pending stores to free up slots
            recorder.completeAllPending();

            // Should be unblocked now
            assertTrue("should accept input after stores complete", op.needsInput());

            op.finish();
            completeAllPendingUntilDone(op, recorder);
            releaseFirstPage(provider);
        }
    }

    public void testNoBlockingDuringNormalFlow() {
        var recorder = new StorageRecorder();
        var provider = createTestProvider(3, recorder);
        try (var op = new StorePageOperator(provider, p -> p)) {
            // With synchronous stores, addInput should never block
            op.addInput(buildPage(0, 3));
            assertTrue("should not be blocked with sync stores", op.isBlocked().listener().isDone());
            assertTrue("should accept more input", op.needsInput());

            op.addInput(buildPage(3, 3));
            assertTrue("should not be blocked with sync stores", op.isBlocked().listener().isDone());

            op.finish();
            drainBlocked(op);
            assertTrue(op.isFinished());

            releaseFirstPage(provider);
        }
    }

    public void testEmptyInput() {
        var recorder = new StorageRecorder();
        var provider = createTestProvider(3, recorder);
        try (var op = new StorePageOperator(provider, p -> p)) {
            op.finish();
            assertTrue(op.isFinished());
            assertThat(provider.totalPages(), equalTo(0));
            assertThat(provider.totalRows(), equalTo(0));
            assertThat(provider.firstPage(), nullValue());
            assertThat(recorder.storedPages.size(), equalTo(0));
            assertThat(recorder.metadataCreated, equalTo(false));
            assertThat(recorder.metadataFinalized, equalTo(false));
        }
    }

    public void testMapperIsApplied() {
        var recorder = new StorageRecorder();
        var provider = createTestProvider(10, recorder);
        var tbf = org.elasticsearch.compute.test.TestBlockFactory.getNonBreakingInstance();
        try (var op = new StorePageOperator(provider, page -> {
            IntBlock block = page.getBlock(0);
            try (IntBlock.Builder builder = tbf.newIntBlockBuilder(page.getPositionCount())) {
                for (int i = 0; i < page.getPositionCount(); i++) {
                    builder.appendInt(block.getInt(i) * 10);
                }
                Page result = new Page(builder.build());
                page.releaseBlocks();
                return result;
            }
        })) {
            op.addInput(buildPage(1, 3));
            op.finish();
            drainBlocked(op);

            assertThat(provider.totalRows(), equalTo(3));
            List<Page> firstPage = provider.firstPage();
            assertThat(firstPage, hasSize(1));
            IntBlock resultBlock = firstPage.get(0).getBlock(0);
            assertThat(resultBlock.getInt(0), equalTo(10));
            assertThat(resultBlock.getInt(1), equalTo(20));
            assertThat(resultBlock.getInt(2), equalTo(30));

            releaseFirstPage(provider);
            releaseRecorderPages(recorder);
        }
    }

    public void testManySmallInputPages() {
        var recorder = new StorageRecorder();
        var provider = createTestProvider(5, recorder);
        try (var op = new StorePageOperator(provider, p -> p)) {
            for (int i = 0; i < 12; i++) {
                op.addInput(buildPage(i, 1));
            }
            op.finish();
            drainBlocked(op);

            assertThat(provider.totalPages(), equalTo(3));
            assertThat(provider.totalRows(), equalTo(12));
            assertStoredPageRows(recorder, 0, 5);
            assertStoredPageRows(recorder, 1, 5);
            assertStoredPageRows(recorder, 2, 2);

            releaseFirstPage(provider);
        }
    }

    public void testMetadataCreatedAfterFirstPage() {
        var recorder = new StorageRecorder();
        var provider = createTestProvider(3, recorder);
        try (var op = new StorePageOperator(provider, p -> p)) {
            assertThat(recorder.metadataCreated, equalTo(false));

            op.addInput(buildPage(0, 3));

            assertThat(recorder.metadataCreated, equalTo(true));
            assertThat(recorder.metadataFinalized, equalTo(false));

            op.finish();
            drainBlocked(op);

            assertThat(recorder.metadataFinalized, equalTo(true));
            assertThat(recorder.finalizedTotalRows, equalTo(3));
            assertThat(recorder.finalizedTotalPages, equalTo(1));

            releaseFirstPage(provider);
        }
    }

    public void testFinishWaitsForInFlightStores() {
        var recorder = new StorageRecorder();
        recorder.asyncMode = true;
        var provider = createTestProvider(3, recorder);
        try (var op = new StorePageOperator(provider, p -> p)) {
            op.addInput(buildPage(0, 5));

            // First page is captured immediately, before any store completes
            assertThat("first page captured immediately", provider.firstPage(), notNullValue());
            // Metadata creation was dispatched
            assertThat("metadata store dispatched", recorder.metadataCreated, equalTo(true));

            op.finish();

            // finish() is waiting for in-flight stores
            assertFalse("not finished while stores are in-flight", op.isFinished());
            assertFalse("should not need input during finish", op.needsInput());

            // Complete all pending stores (metadata + pages + finalize)
            completeAllPendingUntilDone(op, recorder);

            assertTrue("finished after all stores and metadata finalize", op.isFinished());
            assertThat(recorder.metadataFinalized, equalTo(true));
            assertThat(recorder.storedPages.size(), equalTo(2));

            releaseFirstPage(provider);
        }
    }

    public void testStorageFailureFailsQuery() {
        var recorder = new StorageRecorder();
        recorder.failOnPageStore = true;
        var provider = createTestProvider(3, recorder);
        try (var op = new StorePageOperator(provider, p -> p)) {
            op.addInput(buildPage(0, 3));

            assertTrue("operator should be finished after failure", op.isFinished());
            var blockedResult = op.isBlocked();
            assertTrue("blocked listener should be resolved", blockedResult.listener().isDone());

            Exception failure = awaitFailure(blockedResult);
            assertThat(failure.getMessage(), equalTo("simulated storage failure"));

            releaseFirstPage(provider);
        }
    }

    public void testMetadataFailureFailsQuery() {
        var recorder = new StorageRecorder();
        recorder.failOnMetadataStore = true;
        var provider = createTestProvider(3, recorder);
        try (var op = new StorePageOperator(provider, p -> p)) {
            op.addInput(buildPage(0, 3));

            assertTrue("operator should be finished after failure", op.isFinished());
            var blockedResult = op.isBlocked();
            assertTrue("blocked listener should be resolved", blockedResult.listener().isDone());

            Exception failure = awaitFailure(blockedResult);
            assertThat(failure.getMessage(), equalTo("simulated metadata failure"));

            releaseFirstPage(provider);
        }
    }

    public void testMetadataFinalizeFailureFailsQuery() {
        var recorder = new StorageRecorder();
        recorder.failOnMetadataFinalize = true;
        var provider = createTestProvider(10, recorder);
        try (var op = new StorePageOperator(provider, p -> p)) {
            op.addInput(buildPage(0, 5));
            op.finish();
            drainBlocked(op);

            assertTrue("operator should be finished after finalize failure", op.isFinished());
            var blockedResult = op.isBlocked();
            assertTrue("blocked listener should be resolved", blockedResult.listener().isDone());

            Exception failure = awaitFailure(blockedResult);
            assertThat(failure.getMessage(), equalTo("simulated finalize failure"));

            releaseFirstPage(provider);
        }
    }

    public void testFinishIsIdempotent() {
        var recorder = new StorageRecorder();
        var provider = createTestProvider(3, recorder);
        try (var op = new StorePageOperator(provider, p -> p)) {
            op.finish();
            assertTrue(op.isFinished());
            op.finish();
            assertTrue(op.isFinished());
        }
    }

    // --- helpers ---

    private static PaginationStoreProvider createTestProvider(int pageSize) {
        return createTestProvider(pageSize, new StorageRecorder());
    }

    private static PaginationStoreProvider createTestProvider(int pageSize, StorageRecorder recorder) {
        PaginationContext ctx = new PaginationContext("test-cursor-id", pageSize, Long.MAX_VALUE, ZoneOffset.UTC, false);
        FakeCursorIndexService fakeService = new FakeCursorIndexService(recorder);
        PaginationStoreProvider provider = new PaginationStoreProvider(fakeService, ctx);
        provider.setColumns(List.of(new ColumnInfoImpl("val", "integer", null)));
        return provider;
    }

    private static Page buildPage(int startValue, int count) {
        try (
            IntBlock.Builder builder = org.elasticsearch.compute.test.TestBlockFactory.getNonBreakingInstance().newIntBlockBuilder(count)
        ) {
            for (int i = 0; i < count; i++) {
                builder.appendInt(startValue + i);
            }
            return new Page(builder.build());
        }
    }

    private static Exception awaitFailure(IsBlockedResult blockedResult) {
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        blockedResult.listener().addListener(future);
        return expectThrows(Exception.class, future::actionGet);
    }

    private static void completeAllPendingUntilDone(StorePageOperator op, StorageRecorder recorder) {
        int iterations = 0;
        while (op.isFinished() == false || op.isBlocked().listener().isDone() == false) {
            recorder.completeAllPending();
            iterations++;
            if (iterations > 50) {
                fail("operator is still not finished after 50 rounds of completing pending listeners");
            }
        }
    }

    private static void drainBlocked(StorePageOperator op) {
        int iterations = 0;
        while (op.isBlocked().listener().isDone() == false) {
            op.isBlocked().listener().addListener(ActionListener.noop());
            iterations++;
            if (iterations > 100) {
                fail("operator is still blocked after 100 iterations — likely a bug in async completion");
            }
        }
    }

    private static void assertStoredPageRows(StorageRecorder recorder, int pageIndex, int expectedRows) {
        List<List<Page>> pages = recorder.storedPages.get(pageIndex);
        assertThat("page " + pageIndex + " should be stored", pages, notNullValue());
        assertThat(pages, hasSize(greaterThan(0)));
        int totalRows = pages.get(pages.size() - 1).stream().mapToInt(Page::getPositionCount).sum();
        assertThat("page " + pageIndex + " should have " + expectedRows + " rows", totalRows, equalTo(expectedRows));
    }

    private static void releaseFirstPage(PaginationStoreProvider provider) {
        List<Page> firstPage = provider.firstPage();
        if (firstPage != null) {
            firstPage.forEach(Page::releaseBlocks);
        }
    }

    private static void releaseRecorderPages(StorageRecorder recorder) {
        for (List<List<Page>> pageVersions : recorder.storedPages.values()) {
            for (List<Page> pages : pageVersions) {
                pages.forEach(Page::releaseBlocks);
            }
        }
    }

    /**
     * Records storage calls made by the operator without requiring a real Elasticsearch client.
     */
    static class StorageRecorder {
        volatile boolean metadataCreated = false;
        volatile boolean metadataFinalized = false;
        volatile int finalizedTotalRows = -1;
        volatile int finalizedTotalPages = -1;
        final Map<Integer, List<List<Page>>> storedPages = new ConcurrentHashMap<>();
        boolean asyncMode = false;
        boolean failOnPageStore = false;
        boolean failOnMetadataStore = false;
        boolean failOnMetadataFinalize = false;
        final List<ActionListener<?>> pendingListeners = Collections.synchronizedList(new ArrayList<>());

        void completeAllPending() {
            List<ActionListener<?>> pending;
            synchronized (pendingListeners) {
                pending = new ArrayList<>(pendingListeners);
                pendingListeners.clear();
            }
            for (ActionListener<?> listener : pending) {
                @SuppressWarnings("unchecked")
                ActionListener<Object> typed = (ActionListener<Object>) listener;
                typed.onResponse(null);
            }
        }
    }

    /**
     * Simplified subclass that captures storage calls without requiring a real Elasticsearch cluster.
     * All methods that would use the client or cluster service are overridden.
     */
    static class FakeCursorIndexService extends EsqlCursorIndexService {
        private final StorageRecorder recorder;

        FakeCursorIndexService(StorageRecorder recorder) {
            super(new NoOpClient(threadPool), null, null, null);
            this.recorder = recorder;
        }

        @Override
        public void storeMetadataIncomplete(
            String cursorId,
            List<ColumnInfoImpl> columns,
            long expirationMillis,
            java.time.ZoneId zoneId,
            boolean columnar,
            TaskId taskId,
            ActionListener<String> listener
        ) {
            recorder.metadataCreated = true;
            if (recorder.failOnMetadataStore) {
                listener.onFailure(new RuntimeException("simulated metadata failure"));
            } else if (recorder.asyncMode) {
                recorder.pendingListeners.add(listener.map(v -> cursorId));
            } else {
                listener.onResponse(cursorId);
            }
        }

        @Override
        public void storePage(String cursorId, int pageIndex, List<Page> pageData, boolean needsDeepCopy, ActionListener<Void> listener) {
            List<Page> copies = pageData.stream().map(Page::shallowCopy).toList();
            recorder.storedPages.computeIfAbsent(pageIndex, k -> Collections.synchronizedList(new ArrayList<>())).add(copies);
            if (recorder.failOnPageStore) {
                listener.onFailure(new RuntimeException("simulated storage failure"));
            } else if (recorder.asyncMode) {
                recorder.pendingListeners.add(listener);
            } else {
                listener.onResponse(null);
            }
        }

        @Override
        public void updateMetadataComplete(String cursorId, int totalRows, int totalPages, ActionListener<Void> listener) {
            recorder.metadataFinalized = true;
            recorder.finalizedTotalRows = totalRows;
            recorder.finalizedTotalPages = totalPages;
            if (recorder.failOnMetadataFinalize) {
                listener.onFailure(new RuntimeException("simulated finalize failure"));
            } else if (recorder.asyncMode) {
                recorder.pendingListeners.add(listener);
            } else {
                listener.onResponse(null);
            }
        }

        @Override
        public void updateMetadataFailed(String cursorId, ActionListener<Void> listener) {
            listener.onResponse(null);
        }
    }
}
