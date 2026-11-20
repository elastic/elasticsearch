/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.FilterClient;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.reindex.AbstractBulkByPaginatedSearchRequest;
import org.elasticsearch.index.reindex.BulkByPaginatedSearchResponse;
import org.elasticsearch.index.reindex.BulkByPaginatedSearchTask;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.PaginatedSearchFailure;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.SystemIndexDescriptorUtils;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.reindex.PaginatedHitSource.Hit;
import org.elasticsearch.reindex.PaginatedHitSourceDropHitsTests.Cursor;
import org.elasticsearch.reindex.PaginatedHitSourceDropHitsTests.HeldAsyncResponse;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.UpdateByQueryScript;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.Scheduler.ScheduledCancellable;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;

import java.lang.ref.WeakReference;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.LongUnaryOperator;
import java.util.function.Supplier;

import static java.util.Collections.emptySet;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.reindex.PaginatedHitSourceDropHitsTests.assertUnreachable;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A reindex family task reserves one coordinating charge before each search and keeps that same charge with the page until the
 * page is released. A reservation that does not fit leaves the task running, with no page held, until it does fit or the task is
 * cancelled. A page whose own bytes are above the coordinating limit fails the task.
 */
public class ReindexCoordinatingIndexingPressureTests extends ESTestCase {

    /**
     * Bytes reserved for each document before the page exists. The search size is what the worker asked to fetch, so the
     * estimate is this many bytes times that size, plus the request context, and it stops at the coordinating limit.
     * Taken from the setting default, which is what a node reserves when the setting is unset.
     */
    private static final long ESTIMATED_HIT_BYTES = IndexingPressure.REINDEX_BYTES_ESTIMATE_PER_DOCUMENT.getDefault(Settings.EMPTY)
        .getBytes();

    private final List<ScriptedClock> clocks = new ArrayList<>();

    @After
    public void terminateClocks() {
        for (ScriptedClock clock : clocks) {
            terminate(clock);
        }
        clocks.clear();
    }

    /**
     * A full coordinating budget cannot take the next estimate. The task keeps running, sends no search and holds no hit.
     * Once the budget can take the estimate, that one search is sent, and it reserves half the limit because the raw estimate is above
     * that. The page then shrinks to its hits as soon as it arrives, and grows to the bulk that is built from it. Each move is counted
     * on its own estimate error counter.
     */
    public void testWaitingTaskSearchesOnceTheEstimateFits() {
        int pageSize = between(1, 3);
        ScriptedClock clock = new ScriptedClock();
        long estimate = rawEstimate(clock.getThreadPool(), pageSize);
        long context = clock.getThreadPool().getThreadContext().estimatedRequestContextBytes();
        long limit = estimate + between(1, 4_096);
        IndexingPressure pressure = pressure(limit);
        Harness harness = new Harness(clock, pressure, updateRequest(pageSize), NoopCircuitBreaker.INSTANCE, null);
        int sourceLength = between(32, 256);
        CountingHit hit = harness.hit(bytes(sourceLength));
        AtomicLong chargeAtBulk = new AtomicLong();
        harness.client.handler = (bulk, listener) -> {
            assertTrue(bulk.indexingPressureAccounted());
            chargeAtBulk.set(harness.chargedBytes());
            listener.onResponse(harness.success(bulk));
        };
        harness.onDone = harness::finishWithEmptyPage;
        try (Releasable ignored = pressure.markCoordinatingOperationStarted(1, pressure.getCoordinatingLimitInBytes(), false)) {
            harness.start();

            assertThat(harness.client.searches.size(), equalTo(0));
            assertThat(harness.client.bulks, equalTo(List.of()));
            assertThat(hit.releases.get(), equalTo(0));
            assertFalse(harness.listener.isDone());
            assertTrue("the refused estimate has to be tried again", harness.queue.hasDeferredTasks());
            assertThat(harness.chargedBytes(), equalTo(pressure.getCoordinatingLimitInBytes()));
            assertThat(pressure.stats().getReindexPageReserveRejections(), equalTo(1L));
        }

        harness.queue.advanceTime();
        assertThat(harness.client.searches.size(), equalTo(1));
        assertThat("the raw estimate is above half the limit", estimate, greaterThan(halfTheLimit(pressure)));
        assertThat(harness.chargedBytes(), equalTo(halfTheLimit(pressure)));
        harness.feed(List.of(hit));

        BulkByPaginatedSearchResponse response = harness.listener.actionGet(TimeValue.ZERO);
        assertThat(response.getUpdated(), equalTo(1L));
        assertThat(harness.client.searches.size(), equalTo(1));
        assertThat(harness.chargedBytes(), equalTo(0L));
        assertThat(hit.releases.get(), equalTo(1));
        long hitBytes = sourceLength + context;
        assertThat("the bulk is bigger than the source it carries", chargeAtBulk.get(), greaterThan(hitBytes));
        assertThat(pressure.stats().getReindexPageReserveRejections(), equalTo(1L));
        assertThat(
            "shrunk from the estimate to the hit source",
            pressure.stats().getReindexPageBytesBelowEstimate(),
            equalTo(halfTheLimit(pressure) - hitBytes)
        );
        assertThat(
            "grown from the hit source to the bulk",
            pressure.stats().getReindexPageBytesAboveEstimate(),
            equalTo(chargeAtBulk.get() - hitBytes)
        );
        assertThat(pressure.stats().getReindexPageExceedsLimit(), equalTo(0L));
    }

    /**
     * Three tasks share a budget whose limit is the raw estimate of one page, so each reserve is cut to half the limit. The first task
     * holds a small page. The second task's half still fits beside it. The third task's half does not, because the first page and two
     * halves are more than the limit, so it does not search. Releasing the first page lets the third search once, and all three
     * finish with nothing still charged.
     */
    public void testThirdTaskSearchesOnceTheHeldPageReleasesItsCharge() {
        int pageSize = 1;
        ScriptedClock clock = new ScriptedClock();
        long estimate = rawEstimate(clock.getThreadPool(), pageSize);
        IndexingPressure pressure = pressure(estimate);
        Harness first = new Harness(clock, pressure, updateRequest(pageSize), NoopCircuitBreaker.INSTANCE, null);
        Harness second = new Harness(clock, pressure, updateRequest(pageSize), NoopCircuitBreaker.INSTANCE, null);
        Harness third = new Harness(clock, pressure, updateRequest(pageSize), NoopCircuitBreaker.INSTANCE, null);
        CountingHit held = first.hit(bytes(between(64, 512)));
        AtomicReference<ActionListener<BulkResponse>> heldListener = new AtomicReference<>();
        first.client.handler = (bulk, listener) -> {
            first.client.bulks.add(bulk);
            heldListener.set(listener);
        };
        first.onDone = first::finishWithEmptyPage;
        first.start();
        assertThat(first.client.searches.size(), equalTo(1));
        first.feed(List.of(held));

        assertThat(first.client.bulks.size(), equalTo(1));
        assertThat(held.releases.get(), equalTo(0));
        long firstPage = first.chargedBytes();
        assertThat(firstPage, greaterThan(0L));
        assertThat(firstPage, lessThan(halfTheLimit(pressure)));
        assertFalse(first.listener.isDone());

        second.start();
        assertThat("a half reserve fits beside the held page", second.client.searches.size(), equalTo(1));
        assertFalse(clock.hasDeferredTasks());
        assertThat(pressure.stats().getCurrentCoordinatingBytes(), equalTo(firstPage + halfTheLimit(pressure)));

        third.start();
        assertThat(third.client.searches.size(), equalTo(0));
        assertThat(third.client.bulks, equalTo(List.of()));
        assertFalse(third.listener.isDone());
        assertTrue(clock.hasDeferredTasks());
        assertThat(pressure.stats().getReindexPageReserveRejections(), equalTo(1L));
        assertThat(pressure.stats().getCurrentCoordinatingBytes(), equalTo(firstPage + halfTheLimit(pressure)));

        CountingHit secondHit = second.hit(bytes(between(64, 512)));
        CountingHit thirdHit = third.hit(bytes(between(64, 512)));
        second.client.handler = (bulk, listener) -> {
            assertTrue(bulk.indexingPressureAccounted());
            listener.onResponse(second.success(bulk));
        };
        second.onDone = second::finishWithEmptyPage;
        third.client.handler = (bulk, listener) -> {
            assertTrue(bulk.indexingPressureAccounted());
            listener.onResponse(third.success(bulk));
        };
        third.onDone = third::finishWithEmptyPage;
        heldListener.get().onResponse(first.success(first.client.bulks.get(0)));
        assertThat(first.listener.actionGet(TimeValue.ZERO).getUpdated(), equalTo(1L));
        assertThat(held.releases.get(), equalTo(1));
        assertThat(third.client.searches.size(), equalTo(0));

        clock.advanceTime();
        assertThat(third.client.searches.size(), equalTo(1));
        assertThat(pressure.stats().getCurrentCoordinatingBytes(), equalTo(2 * halfTheLimit(pressure)));
        second.feed(List.of(secondHit));
        assertThat(second.listener.actionGet(TimeValue.ZERO).getUpdated(), equalTo(1L));
        third.feed(List.of(thirdHit));
        assertThat(third.listener.actionGet(TimeValue.ZERO).getUpdated(), equalTo(1L));
        assertThat(secondHit.releases.get(), equalTo(1));
        assertThat(thirdHit.releases.get(), equalTo(1));
        assertThat(pressure.stats().getCurrentCoordinatingBytes(), equalTo(0L));
    }

    public void testCoordinatingBytesStayElevatedWhileARejectionWaitsToRetry() {
        Harness harness = new Harness(pressure(Settings.EMPTY), updateRequest(1), NoopCircuitBreaker.INSTANCE, null);
        CountingHit hit = harness.hit(bytes(between(1_000, 4_000)));
        AtomicLong charged = new AtomicLong();
        harness.client.handler = (bulk, listener) -> {
            harness.client.bulks.add(bulk);
            if (harness.client.bulks.size() == 1) {
                charged.set(harness.chargedBytes());
                assertTrue(bulk.indexingPressureAccounted());
                long contextBytes = harness.queue.getThreadPool().getThreadContext().estimatedRequestContextBytes();
                assertThat(charged.get(), equalTo(bulk.ramBytesUsed() + contextBytes));
                listener.onFailure(new EsRejectedExecutionException("full"));
                // The failed attempt's listener has run. The page is still the thing holding the charge.
                assertThat(harness.chargedBytes(), equalTo(charged.get()));
            } else {
                assertThat(bulk, sameInstance(harness.client.bulks.get(0)));
                assertTrue(bulk.indexingPressureAccounted());
                assertThat(harness.chargedBytes(), equalTo(charged.get()));
                listener.onResponse(harness.success(bulk));
            }
        };
        harness.onDone = harness::finishWithEmptyPage;
        harness.start();
        harness.feed(List.of(hit));

        assertThat(harness.client.bulks.size(), equalTo(1));
        assertTrue(harness.queue.hasDeferredTasks());
        assertThat(harness.chargedBytes(), equalTo(charged.get()));
        assertFalse(harness.listener.isDone());

        harness.queue.advanceTime();
        BulkByPaginatedSearchResponse response = harness.listener.actionGet(TimeValue.ZERO);
        assertThat(response.getUpdated(), equalTo(1L));
        assertThat(harness.chargedBytes(), equalTo(0L));
        assertThat(hit.releases.get(), equalTo(1));
    }

    public void testCoordinatingBytesDropWhenRetriesAreExhausted() {
        UpdateByQueryRequest request = updateRequest(1);
        request.setMaxRetries(1);
        Harness harness = new Harness(pressure(Settings.EMPTY), request, NoopCircuitBreaker.INSTANCE, null);
        CountingHit hit = harness.hit(bytes(between(1_000, 4_000)));
        AtomicLong charged = new AtomicLong();
        harness.client.handler = (bulk, listener) -> {
            harness.client.bulks.add(bulk);
            if (harness.client.bulks.size() == 1) {
                charged.set(harness.chargedBytes());
            }
            listener.onFailure(new EsRejectedExecutionException("full"));
        };
        harness.start();
        harness.feed(List.of(hit));

        assertThat(harness.chargedBytes(), equalTo(charged.get()));
        assertThat(charged.get(), greaterThan(0L));
        assertTrue(harness.queue.hasDeferredTasks());

        harness.queue.advanceTime();
        EsRejectedExecutionException rejected = expectThrows(EsRejectedExecutionException.class, harness.listener::actionGet);
        assertThat(rejected.getMessage(), equalTo("full"));
        assertThat(harness.client.bulks.size(), equalTo(2));
        assertThat(harness.chargedBytes(), equalTo(0L));
        assertThat(hit.releases.get(), equalTo(1));
        assertFalse(harness.queue.hasDeferredTasks());
    }

    public void testPartialRetryDoesNotTakeASecondCoordinatingCharge() {
        Harness harness = new Harness(pressure(Settings.EMPTY), updateRequest(1), NoopCircuitBreaker.INSTANCE, null);
        CountingHit kept = harness.hit(bytes(between(1_000, 4_000)));
        CountingHit rejected = harness.hit(bytes(between(1_000, 4_000)));
        AtomicLong charged = new AtomicLong();
        AtomicReference<BulkRequest> retry = new AtomicReference<>();
        AtomicReference<ActionListener<BulkResponse>> retryListener = new AtomicReference<>();
        harness.client.handler = (bulk, listener) -> {
            harness.client.bulks.add(bulk);
            if (harness.client.bulks.size() == 1) {
                charged.set(harness.chargedBytes());
                listener.onResponse(oneRejected(bulk));
            } else {
                retry.set(bulk);
                retryListener.set(listener);
            }
        };
        harness.onDone = harness::finishWithEmptyPage;
        harness.start();
        harness.feed(List.of(kept, rejected));

        assertThat(charged.get(), greaterThan(0L));
        assertThat(harness.chargedBytes(), equalTo(charged.get()));
        harness.queue.advanceTime();

        BulkRequest retryBulk = retry.get();
        assertThat(retryBulk, not(sameInstance(harness.client.bulks.get(0))));
        assertTrue(retryBulk.indexingPressureAccounted());
        assertThat(retryBulk.numberOfActions(), equalTo(1));
        assertThat(retryBulk.ramBytesUsed(), lessThan(charged.get()));
        assertThat(harness.chargedBytes(), equalTo(charged.get()));
        assertThat(kept.releases.get(), equalTo(0));
        assertThat(rejected.releases.get(), equalTo(0));

        retryListener.get().onResponse(harness.success(retryBulk));
        assertThat(harness.listener.actionGet(TimeValue.ZERO).getUpdated(), equalTo(2L));
        assertThat(harness.chargedBytes(), equalTo(0L));
        assertThat(kept.releases.get(), equalTo(1));
        assertThat(rejected.releases.get(), equalTo(1));
    }

    /**
     * The previous page's charge is already gone before the next estimate is reserved. The bytes are therefore not zero
     * once the next page is being fetched. They are the next estimate, then that page's own size.
     */
    public void testSuccessfulBatchReleasesItsChargeBeforeTheNextPage() {
        int pageSize = 1;
        ScriptedClock clock = new ScriptedClock();
        long estimate = rawEstimate(clock.getThreadPool(), pageSize);
        long limit = estimate + estimate;
        WatchingPressure pressure = new WatchingPressure(limit);
        Harness harness = new Harness(clock, pressure, updateRequest(pageSize), NoopCircuitBreaker.INSTANCE, null);
        CountingHit first = harness.hit(bytes(between(64, 512)));
        CountingHit second = harness.hit(bytes(between(64, 512)));
        AtomicInteger pages = new AtomicInteger();
        List<Long> charges = new ArrayList<>();
        harness.client.handler = (bulk, listener) -> {
            charges.add(harness.chargedBytes());
            assertTrue(bulk.indexingPressureAccounted());
            long contextBytes = harness.queue.getThreadPool().getThreadContext().estimatedRequestContextBytes();
            assertThat(harness.chargedBytes(), equalTo(bulk.ramBytesUsed() + contextBytes));
            listener.onResponse(harness.success(bulk));
        };
        harness.onDone = () -> {
            if (pages.getAndIncrement() == 0) {
                harness.feed(List.of(second));
            } else {
                harness.action.setScroll(null);
                harness.finishWithEmptyPage();
            }
        };
        harness.action.setScroll("more");
        harness.start();
        harness.feed(List.of(first));

        BulkByPaginatedSearchResponse response = harness.listener.actionGet(TimeValue.ZERO);
        assertThat(response.getUpdated(), equalTo(2L));
        assertThat(charges.size(), equalTo(2));
        assertThat(charges.get(0), greaterThan(0L));
        assertThat(charges.get(1), greaterThan(0L));
        // The first mark is the opening search. Every later mark is the next page, and the previous page has already let go.
        assertThat(pressure.bytesBeforeReservation.size(), greaterThanOrEqualTo(2));
        for (int i = 1; i < pressure.bytesBeforeReservation.size(); i++) {
            assertThat(pressure.bytesBeforeReservation.get(i), equalTo(0L));
        }
        assertThat(harness.chargedBytes(), equalTo(0L));
        assertThat(first.releases.get(), equalTo(1));
        assertThat(second.releases.get(), equalTo(1));
    }

    public void testCancellingDuringBackoffReleasesTheChargeWhenTheAttemptRuns() {
        Harness harness = new Harness(pressure(Settings.EMPTY), updateRequest(1), NoopCircuitBreaker.INSTANCE, null);
        CountingHit hit = harness.hit(bytes(between(1_000, 4_000)));
        harness.client.handler = (bulk, listener) -> {
            harness.client.bulks.add(bulk);
            listener.onFailure(new EsRejectedExecutionException("full"));
        };
        harness.start();
        harness.feed(List.of(hit));

        assertThat(harness.chargedBytes(), greaterThan(0L));
        assertTrue(harness.queue.hasDeferredTasks());
        harness.taskManager.cancel(harness.task, "stopped", () -> {});
        assertThat(harness.chargedBytes(), greaterThan(0L));
        assertFalse(harness.listener.isDone());

        harness.queue.advanceTime();
        assertThat(harness.client.bulks.size(), equalTo(1));
        assertThat(harness.chargedBytes(), equalTo(0L));
        BulkByPaginatedSearchResponse response = harness.listener.actionGet(TimeValue.ZERO);
        assertThat(response.getReasonCancelled(), equalTo("stopped"));
        assertThat(hit.releases.get(), equalTo(1));
    }

    public void testUnscriptedUpdateByQueryChargesTheSharedHitSource() {
        Harness harness = new Harness(pressure(Settings.EMPTY), updateRequest(1), NoopCircuitBreaker.INSTANCE, null);
        CountingHit hit = harness.hit(bytes(between(1_000, 4_000)));
        harness.client.handler = (bulk, listener) -> {
            IndexRequest index = (IndexRequest) bulk.requests().get(0);
            long charged = harness.chargedBytes();
            long contextBytes = harness.queue.getThreadPool().getThreadContext().estimatedRequestContextBytes();
            assertThat(index.source(), sameInstance(hit.getSource()));
            assertThat(charged, greaterThanOrEqualTo((long) hit.getSource().length()));
            assertThat(charged, equalTo(bulk.ramBytesUsed() + contextBytes));
            listener.onResponse(harness.success(bulk));
        };
        harness.onDone = harness::finishWithEmptyPage;
        harness.start();
        harness.feed(List.of(hit));
        assertThat(harness.listener.actionGet(TimeValue.ZERO).getUpdated(), equalTo(1L));
        assertThat(harness.chargedBytes(), equalTo(0L));
        assertThat(hit.releases.get(), equalTo(1));
    }

    /**
     * A script replaces the source, so the charge follows the new source and the original hit is released once.
     * A rejected attempt keeps those same bytes until the page itself is released.
     */
    public void testScriptedUpdateKeepsTheNewSourceChargeAcrossARejection() {
        ScriptService scripts = mock(ScriptService.class);
        when(scripts.compile(any(), eq(UpdateByQueryScript.CONTEXT))).thenReturn((params, ctx) -> new UpdateByQueryScript(Map.of(), ctx) {
            @Override
            public void execute() {
                @SuppressWarnings("unchecked")
                Map<String, Object> source = (Map<String, Object>) getCtx().get("_source");
                source.clear();
                source.put("a", "b");
            }
        });
        UpdateByQueryRequest request = updateRequest(1);
        request.setScript(mockScript("shrink"));
        Harness harness = new Harness(pressure(Settings.EMPTY), request, NoopCircuitBreaker.INSTANCE, scripts);
        String json = "{\"p\":\"" + "x".repeat(between(2_000, 4_000)) + "\"}";
        CountingHit hit = harness.hit(new BytesArray(json.getBytes(StandardCharsets.UTF_8)));
        AtomicLong charged = new AtomicLong();
        harness.client.handler = (bulk, listener) -> {
            harness.client.bulks.add(bulk);
            IndexRequest index = (IndexRequest) bulk.requests().get(0);
            long contextBytes = harness.queue.getThreadPool().getThreadContext().estimatedRequestContextBytes();
            assertThat(index.source().length(), lessThan(hit.getSource().length() / 10));
            assertThat(hit.releases.get(), equalTo(1));
            if (harness.client.bulks.size() == 1) {
                charged.set(harness.chargedBytes());
                assertThat(charged.get(), greaterThanOrEqualTo((long) index.source().length()));
                assertThat(charged.get(), lessThan((long) hit.getSource().length()));
                assertThat(charged.get(), equalTo(bulk.ramBytesUsed() + contextBytes));
                listener.onFailure(new EsRejectedExecutionException("full"));
                assertThat(harness.chargedBytes(), equalTo(charged.get()));
            } else {
                assertTrue(bulk.indexingPressureAccounted());
                assertThat(harness.chargedBytes(), equalTo(charged.get()));
                listener.onResponse(harness.success(bulk));
            }
        };
        harness.onDone = harness::finishWithEmptyPage;
        harness.start();
        harness.feed(List.of(hit));
        assertTrue(harness.queue.hasDeferredTasks());
        harness.queue.advanceTime();
        assertThat(harness.listener.actionGet(TimeValue.ZERO).getUpdated(), equalTo(1L));
        assertThat(harness.chargedBytes(), equalTo(0L));
        assertThat(hit.releases.get(), equalTo(1));
    }

    public void testDeleteByQueryChargesTheDeleteAndNotTheHitSource() {
        DeleteByQueryRequest request = new DeleteByQueryRequest(randomAlphaOfLength(6));
        request.getSearchRequest().source().size(1);
        request.setRetryBackoffInitialTime(TimeValue.timeValueMillis(randomIntBetween(1, 40)));
        Harness harness = new Harness(pressure(Settings.EMPTY), request, NoopCircuitBreaker.INSTANCE, null);
        CountingHit hit = harness.hit(bytes(between(4_000, 8_000)));
        harness.client.handler = (bulk, listener) -> {
            assertThat(bulk.requests().get(0), instanceOf(DeleteRequest.class));
            long charged = harness.chargedBytes();
            long contextBytes = harness.queue.getThreadPool().getThreadContext().estimatedRequestContextBytes();
            assertThat(charged, lessThan(hit.getSource().length() / 4L));
            assertThat(charged, equalTo(bulk.ramBytesUsed() + contextBytes));
            listener.onResponse(harness.success(bulk));
        };
        harness.onDone = harness::finishWithEmptyPage;
        harness.start();
        harness.feed(List.of(hit));
        assertThat(harness.listener.actionGet(TimeValue.ZERO).getDeleted(), equalTo(1L));
        assertThat(harness.chargedBytes(), equalTo(0L));
        assertThat(hit.releases.get(), equalTo(1));
    }

    public void testDeleteByQueryReleasesTheHitBeforeARejectedBulkRetries() {
        DeleteByQueryRequest request = new DeleteByQueryRequest(randomAlphaOfLength(6));
        request.getSearchRequest().source().size(1);
        request.setRetryBackoffInitialTime(TimeValue.timeValueMillis(randomIntBetween(1, 40)));
        Harness harness = new Harness(pressure(Settings.EMPTY), request, NoopCircuitBreaker.INSTANCE, null);
        CountingHit hit = harness.hit(bytes(between(4_000, 8_000)));
        AtomicInteger attempts = new AtomicInteger();
        harness.client.handler = (bulk, listener) -> {
            long contextBytes = harness.queue.getThreadPool().getThreadContext().estimatedRequestContextBytes();
            assertThat(hit.releases.get(), equalTo(1));
            assertThat(harness.chargedBytes(), equalTo(bulk.ramBytesUsed() + contextBytes));
            assertThat(harness.chargedBytes(), lessThan(hit.getSource().length() / 4L));
            if (attempts.getAndIncrement() == 0) {
                listener.onFailure(new EsRejectedExecutionException("full"));
                assertThat(hit.releases.get(), equalTo(1));
            } else {
                listener.onResponse(harness.success(bulk));
            }
        };
        harness.onDone = harness::finishWithEmptyPage;
        harness.start();
        harness.feed(List.of(hit));
        assertTrue(harness.queue.hasDeferredTasks());
        harness.queue.advanceTime();
        assertThat(harness.listener.actionGet(TimeValue.ZERO).getDeleted(), equalTo(1L));
        assertThat(harness.chargedBytes(), equalTo(0L));
        assertThat(hit.releases.get(), equalTo(1));
    }

    public void testReindexOfOneDocumentReleasesItsCharge() {
        ReindexRequest request = reindexRequest(1);
        Harness harness = new Harness(pressure(Settings.EMPTY), request, NoopCircuitBreaker.INSTANCE, null);
        CountingHit hit = harness.hit(bytes(between(32, 256)), -1L);
        harness.client.handler = (bulk, listener) -> {
            assertTrue(bulk.indexingPressureAccounted());
            long contextBytes = harness.queue.getThreadPool().getThreadContext().estimatedRequestContextBytes();
            assertThat(harness.chargedBytes(), equalTo(bulk.ramBytesUsed() + contextBytes));
            // A reindex of a document that is not already there creates it.
            listener.onResponse(harness.success(bulk, true));
        };
        harness.onDone = harness::finishWithEmptyPage;
        harness.start();
        harness.feed(List.of(hit));
        BulkByPaginatedSearchResponse response = harness.listener.actionGet(TimeValue.ZERO);
        assertThat(response.getCreated(), equalTo(1L));
        assertThat(response.getBulkFailures(), equalTo(List.of()));
        assertThat(harness.chargedBytes(), equalTo(0L));
        assertThat(hit.releases.get(), equalTo(1));
    }

    public void testUpdateByQueryCompletesAndReleasesItsCharge() {
        Harness harness = new Harness(pressure(Settings.EMPTY), updateRequest(1), NoopCircuitBreaker.INSTANCE, null);
        CountingHit hit = harness.hit(bytes(between(8, 64)));
        AtomicLong charged = new AtomicLong();
        harness.client.handler = (bulk, listener) -> {
            charged.set(harness.chargedBytes());
            assertTrue(bulk.indexingPressureAccounted());
            listener.onResponse(harness.success(bulk));
        };
        harness.onDone = harness::finishWithEmptyPage;
        harness.start();
        harness.feed(List.of(hit));

        BulkByPaginatedSearchResponse response = harness.listener.actionGet(TimeValue.ZERO);
        assertThat(response.getUpdated(), equalTo(1L));
        assertThat(response.getBulkFailures(), equalTo(List.of()));
        assertThat(charged.get(), greaterThan(0L));
        assertThat(harness.chargedBytes(), equalTo(0L));
        assertThat(response.getReasonCancelled(), nullValue());
    }

    /**
     * A page whose script drops every hit still has to let go of its charge before the next search is reserved.
     * The following page is then held at its own size, not left at zero.
     */
    public void testScriptNoopReleasesThePageChargeBeforeTheNextSearch() {
        ScriptService scripts = mock(ScriptService.class);
        when(scripts.compile(any(), eq(UpdateByQueryScript.CONTEXT))).thenReturn((params, ctx) -> new UpdateByQueryScript(Map.of(), ctx) {
            @Override
            public void execute() {
                @SuppressWarnings("unchecked")
                Map<String, Object> source = (Map<String, Object>) getCtx().get("_source");
                if (source.containsKey("skip")) {
                    getCtx().put("op", "noop");
                }
            }
        });
        int pageSize = 1;
        ScriptedClock clock = new ScriptedClock();
        WatchingPressure pressure = new WatchingPressure(rawEstimate(clock.getThreadPool(), pageSize) * 4);
        UpdateByQueryRequest request = updateRequest(pageSize);
        request.setScript(mockScript("skip"));
        Harness harness = new Harness(clock, pressure, request, NoopCircuitBreaker.INSTANCE, scripts);
        CountingHit skipped = harness.hit(new BytesArray("{\"skip\":true}".getBytes(StandardCharsets.UTF_8)));
        CountingHit kept = harness.hit(new BytesArray("{\"keep\":true}".getBytes(StandardCharsets.UTF_8)));
        AtomicInteger pages = new AtomicInteger();
        AtomicLong keptCharge = new AtomicLong();
        harness.client.handler = (bulk, listener) -> {
            harness.client.bulks.add(bulk);
            keptCharge.set(harness.chargedBytes());
            assertThat(keptCharge.get(), greaterThan(0L));
            assertTrue(bulk.indexingPressureAccounted());
            listener.onResponse(harness.success(bulk));
        };
        harness.onDone = () -> {
            if (pages.getAndIncrement() == 0) {
                harness.feed(List.of(kept));
            } else {
                harness.action.setScroll(null);
                harness.finishWithEmptyPage();
            }
        };
        harness.action.setScroll("more");
        harness.start();
        harness.feed(List.of(skipped));

        BulkByPaginatedSearchResponse response = harness.listener.actionGet(TimeValue.ZERO);
        assertThat(response.getUpdated(), equalTo(1L));
        assertThat(response.getStatus().getNoops(), equalTo(1L));
        assertThat(harness.client.bulks.size(), equalTo(1));
        assertThat(pressure.bytesBeforeReservation.size(), greaterThanOrEqualTo(2));
        assertThat(pressure.bytesBeforeReservation.get(1), equalTo(0L));
        assertThat(keptCharge.get(), greaterThan(0L));
        assertThat(harness.chargedBytes(), equalTo(0L));
        // The script drops the hit when it copies the source. Page cleanup walks that same hit and must leave it alone.
        assertThat(skipped.releases.get(), equalTo(1));
        assertThat(kept.releases.get(), equalTo(1));
    }

    public void testSearchRejectionReleasesTheReservationWhenRetriesAreExhausted() {
        UpdateByQueryRequest request = updateRequest(1);
        request.setMaxRetries(1);
        Harness harness = new Harness(pressure(Settings.EMPTY), request, NoopCircuitBreaker.INSTANCE, null);
        harness.start();
        assertThat(harness.client.searches.size(), equalTo(1));
        long reserved = harness.chargedBytes();
        assertThat(reserved, greaterThan(0L));

        harness.client.searches.get(0).onFailure(new EsRejectedExecutionException("busy"));
        assertThat(harness.chargedBytes(), equalTo(reserved));
        assertThat(harness.client.bulks, equalTo(List.of()));
        assertTrue(harness.queue.hasDeferredTasks());
        assertFalse(harness.listener.isDone());

        harness.queue.advanceTime();
        assertThat(harness.client.searches.size(), equalTo(2));
        assertThat(harness.chargedBytes(), equalTo(reserved));
        harness.client.searches.get(1).onFailure(new EsRejectedExecutionException("busy"));

        EsRejectedExecutionException rejected = expectThrows(EsRejectedExecutionException.class, harness.listener::actionGet);
        assertThat(rejected.getMessage(), equalTo("busy"));
        assertThat(harness.client.bulks, equalTo(List.of()));
        assertThat(harness.chargedBytes(), equalTo(0L));
    }

    public void testEmptyPageReleasesTheReservation() {
        Harness harness = new Harness(pressure(Settings.EMPTY), updateRequest(1), NoopCircuitBreaker.INSTANCE, null);
        harness.start();
        assertThat(harness.chargedBytes(), greaterThan(0L));
        harness.feed(List.of());
        BulkByPaginatedSearchResponse response = harness.listener.actionGet(TimeValue.ZERO);
        assertThat(response.getUpdated(), equalTo(0L));
        assertThat(harness.client.bulks, equalTo(List.of()));
        assertThat(harness.chargedBytes(), equalTo(0L));
        assertThat(response.getReasonCancelled(), nullValue());
    }

    public void testShardFailuresReleaseTheReservationWithoutABulk() {
        Harness harness = new Harness(pressure(Settings.EMPTY), updateRequest(1), NoopCircuitBreaker.INSTANCE, null);
        CountingHit hit = harness.hit(bytes(between(32, 128)));
        harness.start();
        harness.feed(List.of(hit), false, List.of(new PaginatedSearchFailure(new IllegalStateException("shard failed"))));
        BulkByPaginatedSearchResponse response = harness.listener.actionGet(TimeValue.ZERO);
        assertThat(response.getSearchFailures().size(), equalTo(1));
        assertThat(harness.client.bulks, equalTo(List.of()));
        assertThat(harness.chargedBytes(), equalTo(0L));
        assertThat(hit.releases.get(), equalTo(1));
    }

    public void testTimedOutSearchReleasesTheReservationWithoutABulk() {
        Harness harness = new Harness(pressure(Settings.EMPTY), updateRequest(1), NoopCircuitBreaker.INSTANCE, null);
        CountingHit hit = harness.hit(bytes(between(32, 128)));
        harness.start();
        harness.feed(List.of(hit), true, List.of());
        BulkByPaginatedSearchResponse response = harness.listener.actionGet(TimeValue.ZERO);
        assertTrue(response.isTimedOut());
        assertThat(harness.client.bulks, equalTo(List.of()));
        assertThat(harness.chargedBytes(), equalTo(0L));
        assertThat(hit.releases.get(), equalTo(1));
    }

    /**
     * Cancelling while the estimate is waiting must not send the search, and must not touch the budget that was already in use.
     */
    public void testCancelWhileTheReservationWaitsSendsNoSearch() {
        int pageSize = between(1, 3);
        ScriptedClock clock = new ScriptedClock();
        long estimate = rawEstimate(clock.getThreadPool(), pageSize);
        long limit = estimate + between(1, 2_048);
        IndexingPressure pressure = pressure(limit);
        Harness harness = new Harness(clock, pressure, updateRequest(pageSize), NoopCircuitBreaker.INSTANCE, null);
        try (Releasable ignored = pressure.markCoordinatingOperationStarted(1, pressure.getCoordinatingLimitInBytes(), false)) {
            harness.start();
            assertThat(harness.client.searches.size(), equalTo(0));
            assertTrue(harness.queue.hasDeferredTasks());
            long budget = harness.chargedBytes();
            assertThat(budget, equalTo(pressure.getCoordinatingLimitInBytes()));

            harness.taskManager.cancel(harness.task, "stopped", () -> {});
            if (harness.queue.hasDeferredTasks()) {
                harness.queue.advanceTime();
            }
            BulkByPaginatedSearchResponse response = harness.listener.actionGet(TimeValue.ZERO);
            assertThat(response.getReasonCancelled(), equalTo("stopped"));
            assertThat(harness.client.searches.size(), equalTo(0));
            assertThat(harness.client.bulks, equalTo(List.of()));
            assertThat(harness.chargedBytes(), equalTo(budget));
        }
        assertThat(pressure.stats().getCurrentCoordinatingBytes(), equalTo(0L));
    }

    /**
     * A task that is waiting for budget to reserve its next page can be cancelled, and it finishes when the next reservation poll runs,
     * not at the moment of the cancel. Until then it has not finished. Once the poll has run the listener hears exactly once, the cancel
     * reason is reported, and the task leaves nothing charged behind. The poll comes after the request's retry backoff, kept between
     * 50 ms and 1 second.
     */
    public void testCancellingATaskThatIsWaitingToReserveAPageFinishesItAndLeavesNothingCharged() {
        TimeValue backoff = TimeValue.timeValueMillis(between(0, 5_000));
        long expectedPollMillis = Math.max(50L, Math.min(backoff.millis(), 1_000L));
        assertCancelFinishesOnTheNextPoll(backoff, expectedPollMillis);
    }

    /**
     * A user can set a retry_backoff of minutes. The reservation poll is capped at 1 second, so a cancelled task is noticed within a
     * second whatever the backoff, and the test waits for the cap and not for the user's backoff.
     */
    public void testCancelledTaskIsNoticedWithinOneSecondWhateverTheRetryBackoffIs() {
        TimeValue backoff = TimeValue.timeValueMillis(between(1_001, 600_000));
        assertCancelFinishesOnTheNextPoll(backoff, 1_000L);
    }

    private void assertCancelFinishesOnTheNextPoll(TimeValue backoff, long expectedPollMillis) {
        int pageSize = between(1, 3);
        ScriptedClock clock = new ScriptedClock();
        long limit = rawEstimate(clock.getThreadPool(), pageSize) + between(0, 4_096);
        IndexingPressure pressure = pressure(limit);
        long baseline = pressure.stats().getCurrentCoordinatingBytes();
        UpdateByQueryRequest request = updateRequest(pageSize);
        request.setRetryBackoffInitialTime(backoff);
        Harness harness = new Harness(clock, pressure, request, NoopCircuitBreaker.INSTANCE, null);
        try (Releasable ignored = pressure.markCoordinatingOperationStarted(1, limit, false)) {
            harness.start();
            // Let it be refused a few times first, so the cancel lands on a task that has really been waiting.
            for (int i = between(0, 3); i > 0 && clock.hasDeferredTasks(); i--) {
                clock.advanceTime();
            }
            assertTrue("the refused reserve is waiting for its next poll", clock.hasDeferredTasks());
            assertFalse(harness.listener.isDone());
            assertThat(harness.client.searches.size(), equalTo(0));
            assertThat("only the other holder's bytes", harness.chargedBytes(), equalTo(baseline + limit));

            harness.taskManager.cancel(harness.task, "stopped", () -> {});

            assertFalse("the task finishes when the poll runs and not when it is cancelled", harness.listener.isDone());
            assertThat("the poll is the only thing waiting", clock.deferredCount(), equalTo(1));
            assertThat(clock.nextDelay().millis(), equalTo(expectedPollMillis));
            long cancelledAtMillis = clock.relativeTimeInMillis();

            clock.advanceTime();

            assertThat("the poll ran after its interval", clock.relativeTimeInMillis() - cancelledAtMillis, equalTo(expectedPollMillis));
            assertThat("the task is noticed within a second", clock.relativeTimeInMillis() - cancelledAtMillis, lessThanOrEqualTo(1_000L));
            assertTrue("the cancelled task must finish", harness.listener.isDone());
            assertThat(harness.listener.actionGet(TimeValue.ZERO).getReasonCancelled(), equalTo("stopped"));
            assertThat("the listener completes once", harness.listener.completions.get(), equalTo(1));
            assertFalse("no further poll is left behind", clock.hasDeferredTasks());
            assertThat(harness.client.searches.size(), equalTo(0));
            assertThat(harness.client.bulks, equalTo(List.of()));
            assertThat("the task holds nothing of its own", harness.chargedBytes(), equalTo(baseline + limit));
        }
        assertThat("the budget is back where it started", pressure.stats().getCurrentCoordinatingBytes(), equalTo(baseline));
        assertThat(pressure.stats().getCurrentCombinedCoordinatingAndPrimaryBytes(), equalTo(baseline));
    }

    /**
     * max_docs stops the task. A page larger than the allowance is one truncated bulk. The hits that bulk does not
     * take are still held when it is sent, and those hits and the coordinating charge drop when the task finishes.
     */
    public void testMaxDocsDropsTheChargeAndLeftoverHitsWhenTheTaskFinishes() {
        int maxDocs = between(1, 3);
        int pageSize = maxDocs + between(1, 3);
        UpdateByQueryRequest request = updateRequest(pageSize);
        request.setMaxDocs(maxDocs);
        Harness harness = new Harness(pressure(Settings.EMPTY), request, NoopCircuitBreaker.INSTANCE, null);
        List<CountingHit> hits = new ArrayList<>();
        long sourceBytes = 0;
        for (int i = 0; i < pageSize; i++) {
            int length = between(64, 256);
            sourceBytes += length;
            hits.add(harness.hit(bytes(length)));
        }
        final long pageSourceBytes = sourceBytes;
        harness.client.handler = (bulk, listener) -> {
            harness.client.bulks.add(bulk);
            long contextBytes = harness.queue.getThreadPool().getThreadContext().estimatedRequestContextBytes();
            assertThat(harness.client.bulks.size(), equalTo(1));
            assertThat(bulk.numberOfActions(), equalTo(maxDocs));
            assertTrue(bulk.indexingPressureAccounted());
            // The leftover hits are still in memory, so the charge covers every hit of the page and not only the truncated bulk.
            assertThat(harness.chargedBytes(), greaterThanOrEqualTo(bulk.ramBytesUsed() + contextBytes));
            assertThat(harness.chargedBytes(), greaterThanOrEqualTo(pageSourceBytes + contextBytes));
            for (int i = maxDocs; i < hits.size(); i++) {
                assertThat(hits.get(i).releases.get(), equalTo(0));
            }
            listener.onResponse(harness.success(bulk));
            assertThat(harness.client.bulks.size(), equalTo(1));
            assertThat(harness.chargedBytes(), equalTo(0L));
            for (CountingHit hit : hits) {
                assertThat(hit.releases.get(), equalTo(1));
            }
        };
        harness.start();
        harness.feed(new ArrayList<Hit>(hits));

        BulkByPaginatedSearchResponse response = harness.listener.actionGet(TimeValue.ZERO);
        assertThat(response.getUpdated(), equalTo((long) maxDocs));
        assertThat(harness.client.bulks.size(), equalTo(1));
        assertThat(harness.chargedBytes(), equalTo(0L));
    }

    /**
     * A bulk that does not use up max_docs leaves the rest of the page for another bulk. The page is in memory until that second
     * bulk finishes, so the charge stays at the size of the hits the page holds. It is sized once for the page, and the second bulk
     * neither resizes it nor records another estimate error.
     * <p>
     * Here the estimate is above the page, so the charge shrinks to the whole page when the page arrives. The first bulk may then be
     * bigger than the page, and the charge grows to it, so each move is on its own counter.
     */
    public void testChargeStaysAtThePageSizeAcrossTwoSubBulksWhenTheEstimateIsAboveThePageAndTheConflictIsFirst() {
        assertChargeStaysAtThePageSizeAcrossTwoSubBulks(false, true);
    }

    /**
     * As {@link #testChargeStaysAtThePageSizeAcrossTwoSubBulksWhenTheEstimateIsAboveThePageAndTheConflictIsFirst}, with the version
     * conflict on the second hit of the first bulk.
     */
    public void testChargeStaysAtThePageSizeAcrossTwoSubBulksWhenTheEstimateIsAboveThePageAndTheConflictIsSecond() {
        assertChargeStaysAtThePageSizeAcrossTwoSubBulks(false, false);
    }

    /**
     * Here the estimate is below the page, so the page does not shrink when it arrives, and the charge grows once to the whole page.
     * Nothing is counted as a low estimate.
     */
    public void testChargeStaysAtThePageSizeAcrossTwoSubBulksWhenTheEstimateIsBelowThePageAndTheConflictIsFirst() {
        assertChargeStaysAtThePageSizeAcrossTwoSubBulks(true, true);
    }

    /**
     * As {@link #testChargeStaysAtThePageSizeAcrossTwoSubBulksWhenTheEstimateIsBelowThePageAndTheConflictIsFirst}, with the version
     * conflict on the second hit of the first bulk.
     */
    public void testChargeStaysAtThePageSizeAcrossTwoSubBulksWhenTheEstimateIsBelowThePageAndTheConflictIsSecond() {
        assertChargeStaysAtThePageSizeAcrossTwoSubBulks(true, false);
    }

    private void assertChargeStaysAtThePageSizeAcrossTwoSubBulks(boolean pageLargerThanEstimate, boolean conflictFirst) {
        int maxDocs = 2;
        UpdateByQueryRequest request = updateRequest(3);
        request.setMaxDocs(maxDocs);
        request.setAbortOnVersionConflict(false);
        ScriptedClock clock = new ScriptedClock();
        IndexingPressure pressure = pressure(Settings.EMPTY);
        if (pageLargerThanEstimate) {
            // One byte a document makes the guess far below the real page, so the page resizes upwards.
            pressure.setReindexBytesEstimatePerDocument(ByteSizeValue.ofBytes(1));
        }
        Harness harness = new Harness(clock, pressure, request, NoopCircuitBreaker.INSTANCE, null);
        int updatedLength = between(64, 256);
        int conflictedLength = between(64, 256);
        int leftLength = between(64, 256);
        CountingHit updated = harness.hit(bytes(updatedLength));
        CountingHit conflicted = harness.hit(bytes(conflictedLength));
        CountingHit left = harness.hit(bytes(leftLength));
        long pageSourceBytes = (long) updatedLength + conflictedLength + leftLength;
        List<CountingHit> hits = conflictFirst ? List.of(conflicted, updated, left) : List.of(updated, conflicted, left);
        int conflictAt = conflictFirst ? 0 : 1;
        long context = clock.getThreadPool().getThreadContext().estimatedRequestContextBytes();
        AtomicLong reservedBeforeSearch = new AtomicLong();
        List<Long> chargePerBulk = new ArrayList<>();
        List<Long> belowPerBulk = new ArrayList<>();
        List<Long> abovePerBulk = new ArrayList<>();
        harness.client.handler = (bulk, listener) -> {
            harness.client.bulks.add(bulk);
            assertTrue(bulk.indexingPressureAccounted());
            chargePerBulk.add(harness.chargedBytes());
            belowPerBulk.add(pressure.stats().getReindexPageBytesBelowEstimate());
            abovePerBulk.add(pressure.stats().getReindexPageBytesAboveEstimate());
            assertThat(harness.chargedBytes(), greaterThanOrEqualTo(pageSourceBytes + context));
            if (harness.client.bulks.size() == 1) {
                assertThat("the page still holds the hit the first bulk left out", left.releases.get(), equalTo(0));
                assertThat(bulk.numberOfActions(), equalTo(maxDocs));
                listener.onResponse(withConflict(bulk, conflictAt));
            } else {
                assertThat(bulk.numberOfActions(), equalTo(1));
                listener.onResponse(harness.success(bulk));
            }
        };
        // The scroll stays open so a bulk that has not used up max_docs can come back for the hit it left behind.
        harness.action.setScroll("more");
        harness.start();
        reservedBeforeSearch.set(harness.chargedBytes());
        harness.feed(new ArrayList<Hit>(hits));

        assertThat(harness.client.bulks.size(), equalTo(2));
        BulkByPaginatedSearchResponse response = harness.listener.actionGet(TimeValue.ZERO);
        assertThat(response.getUpdated(), equalTo(2L));
        assertThat(response.getVersionConflicts(), equalTo(1L));
        assertThat("the second sub bulk must not resize the charge", chargePerBulk.get(1), equalTo(chargePerBulk.get(0)));
        assertThat("the second sub bulk must not record an estimate error", belowPerBulk.get(1), equalTo(belowPerBulk.get(0)));
        assertThat("the second sub bulk must not record an estimate error", abovePerBulk.get(1), equalTo(abovePerBulk.get(0)));
        // The page was sized once, so the counters moved on the first bulk only and stayed there.
        long reserved = reservedBeforeSearch.get();
        long charged = chargePerBulk.get(0);
        long pageBytes = pageSourceBytes + context;
        if (pageLargerThanEstimate) {
            assertThat(charged, greaterThan(reserved));
            assertThat(pressure.stats().getReindexPageBytesAboveEstimate(), equalTo(charged - reserved));
            assertThat(pressure.stats().getReindexPageBytesBelowEstimate(), equalTo(0L));
        } else {
            assertThat(pageBytes, lessThan(reserved));
            assertThat(
                "shrunk from the estimate to the page",
                pressure.stats().getReindexPageBytesBelowEstimate(),
                equalTo(reserved - pageBytes)
            );
            assertThat(
                "grown from the page to the bulk, if the bulk is bigger",
                pressure.stats().getReindexPageBytesAboveEstimate(),
                equalTo(charged - pageBytes)
            );
        }
        assertThat(pressure.stats().getReindexPageExceedsLimit(), equalTo(0L));
        assertThat(harness.chargedBytes(), equalTo(0L));
        assertThat(updated.releases.get(), equalTo(1));
        assertThat(conflicted.releases.get(), equalTo(1));
        assertThat(left.releases.get(), equalTo(1));
    }

    /**
     * A single max_docs truncated bulk also sizes the page once. The charge covers the hits the bulk left behind until the task
     * finishes. The page shrinks from the estimate to the whole page when it arrives, so the low estimate error is the difference
     * between the estimate and the whole page. The bulk may then be bigger than the page, in which case the charge grows once more
     * and the high estimate error is that growth.
     */
    public void testTruncatedMaxDocsBulkSizesTheChargeToTheWholePage() {
        int maxDocs = between(1, 3);
        int pageSize = maxDocs + between(1, 3);
        UpdateByQueryRequest request = updateRequest(pageSize);
        request.setMaxDocs(maxDocs);
        ScriptedClock clock = new ScriptedClock();
        IndexingPressure pressure = pressure(Settings.EMPTY);
        Harness harness = new Harness(clock, pressure, request, NoopCircuitBreaker.INSTANCE, null);
        long pageSourceBytes = 0;
        List<CountingHit> hits = new ArrayList<>();
        for (int i = 0; i < pageSize; i++) {
            int length = between(64, 256);
            pageSourceBytes += length;
            hits.add(harness.hit(bytes(length)));
        }
        long context = clock.getThreadPool().getThreadContext().estimatedRequestContextBytes();
        long estimate = rawEstimate(clock.getThreadPool(), pageSize);
        AtomicLong chargeAtBulk = new AtomicLong();
        long expectedFloor = pageSourceBytes + context;
        harness.client.handler = (bulk, listener) -> {
            harness.client.bulks.add(bulk);
            chargeAtBulk.set(harness.chargedBytes());
            listener.onResponse(harness.success(bulk));
        };
        harness.start();
        assertThat(harness.chargedBytes(), equalTo(estimate));
        harness.feed(new ArrayList<Hit>(hits));

        harness.listener.actionGet(TimeValue.ZERO);
        assertThat(chargeAtBulk.get(), greaterThanOrEqualTo(expectedFloor));
        assertThat(
            "shrunk from the estimate to the whole page",
            pressure.stats().getReindexPageBytesBelowEstimate(),
            equalTo(estimate - expectedFloor)
        );
        assertThat(
            "grown from the whole page to the bulk, when the bulk is the bigger of the two",
            pressure.stats().getReindexPageBytesAboveEstimate(),
            equalTo(chargeAtBulk.get() - expectedFloor)
        );
        assertThat(harness.chargedBytes(), equalTo(0L));
    }

    /**
     * A fetched page may be larger than the estimate as long as it is still under the coordinating limit.
     * The charge becomes that real size, and a second task cannot search until its own estimate fits beside it.
     */
    public void testRealPageBetweenTheEstimateAndTheLimitBlocksASibling() {
        int pageSize = 1;
        ScriptedClock clock = new ScriptedClock();
        long estimate = rawEstimate(clock.getThreadPool(), pageSize);
        // The source is past one estimate. The limit leaves room for that source and not for a second estimate beside it.
        int sourceLength = (int) estimate + between(8_192, 16_384);
        long limit = sourceLength + estimate / 2;
        IndexingPressure pressure = pressure(limit);
        Harness first = new Harness(clock, pressure, updateRequest(pageSize), NoopCircuitBreaker.INSTANCE, null);
        Harness second = new Harness(clock, pressure, updateRequest(pageSize), NoopCircuitBreaker.INSTANCE, null);
        CountingHit hit = first.hit(bytes(sourceLength));
        AtomicReference<ActionListener<BulkResponse>> held = new AtomicReference<>();
        AtomicLong realCharge = new AtomicLong();
        first.client.handler = (bulk, listener) -> {
            first.client.bulks.add(bulk);
            long contextBytes = first.queue.getThreadPool().getThreadContext().estimatedRequestContextBytes();
            realCharge.set(first.chargedBytes());
            assertThat(realCharge.get(), greaterThan(estimate));
            assertThat(realCharge.get(), lessThan(limit));
            assertThat(realCharge.get(), equalTo(bulk.ramBytesUsed() + contextBytes));
            assertFalse(first.listener.isDone());
            held.set(listener);
        };
        first.onDone = first::finishWithEmptyPage;
        first.start();
        first.feed(List.of(hit));
        assertThat(pressure.stats().getReindexPageBytesAboveEstimate(), equalTo(realCharge.get() - estimate));
        assertThat(pressure.stats().getReindexPageBytesBelowEstimate(), equalTo(0L));
        assertThat(pressure.stats().getReindexPageExceedsLimit(), equalTo(0L));

        second.start();
        assertThat(pressure.stats().getReindexPageReserveRejections(), equalTo(1L));
        assertThat(second.client.searches.size(), equalTo(0));
        assertFalse(second.listener.isDone());
        assertThat(second.client.bulks, equalTo(List.of()));

        CountingHit followed = second.hit(bytes(between(32, 128)));
        second.client.handler = (bulk, listener) -> listener.onResponse(second.success(bulk));
        second.onDone = second::finishWithEmptyPage;
        held.get().onResponse(first.success(first.client.bulks.get(0)));
        assertThat(first.listener.actionGet(TimeValue.ZERO).getUpdated(), equalTo(1L));
        clock.advanceTime();
        assertThat(second.client.searches.size(), equalTo(1));
        second.feed(List.of(followed));
        assertThat(second.listener.actionGet(TimeValue.ZERO).getUpdated(), equalTo(1L));
        assertThat(pressure.stats().getCurrentCoordinatingBytes(), equalTo(0L));
        assertThat(hit.releases.get(), equalTo(1));
    }

    /**
     * A page that is already in memory and larger than the coordinating limit fails the task. Nothing is sent, the hit is
     * released once, and the budget returns to whatever was held before the search.
     */
    public void testRealPageLargerThanTheLimitFailsTheTask() {
        int pageSize = 1;
        ScriptedClock clock = new ScriptedClock();
        long estimate = rawEstimate(clock.getThreadPool(), pageSize);
        long preexisting = between(128, 1_024);
        long limit = estimate + preexisting + between(1_024, 4_096);
        IndexingPressure pressure = pressure(limit);
        Harness harness = new Harness(clock, pressure, updateRequest(pageSize), NoopCircuitBreaker.INSTANCE, null);
        int sourceLength = (int) (limit + between(1_024, 4_096));
        CountingHit hit = harness.hit(bytes(sourceLength));
        try (Releasable ignored = pressure.markCoordinatingOperationStarted(1, preexisting, false)) {
            harness.start();
            assertThat(harness.client.searches.size(), equalTo(1));
            harness.feed(List.of(hit));

            EsRejectedExecutionException rejected = expectThrows(EsRejectedExecutionException.class, harness.listener::actionGet);
            assertThat(rejected.getMessage(), containsString("max_coordinating_bytes"));
            assertThat(harness.client.bulks, equalTo(List.of()));
            assertThat(hit.releases.get(), equalTo(1));
            assertThat(harness.chargedBytes(), equalTo(preexisting));
            assertThat(pressure.stats().getReindexPageExceedsLimit(), equalTo(1L));
            assertThat(pressure.stats().getReindexPageBytesAboveEstimate(), equalTo(0L));
            assertThat(pressure.stats().getReindexPageBytesBelowEstimate(), equalTo(0L));
            assertThat(pressure.stats().getCoordinatingRejections(), equalTo(1L));
        }
        assertThat(pressure.stats().getCurrentCoordinatingBytes(), equalTo(0L));
    }

    public void testRefusedReserveAfterAFinishedPageHoldsNoHitSources() throws Exception {
        int updatePageSize = between(1, 5);
        assertRefusedReserveHoldsNoHitSources(updateRequest(updatePageSize), updatePageSize, 1L);
        int reindexPageSize = between(1, 5);
        assertRefusedReserveHoldsNoHitSources(reindexRequest(reindexPageSize), reindexPageSize, -1L);
        int deletePageSize = between(1, 5);
        assertRefusedReserveHoldsNoHitSources(deleteRequest(deletePageSize), deletePageSize, 1L);
    }

    /**
     * The worker finishes a page of hits that keep their source after release, as hits from a remote cluster do. Another task then
     * fills the coordinating budget, so the reserve for the next page is refused and the worker waits. The callback that asks for the
     * next page is queued for that wait and the hit source's own response is still held, so the page stays referenced. The wait must
     * hold no page bytes, and neither the hits nor their sources may be reachable, or the page sits in memory uncharged. The search
     * still has to continue once the reserve fits, and the response the hit source holds still shows the cursor the page carried.
     */
    private void assertRefusedReserveHoldsNoHitSources(AbstractBulkByPaginatedSearchRequest<?> request, int pageSize, long version)
        throws Exception {
        ScriptedClock clock = new ScriptedClock();
        IndexingPressure pressure = pressure(rawEstimate(clock.getThreadPool(), pageSize) + between(1, 4_096));
        Harness harness = new Harness(clock, pressure, request, NoopCircuitBreaker.INSTANCE, null);
        AtomicReference<Releasable> otherTask = new AtomicReference<>();
        harness.client.handler = (bulk, listener) -> {
            if (otherTask.get() == null) {
                // Another task takes the whole budget just before this page lets go of its own charge, so the next estimate cannot fit.
                otherTask.set(pressure.markCoordinatingOperationStarted(1, pressure.getCoordinatingLimitInBytes(), true));
            }
            listener.onResponse(harness.success(bulk));
        };
        harness.onDone = () -> {
            harness.action.setScroll(null);
            harness.finishWithEmptyPage();
        };
        CountingHit following = harness.hit(bytes(between(32, 256)), version);
        harness.action.setScroll("more");
        harness.start();
        assertThat(harness.client.searches.size(), equalTo(1));

        int hitCount = between(1, pageSize);
        Cursor cursor = Cursor.random();
        AtomicInteger releases = new AtomicInteger();
        AtomicReference<HeldAsyncResponse> held = new AtomicReference<>();
        List<WeakReference<Object>> weak = feedRemoteStylePage(harness, hitCount, version, cursor, releases, held, () -> {
            harness.feed(List.of(following));
        });

        assertNotNull("the page should have been sent as a bulk", otherTask.get());
        assertThat("every hit of the finished page is released", releases.get(), equalTo(hitCount));
        assertThat("the search must not continue while the reserve is refused", held.get().doneCalls.get(), equalTo(0));
        assertTrue("the refused reserve has to be tried again", harness.queue.hasDeferredTasks());
        assertFalse(harness.listener.isDone());
        assertThat("only the other task's bytes are held", harness.chargedBytes(), equalTo(pressure.getCoordinatingLimitInBytes()));
        assertUnreachable("a hit or source of the finished page", weak);

        otherTask.get().close();
        harness.queue.advanceTime();

        assertThat(held.get().doneCalls.get(), equalTo(1));
        assertThat(held.get().cursorReadByDone.scrollId(), equalTo(cursor.scrollId()));
        assertThat(held.get().cursorReadByDone.searchAfter(), equalTo(cursor.searchAfter()));
        assertThat(held.get().cursorReadByDone.pitId(), equalTo(cursor.pitId()));
        BulkByPaginatedSearchResponse response = harness.listener.actionGet(TimeValue.ZERO);
        assertThat(response.getUpdated() + response.getDeleted() + response.getCreated(), equalTo(hitCount + 1L));
        assertThat(following.releases.get(), equalTo(1));
        assertThat(harness.chargedBytes(), equalTo(0L));
    }

    /**
     * Hands the worker a page of released-on-demand remote style hits. The hits and sources are only reachable through the response
     * that {@code held} keeps, so this is a method of its own: nothing but the weak references leaves it.
     */
    private static List<WeakReference<Object>> feedRemoteStylePage(
        Harness harness,
        int hitCount,
        long version,
        Cursor cursor,
        AtomicInteger releases,
        AtomicReference<HeldAsyncResponse> held,
        Runnable onDone
    ) {
        List<WeakReference<Object>> weak = new ArrayList<>();
        List<Hit> hits = new ArrayList<>();
        String index = harness.action.mainRequest.getSearchRequest().indices()[0];
        for (int i = 0; i < hitCount; i++) {
            BytesReference source = new BytesArray("{\"f\":\"" + randomAlphaOfLength(between(1, 200)) + "\"}");
            Hit hit = new RemoteStyleHit(
                index,
                "id-" + i + "-" + randomAlphaOfLength(4),
                version,
                source,
                new Object[] { randomLong() },
                releases
            );
            weak.add(new WeakReference<>(hit));
            weak.add(new WeakReference<>(source));
            hits.add(hit);
        }
        PaginatedHitSource.Response response = new PaginatedHitSource.Response(
            false,
            List.of(),
            hitCount,
            hits,
            cursor.scrollId(),
            cursor.searchAfter(),
            cursor.pitId()
        );
        held.set(new HeldAsyncResponse(response, onDone));
        harness.action.onPaginatedSearchResponse(held.get());
        return weak;
    }

    public void testBuildBulkCircuitBreakerReturnsToTheBudgetHeldBeforeTheSearch() {
        IndexingPressure pressure = pressure(Settings.EMPTY);
        AtomicLong breakerNet = new AtomicLong();
        long preexisting = between(64, 512);
        try (Releasable ignored = pressure.markCoordinatingOperationStarted(1, preexisting, true)) {
            Harness harness = new Harness(pressure, updateRequest(1), trippingBreaker(breakerNet), null);
            CountingHit hit = harness.hit(bytes(between(100, 1_000)));
            harness.start();
            assertThat(harness.chargedBytes(), greaterThan(preexisting));
            harness.feed(List.of(hit));

            CircuitBreakingException tripped = expectThrows(CircuitBreakingException.class, harness.listener::actionGet);
            assertThat(tripped.getMessage(), equalTo("simulated breaker trip"));
            assertThat(harness.client.bulks, equalTo(List.of()));
            assertFalse(harness.queue.hasDeferredTasks());
            assertThat(hit.releases.get(), equalTo(1));
            assertThat(breakerNet.get(), equalTo(0L));
            assertThat(pressure.stats().getCurrentCoordinatingBytes(), equalTo(preexisting));
        }
        assertThat(pressure.stats().getCurrentCoordinatingBytes(), equalTo(0L));
    }

    public void testNullIndexingPressureSendsAnUnaccountedBulk() {
        Harness harness = new Harness(null, updateRequest(1), NoopCircuitBreaker.INSTANCE, null);
        CountingHit hit = harness.hit(bytes(between(16, 128)));
        harness.client.handler = (bulk, listener) -> {
            assertFalse(bulk.indexingPressureAccounted());
            listener.onResponse(harness.success(bulk));
        };
        harness.onDone = harness::finishWithEmptyPage;
        harness.start();
        assertThat(harness.client.searches.size(), equalTo(1));
        harness.feed(List.of(hit));
        BulkByPaginatedSearchResponse response = harness.listener.actionGet(TimeValue.ZERO);
        assertThat(response.getUpdated(), equalTo(1L));
        assertThat(hit.releases.get(), equalTo(1));
    }

    /**
     * A 32kb guess reserves twice the per-document bytes of the 16kb default for the same page. A page whose guess is
     * above half the coordinating limit still reserves only that half. The second reserve reads the updated setting.
     */
    public void testLargerBytesEstimateReservesTwiceAsMuchUntilHalfTheLimit() {
        int pageSize = 2;
        ScriptedClock clock = new ScriptedClock();
        long context = clock.getThreadPool().getThreadContext().estimatedRequestContextBytes();
        long perDocument = ESTIMATED_HIT_BYTES;
        long doubled = perDocument * 2;
        // Half of this is above a doubled page and below two doubled pages, so the first two reserves are not capped and the third is.
        long roomy = 2 * (doubled * pageSize + context) + 4_096;
        Settings nodeSettings = Settings.builder().put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), roomy + "b").build();
        ClusterSettings clusterSettings = new ClusterSettings(nodeSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        IndexingPressure pressure = new IndexingPressure(nodeSettings);
        clusterSettings.initializeAndWatch(
            IndexingPressure.REINDEX_BYTES_ESTIMATE_PER_DOCUMENT,
            pressure::setReindexBytesEstimatePerDocument
        );

        long atDefault = reservedPage(clock, pressure, pageSize);
        assertThat(atDefault, equalTo(pageSize * perDocument + context));

        clusterSettings.applySettings(
            Settings.builder().put(IndexingPressure.REINDEX_BYTES_ESTIMATE_PER_DOCUMENT.getKey(), doubled + "b").build()
        );
        long atDouble = reservedPage(clock, pressure, pageSize);
        assertThat(atDouble, equalTo(pageSize * doubled + context));
        assertThat(atDouble - context, equalTo(2 * (atDefault - context)));
        assertThat(atDouble, lessThan(halfTheLimit(pressure)));

        int cappedPage = pageSize * 2;
        long capped = reservedPage(clock, pressure, cappedPage);
        assertThat((long) cappedPage * doubled + context, greaterThan(halfTheLimit(pressure)));
        assertThat(capped, equalTo(halfTheLimit(pressure)));
    }

    /**
     * A bulk that only targets system indices was never refused at the coordinating stage, so a page of them that turns out larger than
     * the coordinating limit still completes. The charge grows past the limit to the real page, and nothing counts as over the limit.
     */
    public void testSystemOnlyReindexPageLargerThanTheLimitCompletesAndTheChargeGrowsPastTheLimit() {
        assertSystemOnlyPageLargerThanTheLimitCompletes(true);
    }

    /**
     * As {@link #testSystemOnlyReindexPageLargerThanTheLimitCompletesAndTheChargeGrowsPastTheLimit}, for an update by query.
     */
    public void testSystemOnlyUpdateByQueryPageLargerThanTheLimitCompletesAndTheChargeGrowsPastTheLimit() {
        assertSystemOnlyPageLargerThanTheLimitCompletes(false);
    }

    private void assertSystemOnlyPageLargerThanTheLimitCompletes(boolean reindex) {
        String systemIndex = ".sys-" + randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        int pageSize = 1;
        ScriptedClock clock = new ScriptedClock();
        long limit = rawEstimate(clock.getThreadPool(), pageSize) + between(1, 4_096);
        IndexingPressure pressure = pressure(limit);
        AbstractBulkByPaginatedSearchRequest<?> request = reindex
            ? reindexRequest(pageSize, randomAlphaOfLength(6), systemIndex)
            : updateRequest(pageSize, systemIndex);
        Harness harness = new Harness(
            clock,
            pressure,
            request,
            NoopCircuitBreaker.INSTANCE,
            null,
            systemIndicesMatching(".sys*"),
            lookupOf(true, systemIndex)
        );
        CountingHit hit = harness.hit(bytes((int) limit + between(1_024, 4_096)), reindex ? -1L : 1L);
        AtomicLong chargeAtBulk = new AtomicLong();
        harness.client.handler = (bulk, listener) -> {
            harness.client.bulks.add(bulk);
            chargeAtBulk.set(harness.chargedBytes());
            assertTrue(bulk.indexingPressureAccounted());
            listener.onResponse(harness.success(bulk, reindex));
        };
        harness.onDone = harness::finishWithEmptyPage;
        harness.start();
        harness.feed(List.of(hit));

        BulkByPaginatedSearchResponse response = harness.listener.actionGet(TimeValue.ZERO);
        assertThat(response.getUpdated() + response.getCreated(), equalTo(1L));
        assertThat(harness.client.bulks.size(), equalTo(1));
        assertThat(chargeAtBulk.get(), greaterThan(pressure.getCoordinatingLimitInBytes()));
        assertThat(pressure.stats().getReindexPageExceedsLimit(), equalTo(0L));
        assertThat(pressure.stats().getCoordinatingRejections(), equalTo(0L));
        assertThat(pressure.stats().getReindexPageBytesAboveEstimate(), greaterThan(0L));
        assertThat(hit.releases.get(), equalTo(1));
        assertThat(harness.chargedBytes(), equalTo(0L));
    }

    /**
     * An ordinary index is not a system index, so a page of it that is larger than the limit fails the task even when the worker was
     * given a system index list and an indices lookup that agree on that.
     */
    public void testNonSystemPageLargerThanTheLimitFailsWhenSystemIndicesAreKnown() {
        String index = "plain-" + randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        assertOversizedPageFails(List.of(index), systemIndicesMatching(".sys*"), lookupOf(false, index));
    }

    /**
     * A null system index list means no bulk is system-only, so a page larger than the limit fails even for an index whose name and
     * whose lookup entry say it is a system index.
     */
    public void testNullSystemIndicesMeansTheOversizedPageIsNotSystemOnly() {
        String index = ".sys-" + randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        assertOversizedPageFails(List.of(index), null, lookupOf(true, index));
    }

    /**
     * A bulk is only system-only when every index it targets is a system index. One ordinary index beside a system index makes it an
     * ordinary bulk, and a page of it larger than the limit fails. The system index is named first.
     */
    public void testPageOverTheLimitFailsWhenTheSystemIndexIsFirstAndTheOtherIsNot() {
        assertPageOverTheLimitFailsWhenOnlySomeOfItsIndicesAreSystemIndices(true);
    }

    /**
     * As {@link #testPageOverTheLimitFailsWhenTheSystemIndexIsFirstAndTheOtherIsNot}, with the ordinary index named first.
     */
    public void testPageOverTheLimitFailsWhenTheOrdinaryIndexIsFirstAndTheOtherIsASystemIndex() {
        assertPageOverTheLimitFailsWhenOnlySomeOfItsIndicesAreSystemIndices(false);
    }

    private void assertPageOverTheLimitFailsWhenOnlySomeOfItsIndicesAreSystemIndices(boolean systemFirst) {
        String system = ".sys-" + randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        String plain = "plain-" + randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        List<String> hitIndices = systemFirst ? List.of(system, plain) : List.of(plain, system);
        SortedMap<String, IndexAbstraction> both = new TreeMap<>();
        both.putAll(lookupOf(true, system).get());
        both.putAll(lookupOf(false, plain).get());
        assertOversizedPageFails(hitIndices, systemIndicesMatching(".sys*"), () -> both);
    }

    /**
     * A reindex that writes only to a system index skips the node total check on its page reserve, as a system-only bulk is never
     * refused there. It searches at once even when the node already holds a whole coordinating limit. The source is an ordinary index,
     * because a reindex is judged by where it writes.
     */
    public void testSystemOnlyReindexIsAdmittedOnALoadedNode() {
        String system = systemIndexName();
        assertSystemOnlyTaskIsAdmittedOnALoadedNode(
            reindexRequest(1, "plain-" + randomAlphaOfLength(5), system),
            -1L,
            lookupOf(true, system)
        );
    }

    /**
     * An update by query over only system indices skips the node total check on its page reserve, so it searches at once even when
     * the node already holds a whole coordinating limit.
     */
    public void testSystemOnlyUpdateByQueryIsAdmittedOnALoadedNode() {
        String system = systemIndexName();
        assertSystemOnlyTaskIsAdmittedOnALoadedNode(updateRequest(1, system), 1L, lookupOf(true, system));
    }

    /**
     * A delete by query over only system indices skips the node total check on its page reserve, so it searches at once even when
     * the node already holds a whole coordinating limit.
     */
    public void testSystemOnlyDeleteByQueryIsAdmittedOnALoadedNode() {
        String system = systemIndexName();
        assertSystemOnlyTaskIsAdmittedOnALoadedNode(deleteRequest(1, system), 1L, lookupOf(true, system));
    }

    /**
     * A wildcard that matches only system indices is system-only, so it is admitted on a loaded node. The page that comes back belongs
     * to one of the matched indices.
     */
    public void testWildcardOverOnlySystemIndicesIsAdmittedOnALoadedNode() {
        String first = ".sys-a-" + randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        String second = ".sys-b-" + randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        SortedMap<String, IndexAbstraction> both = new TreeMap<>();
        both.putAll(lookupOf(true, first).get());
        both.putAll(lookupOf(true, second).get());
        assertSystemOnlyTaskIsAdmittedOnALoadedNode(updateRequest(1, ".sys-*"), 1L, () -> both, first);
    }

    /**
     * A reindex is judged by the index it writes to. One that reads a system index and writes an ordinary one is not system-only, so
     * on a loaded node it waits.
     */
    public void testReindexFromASystemIndexToAnOrdinaryIndexIsNotSystemOnlyAndWaits() {
        String system = systemIndexName();
        String plain = "plain-" + randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        SortedMap<String, IndexAbstraction> both = bothLookups(system, plain);
        assertTaskWaitsOnALoadedNode(reindexRequest(1, system, plain), systemIndicesMatching(".sys*"), () -> both);
    }

    /**
     * An ordinary index beside a system index makes the target mixed, so it is not system-only and an update by query waits on a loaded
     * node.
     */
    public void testUpdateByQueryOverASystemAndAnOrdinaryIndexIsNotSystemOnlyAndWaits() {
        String system = systemIndexName();
        String plain = "plain-" + randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        SortedMap<String, IndexAbstraction> both = bothLookups(system, plain);
        assertTaskWaitsOnALoadedNode(updateRequest(1, system, plain), systemIndicesMatching(".sys*"), () -> both);
    }

    /**
     * An ordinary index beside a system index makes the target mixed, so it is not system-only and a delete by query waits on a loaded
     * node.
     */
    public void testDeleteByQueryOverASystemAndAnOrdinaryIndexIsNotSystemOnlyAndWaits() {
        String system = systemIndexName();
        String plain = "plain-" + randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        SortedMap<String, IndexAbstraction> both = bothLookups(system, plain);
        assertTaskWaitsOnALoadedNode(deleteRequest(1, plain, system), systemIndicesMatching(".sys*"), () -> both);
    }

    /**
     * A wildcard that matches both a system index and an ordinary index is a mixed target, so it is not system-only and waits on a
     * loaded node.
     */
    public void testWildcardOverASystemAndAnOrdinaryIndexIsNotSystemOnlyAndWaits() {
        String system = systemIndexName();
        String plain = "plain-" + randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        SortedMap<String, IndexAbstraction> both = bothLookups(system, plain);
        assertTaskWaitsOnALoadedNode(updateRequest(1, "*"), systemIndicesMatching(".sys*"), () -> both);
    }

    /**
     * A target that resolves to nothing is not system-only, so an update by query over it waits on a loaded node. Nothing is known about
     * what such a target would write, so it is treated as an ordinary one.
     */
    public void testUpdateByQueryOverAWildcardThatMatchesNothingIsNotSystemOnlyAndWaits() {
        String system = systemIndexName();
        assertTaskWaitsOnALoadedNode(updateRequest(1, "nomatch-*"), systemIndicesMatching(".sys*"), lookupOf(true, system));
    }

    /**
     * A target that resolves to nothing is not system-only, so a delete by query over it waits on a loaded node.
     */
    public void testDeleteByQueryOverAWildcardThatMatchesNothingIsNotSystemOnlyAndWaits() {
        String system = systemIndexName();
        assertTaskWaitsOnALoadedNode(deleteRequest(1, "nomatch-*"), systemIndicesMatching(".sys*"), lookupOf(true, system));
    }

    /**
     * A page that is waiting out a requests_per_second throttle has already shrunk from its estimate to the size of its hits, so it does
     * not keep an estimate's worth of the budget idle. A small client bulk that fits beside those hits, and would not fit beside the
     * estimate, is admitted while the throttle waits.
     */
    public void testSmallClientBulkIsAdmittedWhileAThrottleWaitHoldsAPageThatHasShrunkToItsHits() {
        ScriptedClock clock = new ScriptedClock();
        long limit = between(64 * 1024, 1024 * 1024);
        IndexingPressure pressure = pressureWhereEveryEstimateHitsTheLimit(limit);
        UpdateByQueryRequest request = updateRequest(1);
        // One document at one request a second makes the wait before the second page about a second, which the clock holds.
        request.setRequestsPerSecond(1f);
        Harness harness = new Harness(clock, pressure, request, NoopCircuitBreaker.INSTANCE, null);
        long context = clock.getThreadPool().getThreadContext().estimatedRequestContextBytes();
        int secondLength = between(32, 256);
        CountingHit first = harness.hit(bytes(between(32, 256)));
        CountingHit second = harness.hit(bytes(secondLength));
        harness.client.handler = (bulk, listener) -> {
            harness.client.bulks.add(bulk);
            listener.onResponse(harness.success(bulk));
        };
        AtomicInteger pages = new AtomicInteger();
        harness.onDone = () -> {
            if (pages.getAndIncrement() == 0) {
                harness.feed(List.of(second));
            } else {
                harness.action.setScroll(null);
                harness.finishWithEmptyPage();
            }
        };
        harness.action.setScroll("more");
        harness.start();
        assertThat("the first page reserves the capped estimate", harness.chargedBytes(), equalTo(halfTheLimit(pressure)));
        // The first page is not throttled. Its bulk completes, and the second page then arrives and has to wait out the throttle.
        harness.feed(List.of(first));

        assertTrue("the second page is waiting out the throttle", clock.hasDeferredTasks());
        assertThat(clock.nextDelay().millis(), greaterThan(0L));
        assertFalse(harness.listener.isDone());
        assertThat(harness.client.bulks.size(), equalTo(1));
        long pageBytes = secondLength + context;
        assertThat("the waiting page holds its hits and not the estimate", harness.chargedBytes(), equalTo(pageBytes));
        assertThat(pressure.stats().getReindexPageBytesBelowEstimate(), greaterThan(0L));

        // Fills the node exactly, and is more than would be left beside an estimate of half the limit.
        long clientBytes = limit - pageBytes - between(0, 64);
        assertThat(clientBytes, greaterThan(limit - halfTheLimit(pressure)));
        try (Releasable client = pressure.markCoordinatingOperationStarted(1, clientBytes, false)) {
            assertThat(pressure.stats().getCurrentCoordinatingBytes(), greaterThanOrEqualTo(clientBytes + pageBytes));
            assertThat(pressure.stats().getCoordinatingRejections(), equalTo(0L));
        }

        runHeldAttemptsUntilDone(harness, clock);
        BulkByPaginatedSearchResponse response = harness.listener.actionGet(TimeValue.ZERO);
        assertThat(response.getUpdated(), equalTo(2L));
        assertThat(harness.client.bulks.size(), equalTo(2));
        assertThat(first.releases.get(), equalTo(1));
        assertThat(second.releases.get(), equalTo(1));
        assertThat(pressure.stats().getCoordinatingRejections(), equalTo(0L));
        assertThat(harness.chargedBytes(), equalTo(0L));
    }

    private static String systemIndexName() {
        return ".sys-" + randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
    }

    private static SortedMap<String, IndexAbstraction> bothLookups(String system, String plain) {
        SortedMap<String, IndexAbstraction> both = new TreeMap<>();
        both.putAll(lookupOf(true, system).get());
        both.putAll(lookupOf(false, plain).get());
        return both;
    }

    private void assertSystemOnlyTaskIsAdmittedOnALoadedNode(
        AbstractBulkByPaginatedSearchRequest<?> request,
        long hitVersion,
        Supplier<SortedMap<String, IndexAbstraction>> lookup
    ) {
        assertSystemOnlyTaskIsAdmittedOnALoadedNode(request, hitVersion, lookup, lookup.get().firstKey());
    }

    /**
     * The node already holds a whole coordinating limit, so a reserve that was checked against the node total would be refused.
     * A system-only task is searching at once, and its page completes.
     *
     * @param hitIndex the index the hit that comes back belongs to
     */
    private void assertSystemOnlyTaskIsAdmittedOnALoadedNode(
        AbstractBulkByPaginatedSearchRequest<?> request,
        long hitVersion,
        Supplier<SortedMap<String, IndexAbstraction>> lookup,
        String hitIndex
    ) {
        ScriptedClock clock = new ScriptedClock();
        IndexingPressure pressure = pressureWhereEveryEstimateHitsTheLimit(between(64 * 1024, 1024 * 1024));
        Harness harness = new Harness(clock, pressure, request, NoopCircuitBreaker.INSTANCE, null, systemIndicesMatching(".sys*"), lookup);
        CountingHit hit = harness.hitIn(hitIndex, bytes(between(32, 256)), hitVersion);
        harness.client.handler = (bulk, listener) -> listener.onResponse(harness.success(bulk, hitVersion == -1L));
        harness.onDone = harness::finishWithEmptyPage;
        long limit = pressure.getCoordinatingLimitInBytes();
        try (Releasable ignored = pressure.markCoordinatingOperationStarted(1, limit, false)) {
            harness.start();

            assertThat("a system-only task is not made to wait for what the node holds", harness.client.searches.size(), equalTo(1));
            assertFalse(clock.hasDeferredTasks());
            assertThat(harness.chargedBytes(), equalTo(limit + halfTheLimit(pressure)));
            assertThat(pressure.stats().getReindexPageReserveRejections(), equalTo(0L));
            assertThat(pressure.stats().getCoordinatingRejections(), equalTo(0L));

            harness.feed(List.of(hit));
            BulkByPaginatedSearchResponse response = harness.listener.actionGet(TimeValue.ZERO);
            assertThat(response.getUpdated() + response.getCreated() + response.getDeleted(), equalTo(1L));
            assertThat(hit.releases.get(), equalTo(1));
            assertThat(pressure.stats().getReindexPageExceedsLimit(), equalTo(0L));
            assertThat(pressure.stats().getReindexPageReserveRejections(), equalTo(0L));
            assertThat("only the other holder's bytes", harness.chargedBytes(), equalTo(limit));
        }
        assertThat(pressure.stats().getCurrentCoordinatingBytes(), equalTo(0L));
    }

    /**
     * The node already holds a whole coordinating limit, and the target is not system-only, so the page reserve is checked against the
     * node total and is refused. The task keeps running with no search sent, and it searches, with half the limit reserved, once the
     * node lets go.
     */
    private void assertTaskWaitsOnALoadedNode(
        AbstractBulkByPaginatedSearchRequest<?> request,
        @Nullable SystemIndices systemIndices,
        Supplier<SortedMap<String, IndexAbstraction>> lookup
    ) {
        ScriptedClock clock = new ScriptedClock();
        IndexingPressure pressure = pressureWhereEveryEstimateHitsTheLimit(between(64 * 1024, 1024 * 1024));
        Harness harness = new Harness(clock, pressure, request, NoopCircuitBreaker.INSTANCE, null, systemIndices, lookup);
        long limit = pressure.getCoordinatingLimitInBytes();
        Releasable other = pressure.markCoordinatingOperationStarted(1, limit, false);
        try {
            harness.start();

            assertThat("a task that is not system-only waits for what the node holds", harness.client.searches.size(), equalTo(0));
            assertTrue("the refused reserve has to be tried again", clock.hasDeferredTasks());
            assertFalse(harness.listener.isDone());
            assertThat("only the other holder's bytes", harness.chargedBytes(), equalTo(limit));
            assertThat(pressure.stats().getReindexPageReserveRejections(), equalTo(1L));
            assertThat(pressure.stats().getCoordinatingRejections(), equalTo(0L));
        } finally {
            other.close();
        }

        clock.advanceTime();
        assertThat(harness.client.searches.size(), equalTo(1));
        assertThat(harness.chargedBytes(), equalTo(halfTheLimit(pressure)));
        harness.finishWithEmptyPage();
        harness.listener.actionGet(TimeValue.ZERO);
        assertThat(pressure.stats().getCurrentCoordinatingBytes(), equalTo(0L));
    }

    private void assertOversizedPageFails(
        List<String> hitIndices,
        @Nullable SystemIndices systemIndices,
        @Nullable Supplier<SortedMap<String, IndexAbstraction>> lookup
    ) {
        int pageSize = hitIndices.size();
        ScriptedClock clock = new ScriptedClock();
        long limit = rawEstimate(clock.getThreadPool(), pageSize) + between(1, 4_096);
        IndexingPressure pressure = pressure(limit);
        Harness harness = new Harness(
            clock,
            pressure,
            updateRequest(pageSize, hitIndices.toArray(String[]::new)),
            NoopCircuitBreaker.INSTANCE,
            null,
            systemIndices,
            lookup
        );
        List<CountingHit> hits = new ArrayList<>();
        for (String index : hitIndices) {
            hits.add(harness.hitIn(index, bytes((int) limit + between(1_024, 4_096)), 1L));
        }
        harness.start();
        harness.feed(new ArrayList<Hit>(hits));

        EsRejectedExecutionException rejected = expectThrows(EsRejectedExecutionException.class, harness.listener::actionGet);
        assertThat(rejected.getMessage(), containsString("max_coordinating_bytes"));
        assertThat(harness.client.bulks, equalTo(List.of()));
        for (CountingHit hit : hits) {
            assertThat(hit.releases.get(), equalTo(1));
        }
        assertThat(pressure.stats().getReindexPageExceedsLimit(), equalTo(1L));
        assertThat(pressure.stats().getReindexPageBytesAboveEstimate(), equalTo(0L));
        assertThat(pressure.stats().getReindexPageBytesBelowEstimate(), equalTo(0L));
        assertThat(harness.chargedBytes(), equalTo(0L));
    }

    /**
     * An estimate that was cut down to half the coordinating limit is admitted when the node holds coordinating bytes that leave room
     * for that half. The worker then fetches the page.
     */
    public void testCappedEstimateIsAdmittedBesideCoordinatingBytesThatLeaveRoomForHalfTheLimit() {
        assertCappedEstimateIsAdmittedBesideOtherBytes(false, limit -> between(1, (int) roomBesideAHalfReserve(limit)));
    }

    /**
     * An estimate that was cut down to half the coordinating limit is admitted when the node holds replica bytes that leave room for
     * that half, because the node total counts replica bytes too.
     */
    public void testCappedEstimateIsAdmittedBesideReplicaBytesThatLeaveRoomForHalfTheLimit() {
        assertCappedEstimateIsAdmittedBesideOtherBytes(true, limit -> between(1, (int) roomBesideAHalfReserve(limit)));
    }

    /**
     * The node total may equal the limit. A node that holds exactly what leaves room for half the limit still admits the estimate.
     */
    public void testCappedEstimateIsAdmittedWhenTheNodeHoldsExactlyTheRoomLeftForHalfTheLimit() {
        assertCappedEstimateIsAdmittedBesideOtherBytes(false, limit -> roomBesideAHalfReserve(limit));
    }

    /**
     * A node holding more than half the limit in coordinating bytes cannot take a capped estimate, so the worker waits and sends no
     * search. It searches once those bytes are released.
     */
    public void testCappedEstimateWaitsBesideCoordinatingBytesAboveHalfTheLimit() {
        assertCappedEstimateWaitsBesideOtherBytes(false, limit -> between((int) roomBesideAHalfReserve(limit) + 1, (int) limit));
    }

    /**
     * A node holding more than half the limit in replica bytes cannot take a capped estimate, so the worker waits and sends no search.
     * It searches once those bytes are released.
     */
    public void testCappedEstimateWaitsBesideReplicaBytesAboveHalfTheLimit() {
        assertCappedEstimateWaitsBesideOtherBytes(true, limit -> between((int) roomBesideAHalfReserve(limit) + 1, (int) limit));
    }

    /**
     * One byte more than the room left for half the limit is enough to make a capped estimate wait.
     */
    public void testCappedEstimateWaitsWhenTheNodeHoldsOneByteMoreThanTheRoomLeftForHalfTheLimit() {
        assertCappedEstimateWaitsBesideOtherBytes(false, limit -> roomBesideAHalfReserve(limit) + 1);
    }

    /**
     * What a node may already hold for a reserve of half the limit to fit.
     */
    private static long roomBesideAHalfReserve(long limit) {
        return limit - limit / 2;
    }

    private void assertCappedEstimateIsAdmittedBesideOtherBytes(boolean otherIsReplica, LongUnaryOperator otherBytesForLimit) {
        int pageSize = between(2, 5);
        ScriptedClock clock = new ScriptedClock();
        long estimate = rawEstimate(clock.getThreadPool(), pageSize);
        long limit = between((int) (estimate / 4), (int) estimate - 1);
        IndexingPressure pressure = pressure(limit);
        assertThat("the raw estimate is above half the limit, so it is capped", estimate, greaterThan(halfTheLimit(pressure)));
        Harness harness = new Harness(clock, pressure, updateRequest(pageSize), NoopCircuitBreaker.INSTANCE, null);
        CountingHit hit = harness.hit(bytes(between(32, 256)));
        harness.client.handler = (bulk, listener) -> listener.onResponse(harness.success(bulk));
        harness.onDone = harness::finishWithEmptyPage;
        long otherBytes = otherBytesForLimit.applyAsLong(limit);
        long otherCoordinating = otherIsReplica ? 0L : otherBytes;
        try (Releasable ignored = holdOtherBytes(pressure, otherBytes, otherIsReplica)) {
            harness.start();

            assertThat("a capped estimate that fits is not made to wait", harness.client.searches.size(), equalTo(1));
            assertFalse(harness.queue.hasDeferredTasks());
            assertThat(harness.chargedBytes(), equalTo(halfTheLimit(pressure) + otherCoordinating));
            assertThat(pressure.stats().getReindexPageReserveRejections(), equalTo(0L));
            assertThat(pressure.stats().getCoordinatingRejections(), equalTo(0L));

            harness.feed(List.of(hit));
            assertThat(harness.listener.actionGet(TimeValue.ZERO).getUpdated(), equalTo(1L));
            assertThat(hit.releases.get(), equalTo(1));
            assertThat(pressure.stats().getReindexPageExceedsLimit(), equalTo(0L));
        }
        assertThat(pressure.stats().getCurrentCoordinatingBytes(), equalTo(0L));
        assertThat(pressure.stats().getCurrentReplicaBytes(), equalTo(0L));
    }

    private void assertCappedEstimateWaitsBesideOtherBytes(boolean otherIsReplica, LongUnaryOperator otherBytesForLimit) {
        int pageSize = between(2, 5);
        ScriptedClock clock = new ScriptedClock();
        long estimate = rawEstimate(clock.getThreadPool(), pageSize);
        long limit = between((int) (estimate / 4), (int) estimate - 1);
        IndexingPressure pressure = pressure(limit);
        assertThat("the raw estimate is above half the limit, so it is capped", estimate, greaterThan(halfTheLimit(pressure)));
        Harness harness = new Harness(clock, pressure, updateRequest(pageSize), NoopCircuitBreaker.INSTANCE, null);
        CountingHit hit = harness.hit(bytes(between(32, 256)));
        harness.client.handler = (bulk, listener) -> listener.onResponse(harness.success(bulk));
        harness.onDone = harness::finishWithEmptyPage;
        long otherBytes = otherBytesForLimit.applyAsLong(limit);
        long otherCoordinating = otherIsReplica ? 0L : otherBytes;
        Releasable other = holdOtherBytes(pressure, otherBytes, otherIsReplica);
        try {
            harness.start();

            assertThat("a capped estimate that does not fit has to wait", harness.client.searches.size(), equalTo(0));
            assertTrue("the refused reserve has to be tried again", harness.queue.hasDeferredTasks());
            assertFalse(harness.listener.isDone());
            assertThat("only the other holder's bytes", harness.chargedBytes(), equalTo(otherCoordinating));
            assertThat(pressure.stats().getReindexPageReserveRejections(), equalTo(1L));
            assertThat(pressure.stats().getCoordinatingRejections(), equalTo(0L));
        } finally {
            other.close();
        }

        harness.queue.advanceTime();
        assertThat(harness.client.searches.size(), equalTo(1));
        assertThat(harness.chargedBytes(), equalTo(halfTheLimit(pressure)));
        harness.feed(List.of(hit));
        assertThat(harness.listener.actionGet(TimeValue.ZERO).getUpdated(), equalTo(1L));
        assertThat(hit.releases.get(), equalTo(1));
        assertThat(pressure.stats().getCurrentCoordinatingBytes(), equalTo(0L));
        assertThat(pressure.stats().getCurrentReplicaBytes(), equalTo(0L));
    }

    /**
     * Several workers on one node each want their first page, and every raw estimate is far above the coordinating limit, so each one
     * reserves half the limit. Two of them fill the budget on an empty node, so the third is not admitted. The node may not hold more
     * coordinating bytes than the limit, and the one that does not fit keeps waiting.
     */
    public void testWorkersWithLimitSizedEstimatesDoNotTogetherHoldMoreThanTheCoordinatingLimit() {
        int pageSize = between(1, 3);
        int workers = 3;
        long limit = between(64 * 1024, 1024 * 1024);
        IndexingPressure pressure = pressureWhereEveryEstimateHitsTheLimit(limit);
        ScriptedClock clock = new ScriptedClock();
        List<Harness> harnesses = new ArrayList<>();
        for (int i = 0; i < workers; i++) {
            harnesses.add(new Harness(clock, pressure, updateRequest(pageSize), NoopCircuitBreaker.INSTANCE, null));
        }

        for (Harness harness : harnesses) {
            harness.start();
        }

        long searching = harnesses.stream().filter(harness -> harness.client.searches.isEmpty() == false).count();
        long charged = pressure.stats().getCurrentCoordinatingBytes();
        assertThat(
            format(
                "[%d] of [%d] workers were admitted, and the node holds [%d] coordinating bytes against a limit of [%d]",
                searching,
                workers,
                charged,
                pressure.getCoordinatingLimitInBytes()
            ),
            charged,
            lessThanOrEqualTo(pressure.getCoordinatingLimitInBytes())
        );
        assertThat(
            format("two of the [%d] workers fit together, and the third has to wait for one of them", workers),
            searching,
            equalTo(2L)
        );
        assertThat(charged, equalTo(2 * halfTheLimit(pressure)));
        assertTrue("a worker that does not fit has to be tried again", clock.hasDeferredTasks());
    }

    /**
     * Replica writes already hold a good part of the node's budget. A worker whose estimate is capped at half the coordinating limit
     * has to wait for them when they leave less than half the limit free, because admitting it would put the node over the limit.
     * It searches once they are gone.
     */
    public void testWorkerWithALimitSizedEstimateWaitsForReplicaBytesAndThenSearches() {
        int pageSize = between(1, 3);
        long limit = between(64 * 1024, 1024 * 1024);
        IndexingPressure pressure = pressureWhereEveryEstimateHitsTheLimit(limit);
        ScriptedClock clock = new ScriptedClock();
        Harness harness = new Harness(clock, pressure, updateRequest(pageSize), NoopCircuitBreaker.INSTANCE, null);
        long replicaBytes = limit * between(55, 65) / 100;
        Releasable replica = holdOtherBytes(pressure, replicaBytes, true);
        try {
            harness.start();

            assertThat(
                format(
                    "the worker searched at once, so the node holds [%d] coordinating and [%d] replica bytes against a limit of [%d]",
                    pressure.stats().getCurrentCoordinatingBytes(),
                    pressure.stats().getCurrentReplicaBytes(),
                    pressure.getCoordinatingLimitInBytes()
                ),
                harness.client.searches.size(),
                equalTo(0)
            );
            assertFalse(harness.listener.isDone());
            assertTrue("the refused reserve has to be tried again", clock.hasDeferredTasks());
        } finally {
            replica.close();
        }

        clock.advanceTime();

        assertThat(harness.client.searches.size(), equalTo(1));
        assertThat(harness.chargedBytes(), equalTo(halfTheLimit(pressure)));
    }

    /**
     * Replica writes hold only a small part of the node's budget, so a worker whose estimate is capped at half the coordinating limit
     * still has to be admitted. Without this a node with a steady trickle of replica writes would never let a reindex search again.
     */
    public void testWorkerWithALimitSizedEstimateIsAdmittedWhenReplicaBytesAreSmall() {
        int pageSize = between(1, 3);
        long limit = between(64 * 1024, 1024 * 1024);
        IndexingPressure pressure = pressureWhereEveryEstimateHitsTheLimit(limit);
        ScriptedClock clock = new ScriptedClock();
        Harness harness = new Harness(clock, pressure, updateRequest(pageSize), NoopCircuitBreaker.INSTANCE, null);
        long replicaBytes = limit * between(15, 25) / 100;
        try (Releasable ignored = holdOtherBytes(pressure, replicaBytes, true)) {
            harness.start();

            assertThat(
                format(
                    "the worker was not admitted with [%d] replica bytes against a limit of [%d], reserve rejections [%d], coordinating [%d]",
                    pressure.stats().getCurrentReplicaBytes(),
                    pressure.getCoordinatingLimitInBytes(),
                    pressure.stats().getReindexPageReserveRejections(),
                    pressure.stats().getCurrentCoordinatingBytes()
                ),
                harness.client.searches.size(),
                equalTo(1)
            );
            assertThat(harness.chargedBytes(), equalTo(halfTheLimit(pressure)));
            assertThat(pressure.stats().getReindexPageReserveRejections(), equalTo(0L));
        }
    }

    /**
     * Every raw page estimate is above the coordinating limit, even for a page of one document, so the estimate is always cut down to
     * half the limit and the worker reserves that. One byte more than the limit per document is enough, because the estimate is the
     * per-document bytes times the page size, plus the request context.
     */
    private static IndexingPressure pressureWhereEveryEstimateHitsTheLimit(long limit) {
        return pressure(
            Settings.builder()
                .put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), limit + "b")
                .put(IndexingPressure.REINDEX_BYTES_ESTIMATE_PER_DOCUMENT.getKey(), (limit + 1) + "b")
                .build()
        );
    }

    /**
     * A capped estimate is admitted without forcing the reservation, beside coordinating bytes that leave room for half the limit. A
     * page that is not a system page and whose own size is above the limit therefore still fails when it arrives, and the charge it
     * held is given back.
     */
    public void testPageLargerThanTheLimitStillFailsAfterACappedEstimateWasAdmittedBesideCoordinatingBytes() {
        assertPageLargerThanTheLimitFailsAfterACappedEstimateWasAdmitted(false);
    }

    /**
     * As {@link #testPageLargerThanTheLimitStillFailsAfterACappedEstimateWasAdmittedBesideCoordinatingBytes}, beside replica bytes.
     */
    public void testPageLargerThanTheLimitStillFailsAfterACappedEstimateWasAdmittedBesideReplicaBytes() {
        assertPageLargerThanTheLimitFailsAfterACappedEstimateWasAdmitted(true);
    }

    private void assertPageLargerThanTheLimitFailsAfterACappedEstimateWasAdmitted(boolean otherIsReplica) {
        int pageSize = between(2, 5);
        ScriptedClock clock = new ScriptedClock();
        long estimate = rawEstimate(clock.getThreadPool(), pageSize);
        long limit = between((int) (estimate / 4), (int) estimate - 1);
        IndexingPressure pressure = pressure(limit);
        Harness harness = new Harness(clock, pressure, updateRequest(pageSize), NoopCircuitBreaker.INSTANCE, null);
        CountingHit hit = harness.hit(bytes((int) limit + between(1_024, 4_096)));
        // Little enough that a reserve of half the limit is admitted beside it.
        long otherBytes = between(1, (int) roomBesideAHalfReserve(limit));
        long otherCoordinating = otherIsReplica ? 0L : otherBytes;
        try (Releasable ignored = holdOtherBytes(pressure, otherBytes, otherIsReplica)) {
            harness.start();
            assertThat(harness.client.searches.size(), equalTo(1));
            harness.feed(List.of(hit));

            EsRejectedExecutionException rejected = expectThrows(EsRejectedExecutionException.class, harness.listener::actionGet);
            assertThat(rejected.getMessage(), containsString("max_coordinating_bytes"));
            assertThat(harness.client.bulks, equalTo(List.of()));
            assertThat(hit.releases.get(), equalTo(1));
            assertThat(pressure.stats().getReindexPageExceedsLimit(), equalTo(1L));
            assertThat(pressure.stats().getReindexPageBytesAboveEstimate(), equalTo(0L));
            assertThat("only the other holder's bytes", harness.chargedBytes(), equalTo(otherCoordinating));
        }
        assertThat(pressure.stats().getCurrentCoordinatingBytes(), equalTo(0L));
        assertThat(pressure.stats().getCurrentReplicaBytes(), equalTo(0L));
    }

    /**
     * An estimate that is under half the limit, and so was not capped, waits when the budget is full of coordinating bytes. Each refused
     * attempt counts as a reserve rejection. None counts as a coordinating rejection, because no client sees a 429 for it.
     */
    public void testRefusedUncappedReserveBesideCoordinatingBytesCountsReserveRejectionsAndNoCoordinatingRejections() {
        assertRefusedUncappedReserveCountsReserveRejectionsAndNoCoordinatingRejections(false);
    }

    /**
     * As {@link #testRefusedUncappedReserveBesideCoordinatingBytesCountsReserveRejectionsAndNoCoordinatingRejections}, with the budget
     * full of replica bytes.
     */
    public void testRefusedUncappedReserveBesideReplicaBytesCountsReserveRejectionsAndNoCoordinatingRejections() {
        assertRefusedUncappedReserveCountsReserveRejectionsAndNoCoordinatingRejections(true);
    }

    private void assertRefusedUncappedReserveCountsReserveRejectionsAndNoCoordinatingRejections(boolean otherIsReplica) {
        int pageSize = between(1, 3);
        ScriptedClock clock = new ScriptedClock();
        long estimate = rawEstimate(clock.getThreadPool(), pageSize);
        // Half of this is above the raw estimate, so the reserve is the raw estimate and nothing cuts it down.
        long limit = 2 * estimate + between(0, 4_096);
        IndexingPressure pressure = pressure(limit);
        Harness harness = new Harness(clock, pressure, updateRequest(pageSize), NoopCircuitBreaker.INSTANCE, null);
        CountingHit hit = harness.hit(bytes(between(32, 256)));
        harness.client.handler = (bulk, listener) -> listener.onResponse(harness.success(bulk));
        harness.onDone = harness::finishWithEmptyPage;
        Releasable other = otherIsReplica
            ? pressure.markReplicaOperationStarted(1, limit, false)
            : pressure.markCoordinatingOperationStarted(1, limit, false);
        try {
            harness.start();
            assertThat(pressure.stats().getReindexPageReserveRejections(), equalTo(1L));
            int retries = between(1, 4);
            for (int i = 0; i < retries; i++) {
                harness.queue.advanceTime();
                assertThat(pressure.stats().getReindexPageReserveRejections(), equalTo(2L + i));
            }
            assertThat(pressure.stats().getCoordinatingRejections(), equalTo(0L));
            assertThat(harness.client.searches.size(), equalTo(0));
            assertFalse(harness.listener.isDone());
            assertTrue(harness.queue.hasDeferredTasks());
        } finally {
            other.close();
        }

        harness.queue.advanceTime();
        assertThat(harness.client.searches.size(), equalTo(1));
        assertThat("the whole raw estimate, as it is under half the limit", harness.chargedBytes(), equalTo(estimate));
        harness.feed(List.of(hit));
        assertThat(harness.listener.actionGet(TimeValue.ZERO).getUpdated(), equalTo(1L));
        assertThat(pressure.stats().getCoordinatingRejections(), equalTo(0L));
        assertThat(pressure.stats().getCurrentCoordinatingBytes(), equalTo(0L));
    }

    /**
     * A request may set its initial retry backoff to zero. A full budget must not make the worker spin on the scheduler with no delay,
     * so across a short scripted advance the scheduler is asked a bounded number of times and never for less than 50 ms.
     */
    public void testZeroInitialBackoffDoesNotSpinOnAFullBudget() {
        int pageSize = between(1, 3);
        ScriptedClock clock = new ScriptedClock();
        long limit = rawEstimate(clock.getThreadPool(), pageSize) + between(0, 4_096);
        IndexingPressure pressure = pressure(limit);
        UpdateByQueryRequest request = updateRequest(pageSize);
        request.setRetryBackoffInitialTime(TimeValue.ZERO);
        Harness harness = new Harness(clock, pressure, request, NoopCircuitBreaker.INSTANCE, null);
        try (Releasable ignored = pressure.markCoordinatingOperationStarted(1, limit, false)) {
            harness.start();
            long advanceMillis = between(200, 2_000);
            long elapsed = 0;
            while (elapsed < advanceMillis) {
                assertTrue("the refused reserve has to be tried again", clock.hasDeferredTasks());
                elapsed += clock.nextDelay().millis();
                clock.advanceTime();
            }

            for (TimeValue delay : clock.delays) {
                assertThat(delay.millis(), greaterThanOrEqualTo(50L));
            }
            assertThat(
                "one attempt for the first refusal and at most one more for each 50 ms advanced",
                (long) clock.delays.size(),
                lessThanOrEqualTo(advanceMillis / 50 + 2)
            );
            assertThat(pressure.stats().getReindexPageReserveRejections(), equalTo((long) clock.delays.size()));
            assertThat(pressure.stats().getCoordinatingRejections(), equalTo(0L));
            assertThat(harness.client.searches.size(), equalTo(0));
            assertFalse(harness.listener.isDone());
        }
    }

    /**
     * Runs held attempts, one at a time and a bounded number of times, until the task is done. A task that is still not done with
     * nothing held is hung, and the caller's assertion reports it.
     */
    private static void runHeldAttemptsUntilDone(Harness harness, ScriptedClock clock) {
        for (int i = 0; i < 5 && harness.listener.isDone() == false && clock.hasDeferredTasks(); i++) {
            clock.advanceTime();
        }
    }

    /**
     * A page count times a per-document setting that wraps around must not reserve a negative number, which would shrink the node's
     * total. It falls back to half the coordinating limit, which is where any other estimate above that is capped.
     */
    public void testOverflowingEstimateReservesHalfTheCoordinatingLimitAndNeverGoesNegative() {
        int pageSize = between(2, 1_000);
        long perDocument = Long.MAX_VALUE / pageSize + between(1, 1_000);
        assertThat("the product has to overflow for this test to mean anything", (long) pageSize * perDocument, lessThan(0L));
        ScriptedClock clock = new ScriptedClock();
        long limit = between(10_000, 1_000_000);
        IndexingPressure pressure = pressure(limit);
        pressure.setReindexBytesEstimatePerDocument(ByteSizeValue.ofBytes(perDocument));
        Harness harness = new Harness(clock, pressure, updateRequest(pageSize), NoopCircuitBreaker.INSTANCE, null);
        CountingHit hit = harness.hit(bytes(between(32, 256)));
        harness.client.handler = (bulk, listener) -> listener.onResponse(harness.success(bulk));
        harness.onDone = harness::finishWithEmptyPage;

        harness.start();

        assertThat(harness.client.searches.size(), equalTo(1));
        assertThat(harness.chargedBytes(), equalTo(halfTheLimit(pressure)));
        assertThat(pressure.stats().getCurrentCoordinatingBytes(), greaterThanOrEqualTo(0L));
        assertThat(pressure.stats().getCurrentCombinedCoordinatingAndPrimaryBytes(), greaterThanOrEqualTo(0L));
        assertThat(pressure.stats().getReindexPageReserveRejections(), equalTo(0L));

        harness.feed(List.of(hit));
        assertThat(harness.listener.actionGet(TimeValue.ZERO).getUpdated(), equalTo(1L));
        assertThat(pressure.stats().getCurrentCoordinatingBytes(), equalTo(0L));
        assertThat(pressure.stats().getCurrentCombinedCoordinatingAndPrimaryBytes(), equalTo(0L));
    }

    /**
     * A reserve that is not capped and has been refused for 60 seconds is logged as one WARN, so a stuck wait leaves a trace. Nothing
     * is logged before that, and the same wait is not logged again however much longer it goes on.
     */
    public void testOneWarnIsLoggedOnceANonCappedReserveHasBeenRefusedForSixtySeconds() {
        int pageSize = between(1, 3);
        ScriptedClock clock = new ScriptedClock();
        long limit = rawEstimate(clock.getThreadPool(), pageSize) + between(0, 4_096);
        IndexingPressure pressure = pressure(limit);
        Harness harness = new Harness(clock, pressure, updateRequest(pageSize), NoopCircuitBreaker.INSTANCE, null);
        WarnCounter warns = new WarnCounter();
        try (MockLog mockLog = MockLog.capture(ReindexCoordinatingIndexingPressureTests.class)) {
            mockLog.addExpectation(warns);
            try (Releasable ignored = pressure.markCoordinatingOperationStarted(1, limit, false)) {
                harness.start();
                assertThat(warns.count.get(), equalTo(0));

                // Well short of a minute since the first refusal, even with the delays the attempts themselves add.
                clock.jump(30_000);
                clock.advanceTime();
                clock.jump(25_000);
                clock.advanceTime();
                assertThat(warns.count.get(), equalTo(0));

                // Well past a minute since the first refusal.
                clock.jump(10_000);
                clock.advanceTime();
                assertThat(warns.count.get(), equalTo(1));

                for (int i = 0; i < between(1, 4); i++) {
                    clock.jump(between(1_000, 120_000));
                    clock.advanceTime();
                }
                assertThat(warns.count.get(), equalTo(1));
                assertThat(harness.client.searches.size(), equalTo(0));
                assertFalse(harness.listener.isDone());
            }
            mockLog.assertAllExpectationsMatched();
        }
    }

    /**
     * Counts the WARN events a captured logger sees. A plain counter, because the message text is not what the rule is about.
     */
    private static final class WarnCounter implements MockLog.LoggingExpectation {
        final AtomicInteger count = new AtomicInteger();

        @Override
        public void match(LogEvent event) {
            if (event.getLevel() == Level.WARN) {
                count.incrementAndGet();
            }
        }

        @Override
        public void assertMatched() {}
    }

    /**
     * Holds bytes the reindex did not take, as a replica write or another task's coordinating bytes would.
     */
    private static Releasable holdOtherBytes(IndexingPressure pressure, long bytes, boolean replica) {
        return replica ? pressure.markReplicaOperationStarted(1, bytes, false) : pressure.markCoordinatingOperationStarted(1, bytes, false);
    }

    /**
     * The most a page estimate may reserve. Two reserves of this size fill the budget on an empty node.
     */
    private static long halfTheLimit(IndexingPressure pressure) {
        return pressure.getCoordinatingLimitInBytes() / 2;
    }

    private static SystemIndices systemIndicesMatching(String pattern) {
        return new SystemIndices(
            List.of(new SystemIndices.Feature("plugin", "test feature", List.of(SystemIndexDescriptorUtils.createUnmanaged(pattern, ""))))
        );
    }

    /**
     * An indices lookup that says each named index is, or is not, a system index.
     */
    private static Supplier<SortedMap<String, IndexAbstraction>> lookupOf(boolean system, String... indices) {
        SortedMap<String, IndexAbstraction> lookup = new TreeMap<>();
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build();
        for (String index : indices) {
            lookup.put(
                index,
                new IndexAbstraction.ConcreteIndex(
                    IndexMetadata.builder(index).settings(settings).system(system).numberOfShards(1).numberOfReplicas(0).build()
                )
            );
        }
        return () -> lookup;
    }

    private long reservedPage(ScriptedClock clock, IndexingPressure pressure, int pageSize) {
        Harness harness = new Harness(clock, pressure, updateRequest(pageSize), NoopCircuitBreaker.INSTANCE, null);
        harness.client.handler = (bulk, listener) -> listener.onResponse(harness.success(bulk));
        harness.onDone = harness::finishWithEmptyPage;
        harness.start();
        long reserved = harness.chargedBytes();
        harness.feed(List.of(harness.hit(bytes(32))));
        harness.listener.actionGet(TimeValue.ZERO);
        assertThat(pressure.stats().getCurrentCoordinatingBytes(), equalTo(0L));
        return reserved;
    }

    private static long rawEstimate(ThreadPool threadPool, int pageSize) {
        return (long) pageSize * ESTIMATED_HIT_BYTES + threadPool.getThreadContext().estimatedRequestContextBytes();
    }

    private static UpdateByQueryRequest updateRequest(int pageSize) {
        return updateRequest(pageSize, randomAlphaOfLength(6));
    }

    private static UpdateByQueryRequest updateRequest(int pageSize, String... indices) {
        UpdateByQueryRequest request = new UpdateByQueryRequest(indices);
        request.getSearchRequest().source().size(pageSize);
        request.setRetryBackoffInitialTime(TimeValue.timeValueMillis(randomIntBetween(1, 40)));
        return request;
    }

    private static ReindexRequest reindexRequest(int pageSize) {
        return reindexRequest(pageSize, randomAlphaOfLength(6), randomAlphaOfLength(6));
    }

    private static ReindexRequest reindexRequest(int pageSize, String source, String destination) {
        ReindexRequest request = new ReindexRequest();
        request.getSearchRequest().indices(source);
        request.getSearchRequest().source().size(pageSize);
        request.getDestination().index(destination);
        request.setRetryBackoffInitialTime(TimeValue.timeValueMillis(randomIntBetween(1, 40)));
        return request;
    }

    private static DeleteByQueryRequest deleteRequest(int pageSize) {
        return deleteRequest(pageSize, randomAlphaOfLength(6));
    }

    private static DeleteByQueryRequest deleteRequest(int pageSize, String... indices) {
        DeleteByQueryRequest request = new DeleteByQueryRequest(indices);
        request.getSearchRequest().source().size(pageSize);
        request.setRetryBackoffInitialTime(TimeValue.timeValueMillis(randomIntBetween(1, 40)));
        return request;
    }

    private static IndexingPressure pressure(long limit) {
        return pressure(Settings.builder().put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), limit + "b").build());
    }

    private static IndexingPressure pressure(Settings settings) {
        return new IndexingPressure(settings);
    }

    private static BytesReference bytes(int length) {
        return new BytesArray(new byte[length]);
    }

    private static CircuitBreaker trippingBreaker(AtomicLong net) {
        return new NoopCircuitBreaker("test") {
            @Override
            public void addEstimateBytesAndMaybeBreak(long bytes, String label) {
                throw new CircuitBreakingException("simulated breaker trip", bytes, 1L, CircuitBreaker.Durability.TRANSIENT);
            }

            @Override
            public void addWithoutBreaking(long bytes) {
                net.addAndGet(bytes);
            }
        };
    }

    private static BulkResponse withConflict(BulkRequest bulk, int conflictAt) {
        BulkItemResponse[] items = new BulkItemResponse[bulk.requests().size()];
        for (int i = 0; i < items.length; i++) {
            DocWriteRequest<?> item = bulk.requests().get(i);
            ShardId shardId = new ShardId(item.index(), "uuid", 0);
            if (i == conflictAt) {
                items[i] = BulkItemResponse.failure(
                    i,
                    item.opType(),
                    new BulkItemResponse.Failure(
                        item.index(),
                        item.id(),
                        new VersionConflictEngineException(shardId, item.id(), "conflict")
                    )
                );
            } else {
                items[i] = BulkItemResponse.success(i, item.opType(), new IndexResponse(shardId, item.id(), 1L, 1L, 1L, false));
            }
        }
        return new BulkResponse(items, 1L);
    }

    private static BulkResponse oneRejected(BulkRequest bulk) {
        BulkItemResponse[] items = new BulkItemResponse[bulk.requests().size()];
        for (int i = 0; i < items.length; i++) {
            DocWriteRequest<?> item = bulk.requests().get(i);
            if (i == items.length - 1) {
                items[i] = BulkItemResponse.failure(
                    i,
                    item.opType(),
                    new BulkItemResponse.Failure(item.index(), item.id(), new EsRejectedExecutionException("full"))
                );
            } else {
                items[i] = BulkItemResponse.success(
                    i,
                    item.opType(),
                    new IndexResponse(new ShardId(item.index(), "uuid", 0), item.id(), 1L, 1L, 1L, false)
                );
            }
        }
        return new BulkResponse(items, 1L);
    }

    /**
     * Records the coordinating total just before each reservation so a test can see the gap after one page lets go and
     * before the next estimate is taken. A page reservation has to be allowed to fail, so force execution stays off.
     */
    private static final class WatchingPressure extends IndexingPressure {
        final List<Long> bytesBeforeReservation = new ArrayList<>();

        WatchingPressure(long limit) {
            super(Settings.builder().put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), limit + "b").build());
        }

        @Override
        public Coordinating markCoordinatingOperationStarted(int operations, long bytes, boolean forceExecution) {
            assertFalse(forceExecution);
            bytesBeforeReservation.add(stats().getCurrentCoordinatingBytes());
            return super.markCoordinatingOperationStarted(operations, bytes, forceExecution);
        }

        @Override
        public Coordinating reserveReindexPage(int operations, long bytes, boolean systemOnly) {
            bytesBeforeReservation.add(stats().getCurrentCoordinatingBytes());
            return super.reserveReindexPage(operations, bytes, systemOnly);
        }
    }

    private final class Harness {
        final ScriptedClock queue;
        final IndexingPressure pressure;
        final CountingListener listener = new CountingListener();
        final RecordingClient client;
        final TaskManager taskManager;
        final BulkByPaginatedSearchTask task;
        final AbstractAsyncBulkByPaginatedSearchAction<?, ?> action;
        Runnable onDone = () -> {};

        Harness(IndexingPressure pressure, AbstractBulkByPaginatedSearchRequest<?> request, CircuitBreaker breaker, ScriptService scripts) {
            this(new ScriptedClock(), pressure, request, breaker, scripts);
        }

        Harness(
            ScriptedClock queue,
            IndexingPressure pressure,
            AbstractBulkByPaginatedSearchRequest<?> request,
            CircuitBreaker breaker,
            ScriptService scripts
        ) {
            this(queue, pressure, request, breaker, scripts, null, null);
        }

        Harness(
            ScriptedClock queue,
            IndexingPressure pressure,
            AbstractBulkByPaginatedSearchRequest<?> request,
            CircuitBreaker breaker,
            ScriptService scripts,
            @Nullable SystemIndices systemIndices,
            @Nullable Supplier<SortedMap<String, IndexAbstraction>> indicesLookup
        ) {
            this.queue = queue;
            this.pressure = pressure;
            this.client = new RecordingClient(new NoOpClient(queue.getThreadPool()));
            this.taskManager = new TaskManager(Settings.EMPTY, queue.getThreadPool(), Set.of());
            this.task = (BulkByPaginatedSearchTask) taskManager.register("reindex", "test", request);
            task.setWorker(request.getRequestsPerSecond(), null);
            DiscoveryNode localNode = DiscoveryNodeUtils.builder("coordinator").roles(emptySet()).build();
            ParentTaskAssigningClient parent = new ParentTaskAssigningClient(client, localNode, task);
            if (request instanceof DeleteByQueryRequest delete) {
                this.action = new AsyncDeleteByQueryAction(
                    task,
                    logger,
                    parent,
                    queue.getThreadPool(),
                    delete,
                    scripts,
                    listener,
                    null,
                    TimeValue.ZERO,
                    new ReindexSettings(),
                    breaker,
                    pressure,
                    systemIndices,
                    indicesLookup
                );
            } else if (request instanceof ReindexRequest reindex) {
                this.action = new Reindexer.AsyncIndexBySearchAction(
                    task,
                    logger,
                    parent,
                    parent,
                    queue.getThreadPool(),
                    scripts,
                    ClusterState.EMPTY_STATE.projectState(Metadata.DEFAULT_PROJECT_ID),
                    null,
                    reindex,
                    listener,
                    null,
                    TimeValue.ZERO,
                    null,
                    new ReindexSettings(),
                    breaker,
                    pressure,
                    systemIndices,
                    indicesLookup
                );
            } else {
                this.action = new TransportUpdateByQueryAction.AsyncIndexBySearchAction(
                    task,
                    logger,
                    parent,
                    queue.getThreadPool(),
                    scripts,
                    (UpdateByQueryRequest) request,
                    listener,
                    TimeValue.ZERO,
                    null,
                    new ReindexSettings(),
                    breaker,
                    pressure,
                    systemIndices,
                    indicesLookup
                );
            }
        }

        CountingHit hit(BytesReference source) {
            return hit(source, 1L);
        }

        CountingHit hit(BytesReference source, long version) {
            return hitIn(action.mainRequest.getSearchRequest().indices()[0], source, version);
        }

        CountingHit hitIn(String index, BytesReference source, long version) {
            return new CountingHit(index, format("id-%s", randomAlphaOfLength(4)), source, version);
        }

        void start() {
            action.start();
        }

        void feed(List<Hit> hits) {
            feed(hits, false, List.of());
        }

        void feed(List<Hit> hits, boolean timedOut, List<PaginatedSearchFailure> failures) {
            PaginatedHitSource.Response response = new PaginatedHitSource.Response(timedOut, failures, hits.size(), hits, null);
            action.onPaginatedSearchResponse(new PaginatedHitSource.AsyncResponse() {
                @Override
                public PaginatedHitSource.Response response() {
                    return response;
                }

                @Override
                public void done(TimeValue extraKeepAlive) {
                    onDone.run();
                }
            });
        }

        void finishWithEmptyPage() {
            feed(List.of());
        }

        long chargedBytes() {
            return pressure.stats().getCurrentCoordinatingBytes();
        }

        BulkResponse success(BulkRequest bulk) {
            return success(bulk, false);
        }

        BulkResponse success(BulkRequest bulk, boolean created) {
            BulkItemResponse[] items = new BulkItemResponse[bulk.requests().size()];
            for (int i = 0; i < items.length; i++) {
                DocWriteRequest<?> item = bulk.requests().get(i);
                ShardId shardId = new ShardId(item.index(), "uuid", 0);
                if (item instanceof DeleteRequest delete) {
                    items[i] = BulkItemResponse.success(i, item.opType(), new DeleteResponse(shardId, delete.id(), 1L, 1L, 1L, true));
                } else {
                    items[i] = BulkItemResponse.success(i, item.opType(), new IndexResponse(shardId, item.id(), 1L, 1L, 1L, created));
                }
            }
            return new BulkResponse(items, 1L);
        }
    }

    /**
     * The task's listener, counting how often it is completed. A plain future ignores a second completion, and a task has to finish
     * exactly once however many paths try to finish it.
     */
    private static final class CountingListener extends PlainActionFuture<BulkByPaginatedSearchResponse> {
        final AtomicInteger completions = new AtomicInteger();

        @Override
        public void onResponse(BulkByPaginatedSearchResponse result) {
            completions.incrementAndGet();
            super.onResponse(result);
        }

        @Override
        public void onFailure(Exception e) {
            completions.incrementAndGet();
            super.onFailure(e);
        }
    }

    private static final class RecordingClient extends FilterClient {
        final List<BulkRequest> bulks = new ArrayList<>();
        final List<ActionListener<SearchResponse>> searches = new ArrayList<>();
        BiConsumer<BulkRequest, ActionListener<BulkResponse>> handler = (bulk, listener) -> {
            throw new AssertionError("unexpected bulk");
        };

        RecordingClient(Client in) {
            super(in);
        }

        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            if (request instanceof BulkRequest bulk) {
                @SuppressWarnings("unchecked")
                ActionListener<BulkResponse> bulkListener = (ActionListener<BulkResponse>) listener;
                handler.accept(bulk, bulkListener);
                return;
            }
            if (request instanceof SearchRequest) {
                @SuppressWarnings("unchecked")
                ActionListener<SearchResponse> searchListener = (ActionListener<SearchResponse>) listener;
                searches.add(searchListener);
                return;
            }
            if (request instanceof ClearScrollRequest) {
                @SuppressWarnings("unchecked")
                ActionListener<ClearScrollResponse> clearListener = (ActionListener<ClearScrollResponse>) listener;
                clearListener.onResponse(new ClearScrollResponse(true, 1));
                return;
            }
            throw new AssertionError("unexpected " + request.getClass());
        }
    }

    /**
     * Runs a zero delay inline, and holds a positive delay until the test releases it.
     * Cancelling removes the held attempt so a reservation retry does not search after the task is cancelled.
     * <p>
     * The clock is scripted: {@link #relativeTimeInMillis()} only moves when the test advances it, by running a held attempt, which
     * moves it on by that attempt's delay, or by {@link #jump}. Every delay the worker asked for is recorded in {@link #delays}.
     */
    private final class ScriptedClock extends TestThreadPool {
        private static final class Held {
            final Runnable command;
            final TimeValue delay;

            Held(Runnable command, TimeValue delay) {
                this.command = command;
                this.delay = delay;
            }
        }

        private final List<Held> deferred = new ArrayList<>();
        private boolean open;
        private long advancedMillis;
        final List<TimeValue> delays = new ArrayList<>();

        ScriptedClock() {
            super(getTestName() + "-" + clocks.size());
            clocks.add(this);
            open = true;
        }

        @Override
        public long relativeTimeInMillis() {
            return super.relativeTimeInMillis() + advancedMillis;
        }

        @Override
        public ScheduledCancellable schedule(Runnable command, TimeValue delay, java.util.concurrent.Executor executor) {
            if (open == false) {
                return super.schedule(command, delay, executor);
            }
            delays.add(delay);
            // A worker that spins on a zero delay would recurse here for ever, so fail the test long before the stack goes.
            assertThat("the worker is spinning on the scheduler", delays.size(), lessThan(5_000));
            if (delay.millis() <= 0) {
                command.run();
                return immediate();
            }
            Held held = new Held(command, delay);
            deferred.add(held);
            return held(held);
        }

        ThreadPool getThreadPool() {
            return this;
        }

        boolean hasDeferredTasks() {
            return deferred.isEmpty() == false;
        }

        int deferredCount() {
            return deferred.size();
        }

        /**
         * Runs the oldest held attempt, after moving the clock on by the delay it asked for.
         */
        void advanceTime() {
            Held next = deferred.remove(0);
            advancedMillis += next.delay.millis();
            next.command.run();
        }

        TimeValue nextDelay() {
            return deferred.get(0).delay;
        }

        /**
         * Moves the clock on without running anything.
         */
        void jump(long millis) {
            advancedMillis += millis;
        }

        private static ScheduledCancellable immediate() {
            return new ScheduledCancellable() {
                @Override
                public long getDelay(TimeUnit unit) {
                    return 0;
                }

                @Override
                public int compareTo(Delayed other) {
                    return 0;
                }

                @Override
                public boolean cancel() {
                    return false;
                }

                @Override
                public boolean isCancelled() {
                    return false;
                }
            };
        }

        private ScheduledCancellable held(Held held) {
            AtomicBoolean cancelled = new AtomicBoolean();
            return new ScheduledCancellable() {
                @Override
                public long getDelay(TimeUnit unit) {
                    return unit.convert(held.delay.millis(), TimeUnit.MILLISECONDS);
                }

                @Override
                public int compareTo(Delayed other) {
                    return 0;
                }

                @Override
                public boolean cancel() {
                    if (cancelled.compareAndSet(false, true) == false) {
                        return false;
                    }
                    return deferred.remove(held);
                }

                @Override
                public boolean isCancelled() {
                    return cancelled.get();
                }
            };
        }
    }

    private static final class CountingHit extends PaginatedHitSource.BasicHit {
        final AtomicInteger releases = new AtomicInteger();
        private final AtomicBoolean released = new AtomicBoolean();

        CountingHit(String index, String id, BytesReference source, long version) {
            super(index, id, version);
            setSource(source, XContentType.JSON);
            setSeqNo(1L);
            setPrimaryTerm(1L);
        }

        @Override
        public void release() {
            // A second release does nothing, as PaginatedHitSource.Hit#release requires. Dropping a page releases hits that were
            // already released, so counting every call would report releases no real hit would see.
            if (released.compareAndSet(false, true)) {
                releases.incrementAndGet();
            }
        }
    }
}
