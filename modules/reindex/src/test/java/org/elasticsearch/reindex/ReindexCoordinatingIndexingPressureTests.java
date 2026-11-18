/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v 1".
 */

package org.elasticsearch.reindex;

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
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.FilterClient;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.reindex.AbstractBulkByPaginatedSearchRequest;
import org.elasticsearch.index.reindex.BulkByPaginatedSearchResponse;
import org.elasticsearch.index.reindex.BulkByPaginatedSearchTask;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.reindex.PaginatedHitSource.Hit;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.UpdateByQueryScript;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.Scheduler.ScheduledCancellable;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static java.util.Collections.emptySet;
import static org.elasticsearch.core.Strings.format;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A reindex family bulk takes one coordinating indexing pressure charge for its original size and holds it until the bulk's own
 * cleanup. A batch that does not fit fails the task and does not schedule a retry.
 */
public class ReindexCoordinatingIndexingPressureTests extends ESTestCase {

    /**
     * One source fits in this limit. Two copies of it do not, so a second charge of the same bulk is rejected.
     * The slack covers the request object and the thread context, which are small next to the source.
     */
    private static final int SOURCE_BYTES = 100_000;
    private static final long ONE_BATCH_LIMIT = SOURCE_BYTES + 32_768L;

    private final List<ScriptedClock> clocks = new ArrayList<>();

    @After
    public void terminateClocks() {
        for (ScriptedClock clock : clocks) {
            terminate(clock);
        }
        clocks.clear();
    }

    public void testPrepareBulkRequestFailsWhenTheCoordinatingBudgetIsFull() {
        IndexingPressure pressure = pressure(ONE_BATCH_LIMIT);
        AtomicLong breakerNet = new AtomicLong();
        try (Releasable ignored = pressure.markCoordinatingOperationStarted(1, pressure.stats().getMemoryLimit(), false)) {
            Harness harness = new Harness(pressure, updateRequest(), trackingBreaker(breakerNet), null);
            CountingHit hit = harness.hit(bytes(SOURCE_BYTES));
            harness.feed(List.of(hit));
            harness.drain();

            EsRejectedExecutionException rejected = expectThrows(EsRejectedExecutionException.class, harness.listener::actionGet);
            assertThat(rejected.getMessage(), containsString("max_coordinating_bytes"));
            assertThat(harness.client.bulks, equalTo(List.of()));
            assertFalse(harness.queue.hasDeferredTasks());
            assertThat(hit.releases.get(), equalTo(1));
            assertThat(breakerNet.get(), equalTo(0L));
            assertThat(pressure.stats().getCurrentCoordinatingBytes(), equalTo(pressure.stats().getMemoryLimit()));
        }
        assertThat(pressure.stats().getCurrentCoordinatingBytes(), equalTo(0L));
    }

    public void testCoordinatingBytesStayElevatedWhileARejectionWaitsToRetry() {
        Harness harness = new Harness(pressure(Settings.EMPTY), updateRequest(), NoopCircuitBreaker.INSTANCE, null);
        CountingHit hit = harness.hit(bytes(SOURCE_BYTES));
        AtomicLong charged = new AtomicLong();
        harness.client.handler = (bulk, listener) -> {
            harness.client.bulks.add(bulk);
            if (harness.client.bulks.size() == 1) {
                charged.set(harness.chargedBytes());
                assertTrue(bulk.indexingPressureAccounted());
                assertThat(charged.get(), greaterThanOrEqualTo((long) hit.getSource().length()));
                listener.onFailure(new EsRejectedExecutionException("full"));
            } else {
                assertThat(bulk, sameInstance(harness.client.bulks.get(0)));
                assertTrue(bulk.indexingPressureAccounted());
                listener.onResponse(harness.success(bulk));
            }
        };
        harness.onDone = harness::finishWithEmptyPage;
        harness.feed(List.of(hit));
        harness.drain();

        assertThat(harness.client.bulks.size(), equalTo(1));
        assertTrue(harness.queue.hasDeferredTasks());
        assertThat(harness.chargedBytes(), equalTo(charged.get()));
        assertFalse(harness.listener.isDone());

        harness.queue.advanceTime();
        harness.drain();
        BulkByPaginatedSearchResponse response = harness.listener.actionGet(TimeValue.ZERO);
        assertThat(response.getUpdated(), equalTo(1L));
        assertThat(harness.chargedBytes(), equalTo(0L));
        assertThat(hit.releases.get(), equalTo(1));
    }

    public void testCoordinatingBytesDropWhenRetriesAreExhausted() {
        UpdateByQueryRequest request = updateRequest();
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
        harness.feed(List.of(hit));
        harness.drain();

        assertThat(harness.chargedBytes(), equalTo(charged.get()));
        assertThat(charged.get(), greaterThan(0L));
        assertTrue(harness.queue.hasDeferredTasks());

        harness.queue.advanceTime();
        harness.drain();
        EsRejectedExecutionException rejected = expectThrows(EsRejectedExecutionException.class, harness.listener::actionGet);
        assertThat(rejected.getMessage(), equalTo("full"));
        assertThat(harness.client.bulks.size(), equalTo(2));
        assertThat(harness.chargedBytes(), equalTo(0L));
        assertThat(hit.releases.get(), equalTo(1));
        assertFalse(harness.queue.hasDeferredTasks());
    }

    public void testPartialRetryDoesNotTakeASecondCoordinatingCharge() {
        Harness harness = new Harness(pressure(Settings.EMPTY), updateRequest(), NoopCircuitBreaker.INSTANCE, null);
        CountingHit kept = harness.hit(bytes(SOURCE_BYTES));
        CountingHit rejected = harness.hit(bytes(SOURCE_BYTES));
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
        harness.feed(List.of(kept, rejected));
        harness.drain();

        assertThat(charged.get(), greaterThanOrEqualTo((long) SOURCE_BYTES));
        assertThat(harness.chargedBytes(), equalTo(charged.get()));
        harness.queue.advanceTime();
        harness.drain();

        BulkRequest retryBulk = retry.get();
        assertThat(retryBulk, not(sameInstance(harness.client.bulks.get(0))));
        assertTrue(retryBulk.indexingPressureAccounted());
        assertThat(retryBulk.numberOfActions(), equalTo(1));
        assertThat(retryBulk.ramBytesUsed(), lessThan(charged.get()));
        assertThat(harness.chargedBytes(), equalTo(charged.get()));
        assertThat(kept.releases.get(), equalTo(0));
        assertThat(rejected.releases.get(), equalTo(0));

        retryListener.get().onResponse(harness.success(retryBulk));
        harness.drain();
        assertThat(harness.listener.actionGet(TimeValue.ZERO).getUpdated(), equalTo(2L));
        assertThat(harness.chargedBytes(), equalTo(0L));
        assertThat(kept.releases.get(), equalTo(1));
        assertThat(rejected.releases.get(), equalTo(1));
    }

    public void testSuccessfulBatchReleasesItsChargeBeforeTheNextPage() {
        IndexingPressure pressure = pressure(ONE_BATCH_LIMIT);
        assertThat(pressure.stats().getMemoryLimit(), equalTo(ONE_BATCH_LIMIT));
        Harness harness = new Harness(pressure, updateRequest(), NoopCircuitBreaker.INSTANCE, null);
        CountingHit first = harness.hit(bytes(SOURCE_BYTES));
        CountingHit second = harness.hit(bytes(SOURCE_BYTES));
        AtomicInteger pages = new AtomicInteger();
        List<Long> charges = new ArrayList<>();
        harness.client.handler = (bulk, listener) -> {
            charges.add(harness.chargedBytes());
            assertTrue(bulk.indexingPressureAccounted());
            listener.onResponse(harness.success(bulk));
        };
        harness.onDone = () -> {
            assertThat(harness.chargedBytes(), equalTo(0L));
            if (pages.getAndIncrement() == 0) {
                harness.feed(List.of(second));
            } else {
                harness.action.setScroll(null);
                harness.finishWithEmptyPage();
            }
        };
        harness.action.setScroll("more");
        harness.feed(List.of(first));
        harness.drain();

        BulkByPaginatedSearchResponse response = harness.listener.actionGet(TimeValue.ZERO);
        assertThat(response.getUpdated(), equalTo(2L));
        assertThat(charges.size(), equalTo(2));
        assertThat(charges.get(0), greaterThanOrEqualTo((long) SOURCE_BYTES));
        assertThat(charges.get(0), lessThan(ONE_BATCH_LIMIT));
        assertThat(charges.get(1), greaterThanOrEqualTo((long) SOURCE_BYTES));
        assertThat(charges.get(1), lessThan(ONE_BATCH_LIMIT));
        assertThat(harness.chargedBytes(), equalTo(0L));
        assertThat(first.releases.get(), equalTo(1));
        assertThat(second.releases.get(), equalTo(1));
    }

    public void testCancellingDuringBackoffReleasesTheChargeWhenTheAttemptRuns() {
        Harness harness = new Harness(pressure(Settings.EMPTY), updateRequest(), NoopCircuitBreaker.INSTANCE, null);
        CountingHit hit = harness.hit(bytes(between(1_000, 4_000)));
        harness.client.handler = (bulk, listener) -> {
            harness.client.bulks.add(bulk);
            listener.onFailure(new EsRejectedExecutionException("full"));
        };
        harness.feed(List.of(hit));
        harness.drain();

        assertThat(harness.chargedBytes(), greaterThan(0L));
        assertTrue(harness.queue.hasDeferredTasks());
        harness.taskManager.cancel(harness.task, "stopped", () -> {});
        assertThat(harness.chargedBytes(), greaterThan(0L));
        assertFalse(harness.listener.isDone());

        harness.queue.advanceTime();
        harness.drain();
        assertThat(harness.client.bulks.size(), equalTo(1));
        assertThat(harness.chargedBytes(), equalTo(0L));
        BulkByPaginatedSearchResponse response = harness.listener.actionGet(TimeValue.ZERO);
        assertThat(response.getReasonCancelled(), equalTo("stopped"));
        assertThat(hit.releases.get(), equalTo(1));
    }

    public void testSecondBatchIsRejectedWhileTheFirstChargeIsHeld() {
        ScriptedClock queue = new ScriptedClock();
        IndexingPressure pressure = pressure(ONE_BATCH_LIMIT);
        Harness first = new Harness(queue, pressure, updateRequest(), NoopCircuitBreaker.INSTANCE, null);
        Harness second = new Harness(queue, pressure, updateRequest(), NoopCircuitBreaker.INSTANCE, null);
        CountingHit held = first.hit(bytes(SOURCE_BYTES));
        AtomicLong charged = new AtomicLong();
        first.client.handler = (bulk, listener) -> {
            first.client.bulks.add(bulk);
            charged.set(first.chargedBytes());
            listener.onFailure(new EsRejectedExecutionException("full"));
        };
        first.feed(List.of(held));
        queue.runAllRunnableTasks();

        assertThat(first.client.bulks.size(), equalTo(1));
        assertThat(charged.get(), greaterThanOrEqualTo((long) SOURCE_BYTES));
        assertThat(charged.get(), lessThan(ONE_BATCH_LIMIT));
        assertTrue(queue.hasDeferredTasks());

        CountingHit rejected = second.hit(bytes(SOURCE_BYTES));
        second.feed(List.of(rejected));
        queue.runAllRunnableTasks();

        EsRejectedExecutionException failure = expectThrows(EsRejectedExecutionException.class, second.listener::actionGet);
        assertThat(failure.getMessage(), containsString("max_coordinating_bytes"));
        assertThat(second.client.bulks, equalTo(List.of()));
        assertThat(pressure.stats().getCurrentCoordinatingBytes(), equalTo(charged.get()));
        assertThat(rejected.releases.get(), equalTo(1));
        assertThat(held.releases.get(), equalTo(0));
        assertFalse(first.listener.isDone());
    }

    public void testUnscriptedUpdateByQueryChargesTheSharedHitSource() {
        Harness harness = new Harness(pressure(Settings.EMPTY), updateRequest(), NoopCircuitBreaker.INSTANCE, null);
        CountingHit hit = harness.hit(bytes(SOURCE_BYTES));
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
        harness.feed(List.of(hit));
        harness.drain();
        assertThat(harness.listener.actionGet(TimeValue.ZERO).getUpdated(), equalTo(1L));
        assertThat(harness.chargedBytes(), equalTo(0L));
    }

    public void testScriptedUpdateByQueryChargesTheReserializedSource() {
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
        UpdateByQueryRequest request = updateRequest();
        request.setScript(mockScript("shrink"));
        Harness harness = new Harness(pressure(Settings.EMPTY), request, NoopCircuitBreaker.INSTANCE, scripts);
        String json = "{\"p\":\"" + "x".repeat(SOURCE_BYTES) + "\"}";
        CountingHit hit = harness.hit(new BytesArray(json.getBytes(StandardCharsets.UTF_8)));
        harness.client.handler = (bulk, listener) -> {
            IndexRequest index = (IndexRequest) bulk.requests().get(0);
            long charged = harness.chargedBytes();
            long contextBytes = harness.queue.getThreadPool().getThreadContext().estimatedRequestContextBytes();
            assertThat(index.source().length(), lessThan(hit.getSource().length() / 10));
            assertThat(charged, greaterThanOrEqualTo((long) index.source().length()));
            assertThat(charged, lessThan((long) hit.getSource().length()));
            assertThat(charged, equalTo(bulk.ramBytesUsed() + contextBytes));
            listener.onResponse(harness.success(bulk));
        };
        harness.onDone = harness::finishWithEmptyPage;
        harness.feed(List.of(hit));
        harness.drain();
        assertThat(harness.listener.actionGet(TimeValue.ZERO).getUpdated(), equalTo(1L));
        assertThat(harness.chargedBytes(), equalTo(0L));
    }

    public void testDeleteByQueryChargesTheDeleteAndNotTheHitSource() {
        Harness harness = new Harness(pressure(Settings.EMPTY), new DeleteByQueryRequest("docs"), NoopCircuitBreaker.INSTANCE, null);
        CountingHit hit = harness.hit(bytes(SOURCE_BYTES));
        harness.client.handler = (bulk, listener) -> {
            assertThat(bulk.requests().get(0), instanceOf(DeleteRequest.class));
            long charged = harness.chargedBytes();
            long contextBytes = harness.queue.getThreadPool().getThreadContext().estimatedRequestContextBytes();
            assertThat(charged, lessThan(hit.getSource().length() / 4L));
            assertThat(charged, equalTo(bulk.ramBytesUsed() + contextBytes));
            listener.onResponse(harness.success(bulk));
        };
        harness.onDone = harness::finishWithEmptyPage;
        harness.feed(List.of(hit));
        harness.drain();
        assertThat(harness.listener.actionGet(TimeValue.ZERO).getDeleted(), equalTo(1L));
        assertThat(harness.chargedBytes(), equalTo(0L));
    }

    public void testCircuitBreakerTripLeavesCoordinatingBytesUnchanged() {
        IndexingPressure pressure = pressure(Settings.EMPTY);
        AtomicLong breakerNet = new AtomicLong();
        try (Releasable ignored = pressure.markCoordinatingOperationStarted(1, 100, true)) {
            Harness harness = new Harness(pressure, updateRequest(), trippingBreaker(breakerNet), null);
            CountingHit hit = harness.hit(bytes(between(100, 1_000)));
            harness.feed(List.of(hit));
            harness.drain();

            CircuitBreakingException tripped = expectThrows(CircuitBreakingException.class, harness.listener::actionGet);
            assertThat(tripped.getMessage(), equalTo("simulated breaker trip"));
            assertThat(harness.client.bulks, equalTo(List.of()));
            assertFalse(harness.queue.hasDeferredTasks());
            assertThat(hit.releases.get(), equalTo(1));
            assertThat(breakerNet.get(), equalTo(0L));
            assertThat(pressure.stats().getCurrentCoordinatingBytes(), equalTo(100L));
        }
        assertThat(pressure.stats().getCurrentCoordinatingBytes(), equalTo(0L));
    }

    public void testUpdateByQueryCompletesAndReleasesItsCharge() {
        Harness harness = new Harness(pressure(Settings.EMPTY), updateRequest(), NoopCircuitBreaker.INSTANCE, null);
        CountingHit hit = harness.hit(bytes(between(8, 64)));
        AtomicLong charged = new AtomicLong();
        harness.client.handler = (bulk, listener) -> {
            charged.set(harness.chargedBytes());
            assertTrue(bulk.indexingPressureAccounted());
            listener.onResponse(harness.success(bulk));
        };
        harness.onDone = harness::finishWithEmptyPage;
        harness.feed(List.of(hit));
        harness.drain();

        BulkByPaginatedSearchResponse response = harness.listener.actionGet(TimeValue.ZERO);
        assertThat(response.getUpdated(), equalTo(1L));
        assertThat(response.getBulkFailures(), equalTo(List.of()));
        assertThat(charged.get(), greaterThan(0L));
        assertThat(harness.chargedBytes(), equalTo(0L));
        assertThat(response.getReasonCancelled(), nullValue());
    }

    private static UpdateByQueryRequest updateRequest() {
        UpdateByQueryRequest request = new UpdateByQueryRequest("docs");
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

    private static CircuitBreaker trackingBreaker(AtomicLong net) {
        return new NoopCircuitBreaker("test") {
            @Override
            public void addEstimateBytesAndMaybeBreak(long bytes, String label) {
                net.addAndGet(bytes);
            }

            @Override
            public void addWithoutBreaking(long bytes) {
                net.addAndGet(bytes);
            }
        };
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

    private final class Harness {
        final ScriptedClock queue;
        final IndexingPressure pressure;
        final PlainActionFuture<BulkByPaginatedSearchResponse> listener = new PlainActionFuture<>();
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
                    pressure
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
                    pressure
                );
            }
        }

        CountingHit hit(BytesReference source) {
            return new CountingHit("docs", format("id-%s", randomAlphaOfLength(4)), source);
        }

        void feed(List<Hit> hits) {
            PaginatedHitSource.Response response = new PaginatedHitSource.Response(false, List.of(), hits.size(), hits, null);
            action.onPaginatedSearchResponse(
                new AbstractAsyncBulkByPaginatedSearchAction.PaginatedSearchConsumableHitsResponse(new PaginatedHitSource.AsyncResponse() {
                    @Override
                    public PaginatedHitSource.Response response() {
                        return response;
                    }

                    @Override
                    public void done(TimeValue extraKeepAlive) {
                        onDone.run();
                    }
                })
            );
        }

        void finishWithEmptyPage() {
            feed(List.of());
        }

        void drain() {
            queue.runAllRunnableTasks();
        }

        long chargedBytes() {
            return pressure.stats().getCurrentCoordinatingBytes();
        }

        BulkResponse success(BulkRequest bulk) {
            BulkItemResponse[] items = new BulkItemResponse[bulk.requests().size()];
            for (int i = 0; i < items.length; i++) {
                DocWriteRequest<?> item = bulk.requests().get(i);
                ShardId shardId = new ShardId(item.index(), "uuid", 0);
                if (item instanceof DeleteRequest delete) {
                    items[i] = BulkItemResponse.success(i, item.opType(), new DeleteResponse(shardId, delete.id(), 1L, 1L, 1L, true));
                } else {
                    items[i] = BulkItemResponse.success(i, item.opType(), new IndexResponse(shardId, item.id(), 1L, 1L, 1L, false));
                }
            }
            return new BulkResponse(items, 1L);
        }
    }

    private static final class RecordingClient extends FilterClient {
        final List<BulkRequest> bulks = new ArrayList<>();
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
            throw new AssertionError("unexpected " + request.getClass());
        }
    }

    /**
     * Runs a zero delay inline, and holds a positive delay until the test releases it.
     * Retry backoff stays pending without a real sleep, and status reads of the scheduled task still work.
     */
    private final class ScriptedClock extends TestThreadPool {
        private final List<Runnable> deferred = new ArrayList<>();
        private boolean open;

        ScriptedClock() {
            super(getTestName() + "-" + clocks.size());
            clocks.add(this);
            open = true;
        }

        @Override
        public ScheduledCancellable schedule(Runnable command, TimeValue delay, java.util.concurrent.Executor executor) {
            if (open == false) {
                return super.schedule(command, delay, executor);
            }
            if (delay.millis() <= 0) {
                command.run();
                return immediate();
            }
            deferred.add(command);
            return held(delay);
        }

        ThreadPool getThreadPool() {
            return this;
        }

        boolean hasDeferredTasks() {
            return deferred.isEmpty() == false;
        }

        void advanceTime() {
            deferred.remove(0).run();
        }

        void runAllRunnableTasks() {}

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

        private static ScheduledCancellable held(TimeValue delay) {
            return new ScheduledCancellable() {
                @Override
                public long getDelay(TimeUnit unit) {
                    return unit.convert(delay.millis(), TimeUnit.MILLISECONDS);
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
    }

    private static final class CountingHit extends PaginatedHitSource.BasicHit {
        final AtomicInteger releases = new AtomicInteger();

        CountingHit(String index, String id, BytesReference source) {
            super(index, id, 1L);
            setSource(source, XContentType.JSON);
            setSeqNo(1L);
            setPrimaryTerm(1L);
        }

        @Override
        public void release() {
            releases.incrementAndGet();
        }
    }
}
