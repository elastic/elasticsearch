/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfBatchBuilder;
import org.elasticsearch.escf.EscfRowBuffer;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.sourcebatch.SourceBatch;
import org.elasticsearch.sourcebatch.SourceBatchEncodeHelper;
import org.elasticsearch.sourcebatch.SourceValueType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BytesRefRecycler;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.action.bulk.EscfBulkWriter.ResponseHandler.FAIL_ON_FAILED_ITEM;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class EscfBulkWriterTests extends ESTestCase {
    private static final String INDEX = "dst";
    private static final ShardId SHARD = new ShardId(INDEX, "uuid", 0);
    private static final long UNBOUNDED = ByteSizeValue.ofGb(100).getBytes();
    private static final long MAX_TARGET = EscfBulkWriter.Policy.MAX_TARGET_BULK_BYTES;
    private static final long ONE_MB = ByteSizeValue.ofMb(1).getBytes();
    private static final String USER = "user";

    private DeterministicTaskQueue taskQueue;
    private ThreadPool threadPool;
    private Recycler<BytesRef> recycler;
    private CapturingClient client;
    private CircuitBreaker breaker;
    private final List<CircuitBreaker> breakers = new ArrayList<>();
    private final List<IndexingPressure> pressures = new ArrayList<>();
    private final AtomicInteger capacitySignals = new AtomicInteger();

    @Before
    public void setUpClient() {
        taskQueue = new DeterministicTaskQueue();
        threadPool = taskQueue.getThreadPool();
        // a test that leaves a page unreleased fails
        recycler = new BytesRefRecycler(new MockPageCacheRecycler(Settings.EMPTY));
        client = new CapturingClient(threadPool);
        breaker = breaker(ByteSizeValue.ofMb(100));
    }

    @After
    public void checkEverythingIsReleased() {
        while (client.listeners.isEmpty() == false) {
            client.fail(new IllegalStateException("test cleanup"));
        }
        taskQueue.runAllRunnableTasks();
        for (CircuitBreaker b : breakers) {
            assertThat(b.getUsed(), equalTo(0L));
        }
        for (IndexingPressure pressure : pressures) {
            assertThat(pressure.stats().getCurrentCoordinatingBytes(), equalTo(0L));
            assertThat(pressure.stats().getCurrentCoordinatingOps(), equalTo(0L));
        }
    }

    /**
     * Holds the bulks until the test answers them, and records the thread context they are sent under. Like a bulk action that
     * rejects a bulk before taking its batches, it leaves releasing them to the writer.
     */
    private static class CapturingClient extends NoOpClient {
        final List<BulkRequest> bulks = Collections.synchronizedList(new ArrayList<>());
        final List<String> users = Collections.synchronizedList(new ArrayList<>());
        final List<ActionListener<BulkResponse>> listeners = Collections.synchronizedList(new ArrayList<>());

        CapturingClient(ThreadPool threadPool) {
            super(threadPool);
        }

        @Override
        @SuppressWarnings("unchecked")
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            assertThat(action, equalTo(TransportBulkAction.TYPE));
            bulks.add((BulkRequest) request);
            users.add(threadPool().getThreadContext().getHeader(USER));
            listeners.add((ActionListener<BulkResponse>) listener);
        }

        /** Acknowledges the oldest bulk, failing the item at {@code failedItem} unless it is negative. */
        void answer(int failedItem) {
            BulkRequest bulk = bulks.removeFirst();
            users.removeFirst();
            listeners.removeFirst().onResponse(response(bulk, failedItem));
        }

        void answer() {
            answer(-1);
        }

        void fail(Exception e) {
            bulks.removeFirst();
            users.removeFirst();
            listeners.removeFirst().onFailure(e);
        }
    }

    private static BulkResponse response(BulkRequest bulk, int failedItem) {
        BulkItemResponse[] items = new BulkItemResponse[bulk.numberOfActions()];
        for (int i = 0; i < items.length; i++) {
            DocWriteRequest<?> item = bulk.requests().get(i);
            items[i] = i == failedItem
                ? BulkItemResponse.failure(
                    i,
                    item.opType(),
                    new BulkItemResponse.Failure(INDEX, item.id(), new IllegalArgumentException("boom"))
                )
                : BulkItemResponse.success(i, item.opType(), new IndexResponse(SHARD, item.id(), 0, 1, 1, true));
        }
        return new BulkResponse(items, 1);
    }

    private CircuitBreaker breaker(ByteSizeValue limit) {
        CircuitBreaker b = newLimitedBreaker(limit);
        breakers.add(b);
        return b;
    }

    private EscfBulkService service(long coordinatingLimit) {
        return service(threadPool.generic(), coordinatingLimit);
    }

    private EscfBulkService service(Executor executor, long coordinatingLimit) {
        IndexingPressure pressure = new IndexingPressure(
            Settings.builder().put(IndexingPressure.MAX_COORDINATING_BYTES.getKey(), coordinatingLimit + "b").build()
        );
        pressures.add(pressure);
        return new EscfBulkService(pressure, executor, recycler);
    }

    private static long coordinatingBytes(EscfBulkService service) {
        return service.indexingPressure().stats().getCurrentCoordinatingBytes();
    }

    private EscfBulkWriter writer(EscfBulkService service, EscfBulkWriter.Policy policy) {
        return service.newWriter(client, breaker, INDEX, policy, FAIL_ON_FAILED_ITEM, capacitySignals::incrementAndGet);
    }

    private EscfBulkWriter writer(EscfBulkWriter.Policy policy) {
        return writer(service(UNBOUNDED), policy);
    }

    /** Answers the oldest bulk and runs what the writers do next. */
    private void answer() {
        client.answer();
        taskQueue.runAllRunnableTasks();
    }

    /** The result of {@code done}, which must have completed. */
    private static <T> T completed(PlainActionFuture<T> done) {
        assertTrue("not completed", done.isDone());
        return done.actionGet();
    }

    /** {@code count} rows, each holding its position from {@code first}. */
    static EscfBulkWriter.Rows page(int first, int count) {
        return new EscfBulkWriter.Rows() {
            @Override
            public int count() {
                return count;
            }

            @Override
            public void append(EscfBatchBuilder batch) {
                for (int i = 0; i < count; i++) {
                    EscfRowBuffer row = batch.beginRow();
                    row.longField("value", first + i);
                    row.finishRow();
                    batch.commit(0);
                }
            }

            @Override
            public IndexRequest request(int row) {
                return new IndexRequest(INDEX).id("id-" + (first + row)).opType(DocWriteRequest.OpType.CREATE);
            }
        };
    }

    /** One row holding an array of two values, under the same field as {@link #page}. */
    static EscfBulkWriter.Rows arrayRow(int id) {
        return new EscfBulkWriter.Rows() {
            @Override
            public int count() {
                return 1;
            }

            @Override
            public void append(EscfBatchBuilder batch) {
                long[] values = new long[] { id, id + 1 };
                byte[] packed = SourceBatchEncodeHelper.packFixedArray(SourceValueType.LONG, values, new Object[2], 2);
                EscfRowBuffer row = batch.beginRow();
                row.arrayField("value", SourceValueType.FIXED_ARRAY, packed);
                row.finishRow();
                batch.commit(0);
            }

            @Override
            public IndexRequest request(int row) {
                return new IndexRequest(INDEX).id("id-" + id).opType(DocWriteRequest.OpType.CREATE);
            }
        };
    }

    private static List<String> ids(BulkRequest bulk) {
        return bulk.requests().stream().map(DocWriteRequest::id).toList();
    }

    public void testPolicy() {
        expectThrows(IllegalArgumentException.class, () -> new EscfBulkWriter.Policy(0, ONE_MB));
        expectThrows(IllegalArgumentException.class, () -> new EscfBulkWriter.Policy(1, 0));
        expectThrows(IllegalArgumentException.class, () -> new EscfBulkWriter.Policy(1, MAX_TARGET + 1));
    }

    public void testDefaultPolicyFollowsIndexingPressure() {
        assertThat(defaultPolicy("20mb", "4mb", "1gb"), equalTo(new EscfBulkWriter.Policy(5, ONE_MB * 4, ONE_MB * 20)));
        // a large heap still keeps few bulks in flight until rejected bulks are retried
        assertThat(
            defaultPolicy("2gb", "4mb", "8gb"),
            equalTo(new EscfBulkWriter.Policy(EscfBulkWriter.Policy.MAX_DEFAULT_BULKS_IN_FLIGHT, ONE_MB * 4, ONE_MB * 2048))
        );
        // half the coordinating limit when it is below the watermark
        assertThat(defaultPolicy("64mb", "4mb", "8mb"), equalTo(new EscfBulkWriter.Policy(1, ONE_MB * 4, ONE_MB * 4)));
        // batches shrink to a budget below one batch
        assertThat(defaultPolicy("1mb", "4mb", "1gb"), equalTo(new EscfBulkWriter.Policy(1, ONE_MB, ONE_MB)));
        // batches stay within what a writer may build
        assertThat(
            defaultPolicy("1gb", "1gb", "4gb"),
            equalTo(new EscfBulkWriter.Policy(EscfBulkWriter.Policy.MAX_DEFAULT_BULKS_IN_FLIGHT, MAX_TARGET, ONE_MB * 1024))
        );
    }

    private static EscfBulkWriter.Policy defaultPolicy(String lowWatermark, String lowWatermarkSize, String coordinatingLimit) {
        IndexingPressure pressure = new IndexingPressure(
            Settings.builder()
                .put(IndexingPressure.SPLIT_BULK_LOW_WATERMARK.getKey(), lowWatermark)
                .put(IndexingPressure.SPLIT_BULK_LOW_WATERMARK_SIZE.getKey(), lowWatermarkSize)
                .put(IndexingPressure.MAX_COORDINATING_BYTES.getKey(), coordinatingLimit)
                .build()
        );
        return new EscfBulkService(pressure, Runnable::run, BytesRefRecycler.NON_RECYCLING_INSTANCE).defaultPolicy();
    }

    public void testOpenBatchesCountTowardsTheNodeLimit() {
        long oneRow;
        EscfBulkService measure = service(UNBOUNDED);
        try (EscfBulkWriter.Producer producer = writer(measure, new EscfBulkWriter.Policy(1, MAX_TARGET)).newProducer()) {
            producer.add(page(0, 1), false);
            oneRow = coordinatingBytes(measure);
        }
        answer();
        EscfBulkService service = service(UNBOUNDED);
        // free slots and batches that never fill: only the node's usage holds the producers
        EscfBulkWriter writer = writer(service, new EscfBulkWriter.Policy(8, MAX_TARGET, 2 * oneRow + oneRow / 2));
        List<EscfBulkWriter.Producer> producers = List.of(writer.newProducer(), writer.newProducer(), writer.newProducer());
        for (int i = 0; i < producers.size(); i++) {
            assertTrue(producers.get(i).hasCapacity());
            producers.get(i).add(page(i, 1), false);
        }
        // the row that reached the limit gave its batch, which waits since one bulk is in flight
        assertThat(client.bulks, hasSize(1));
        assertThat(coordinatingBytes(service), equalTo(3 * oneRow));
        for (EscfBulkWriter.Producer producer : producers) {
            assertFalse(producer.hasCapacity());
        }
        // two rows are below the limit
        answer();
        assertThat(ids(client.bulks.getFirst()), equalTo(List.of("id-2")));
        assertTrue(producers.get(0).hasCapacity());
        // the idle writer takes the batch left open
        answer();
        assertThat(ids(client.bulks.getFirst()), equalTo(List.of("id-1")));
        answer();
        Releasables.close(producers);
    }

    public void testNodeUsageLeavesOneBulkInFlight() {
        EscfBulkService service = service(UNBOUNDED);
        EscfBulkWriter writer = writer(service, new EscfBulkWriter.Policy(8, MAX_TARGET, ONE_MB));
        // primary and replica bulks of the node take the writer's share together, neither alone
        IndexingPressure pressure = service.indexingPressure();
        Releasable primary = pressure.validateAndMarkPrimaryOperationStarted(1, ONE_MB / 2, 0, true, false);
        Releasable replica = pressure.markReplicaOperationStarted(1, ONE_MB / 2, true);
        EscfBulkWriter.Producer first = writer.newProducer();
        EscfBulkWriter.Producer second = writer.newProducer();
        // both pass while the writer is idle, but only one bulk goes
        assertTrue(first.hasCapacity());
        assertTrue(second.hasCapacity());
        first.add(page(0, 1), false);
        second.add(page(1, 1), false);
        assertThat(client.bulks, hasSize(1));
        assertFalse(first.hasCapacity());
        answer();
        assertThat(client.bulks, hasSize(1));
        answer();
        assertTrue(first.hasCapacity());
        Releasables.close(first, second, primary, replica);
    }

    public void testRowsGoToTheBatchInOrder() {
        try (EscfBulkWriter.Producer producer = writer(new EscfBulkWriter.Policy(1, MAX_TARGET)).newProducer()) {
            producer.add(page(0, 3), false);
        }
        BulkRequest bulk = client.bulks.getFirst();
        assertThat(ids(bulk), equalTo(List.of("id-0", "id-1", "id-2")));
        for (int i = 0; i < 3; i++) {
            assertThat(((IndexRequest) bulk.requests().get(i)).indexSource().rowIndex(), equalTo(i));
        }
        SourceBatch batch = bulk.takePreBuiltBatches().get(INDEX);
        assertThat(batch, instanceOf(EscfBatch.class));
        assertThat(batch.docCount(), equalTo(3));
        batch.close();
    }

    /**
     * A field can hold an array in one row and a scalar in the next. The rows go out in one bulk, and what the writer charges
     * covers the batch that bulk holds until it is acknowledged.
     */
    public void testFieldChangingKindAcrossRows() {
        try (EscfBulkWriter.Producer producer = writer(new EscfBulkWriter.Policy(1, MAX_TARGET)).newProducer()) {
            producer.add(page(0, 1), false);
            // while that bulk is in flight, the next rows share a batch
            producer.add(arrayRow(1), false);
            producer.add(page(2, 1), false);
        }
        answer();
        BulkRequest bulk = client.bulks.getFirst();
        assertThat(ids(bulk), equalTo(List.of("id-1", "id-2")));
        SourceBatch batch = bulk.takePreBuiltBatches().get(INDEX);
        try {
            assertThat(batch.docCount(), equalTo(2));
            assertThat(breaker.getUsed(), greaterThanOrEqualTo(((EscfBatch) batch).ramBytesUsed()));
        } finally {
            batch.close();
        }
        answer();
    }

    public void testChargesTheBuilderAndTheRequests() throws IOException {
        EscfBulkService service = service(UNBOUNDED);
        // indexing pressure also counts the context the bulk carries, as the bulk action does
        long contextBytes;
        EscfBulkWriter writer;
        try (var ignored = threadPool.getThreadContext().stashContext()) {
            threadPool.getThreadContext().putHeader(USER, "user");
            contextBytes = threadPool.getThreadContext().estimatedRequestContextBytes();
            writer = writer(service, new EscfBulkWriter.Policy(1, MAX_TARGET));
        }
        assertThat(contextBytes, greaterThan(0L));
        EscfBulkWriter.Rows rows = page(0, between(1, 100));
        long expected;
        try (EscfBatchBuilder reference = new EscfBatchBuilder(recycler)) {
            rows.append(reference);
            expected = reference.ramBytesUsed();
        }
        for (int i = 0; i < rows.count(); i++) {
            expected += rows.request(i).ramBytesUsed();
        }
        try (EscfBulkWriter.Producer producer = writer.newProducer()) {
            producer.add(rows, false);
            assertThat(breaker.getUsed(), equalTo(expected));
            assertThat(coordinatingBytes(service), equalTo(expected + contextBytes));
            assertThat(service.indexingPressure().stats().getCurrentCoordinatingOps(), equalTo((long) rows.count()));
        }
        // so that the bulk action does not count the rows again
        assertTrue(client.bulks.getFirst().incrementalState().indexingPressureAccounted());
        answer();
        assertThat(breaker.getUsed(), equalTo(0L));
        assertThat(coordinatingBytes(service), equalTo(0L));
    }

    public void testIndexingPressureCountsTheContextOncePerBulk() {
        EscfBulkService service = service(UNBOUNDED);
        long contextBytes;
        EscfBulkWriter writer;
        try (var ignored = threadPool.getThreadContext().stashContext()) {
            threadPool.getThreadContext().putHeader(USER, "user");
            contextBytes = threadPool.getThreadContext().estimatedRequestContextBytes();
            writer = writer(service, new EscfBulkWriter.Policy(1, MAX_TARGET));
        }
        assertThat(contextBytes, greaterThan(0L));
        try (EscfBulkWriter.Producer producer = writer.newProducer()) {
            producer.add(page(0, 1), false);
            // two pages for the next bulk
            producer.add(page(1, 1), false);
            producer.add(page(2, 1), false);
            assertThat(coordinatingBytes(service), equalTo(breaker.getUsed() + 2 * contextBytes));
        }
        answer();
        answer();
    }

    public void testPagesCoalesceWhileABulkIsInFlight() {
        try (EscfBulkWriter.Producer producer = writer(new EscfBulkWriter.Policy(1, MAX_TARGET)).newProducer()) {
            producer.add(page(0, 1), false);
            producer.add(page(1, 2), false);
            producer.add(page(3, 2), false);
            assertThat(client.bulks, hasSize(1));

            answer();
            assertThat(ids(client.bulks.getFirst()), equalTo(List.of("id-1", "id-2", "id-3", "id-4")));
        }
    }

    public void testEachProducerCoalescesItsOwnPages() {
        EscfBulkWriter writer = writer(new EscfBulkWriter.Policy(1, MAX_TARGET));
        EscfBulkWriter.Producer first = writer.newProducer();
        EscfBulkWriter.Producer second = writer.newProducer();
        first.add(page(0, 1), false);
        for (int i = 0; i < 3; i++) {
            first.add(page(10 + i, 1), false);
            second.add(page(20 + i, 1), false);
        }
        answer();
        assertThat(ids(client.bulks.getFirst()), equalTo(List.of("id-10", "id-11", "id-12")));
        answer();
        assertThat(ids(client.bulks.getFirst()), equalTo(List.of("id-20", "id-21", "id-22")));
        first.close();
        second.close();
    }

    public void testTheBatchOfAnIdleProducerIsSentWhenBulksSettle() {
        EscfBulkWriter writer = writer(new EscfBulkWriter.Policy(1, MAX_TARGET));
        EscfBulkWriter.Producer busy = writer.newProducer();
        EscfBulkWriter.Producer idle = writer.newProducer();
        busy.add(page(0, 1), false);
        // the producer keeps its batch while a bulk is in flight, then adds nothing more
        idle.add(page(1, 1), false);
        answer();
        assertThat(ids(client.bulks.getFirst()), equalTo(List.of("id-1")));
        busy.close();
        idle.close();
    }

    public void testFullBatchWaitsForAFreeSlot() {
        // every page fills a batch
        try (EscfBulkWriter.Producer producer = writer(new EscfBulkWriter.Policy(2, 1)).newProducer()) {
            producer.add(page(0, 1), false);
            producer.add(page(1, 1), false);
            assertThat(client.bulks, hasSize(2));

            producer.add(page(2, 1), false);
            assertThat(client.bulks, hasSize(2));
            assertFalse(producer.hasCapacity());

            answer();
            assertThat(capacitySignals.get(), greaterThan(0));
            assertThat(ids(client.bulks.get(1)), equalTo(List.of("id-2")));
            assertTrue(producer.hasCapacity());
        }
    }

    public void testOneBulkInFlightKeepsOrder() {
        try (EscfBulkWriter.Producer producer = writer(new EscfBulkWriter.Policy(1, 1)).newProducer()) {
            for (int i = 0; i < 3; i++) {
                producer.add(page(i, 1), false);
            }
        }
        List<String> sent = new ArrayList<>();
        while (client.bulks.isEmpty() == false) {
            assertThat(client.bulks, hasSize(1));
            sent.addAll(ids(client.bulks.getFirst()));
            answer();
        }
        assertThat(sent, equalTo(List.of("id-0", "id-1", "id-2")));
    }

    public void testNewBulk() {
        try (EscfBulkWriter.Producer producer = writer(new EscfBulkWriter.Policy(1, MAX_TARGET)).newProducer()) {
            producer.add(page(0, 1), false);
            producer.add(page(1, 1), false);
            producer.add(page(1, 1), true);
        }
        assertThat(ids(client.bulks.getFirst()), equalTo(List.of("id-0")));
        answer();
        assertThat(ids(client.bulks.getFirst()), equalTo(List.of("id-1")));
        answer();
        assertThat(ids(client.bulks.getFirst()), equalTo(List.of("id-1")));
    }

    public void testEmptyRows() {
        EscfBulkWriter writer = writer(new EscfBulkWriter.Policy(1, MAX_TARGET));
        try (EscfBulkWriter.Producer producer = writer.newProducer()) {
            producer.add(page(0, 0), false);
        }
        assertNull(writer.failure());
        assertThat(client.bulks, empty());
    }

    public void testFinishWithNothingToSend() {
        PlainActionFuture<Void> done = new PlainActionFuture<>();
        writer(new EscfBulkWriter.Policy(1, MAX_TARGET)).finish(done);
        assertNull(safeGet(done));
    }

    public void testFinishOnlyOnce() {
        EscfBulkWriter writer = writer(new EscfBulkWriter.Policy(1, MAX_TARGET));
        EscfBulkWriter.Producer producer = writer.newProducer();
        PlainActionFuture<Void> done = new PlainActionFuture<>();
        writer.finish(done);
        expectThrows(IllegalStateException.class, () -> writer.finish(new PlainActionFuture<>()));
        // the first listener still completes
        producer.close();
        assertNull(safeGet(done));
        expectThrows(IllegalStateException.class, () -> writer.finish(new PlainActionFuture<>()));
    }

    public void testFinishWaitsForTheProducersAndTheBulks() {
        EscfBulkWriter writer = writer(new EscfBulkWriter.Policy(1, MAX_TARGET));
        EscfBulkWriter.Producer producer = writer.newProducer();
        PlainActionFuture<Void> done = new PlainActionFuture<>();
        writer.finish(done);
        producer.add(page(0, 1), false);
        assertFalse(done.isDone());
        producer.close();
        assertFalse(done.isDone());
        answer();
        assertNull(safeGet(done));
    }

    public void testBulksSettleOffTheThreadThatCompletesThem() {
        try (EscfBulkWriter.Producer producer = writer(new EscfBulkWriter.Policy(1, 1)).newProducer()) {
            producer.add(page(0, 1), false);
            producer.add(page(1, 1), false);
            long charged = breaker.getUsed();
            client.answer();
            // nothing happened on the completing thread
            assertThat(breaker.getUsed(), equalTo(charged));
            assertThat(client.bulks, empty());
            taskQueue.runAllRunnableTasks();
            assertThat(ids(client.bulks.getFirst()), equalTo(List.of("id-1")));
        }
    }

    public void testBulksAndCallbacksRunInTheContextOfTheirWriter() {
        ThreadContext threadContext = threadPool.getThreadContext();
        EscfBulkService service = service(UNBOUNDED);
        List<String> callbackUsers = Collections.synchronizedList(new ArrayList<>());
        Runnable recordUser = () -> callbackUsers.add(threadContext.getHeader(USER));
        List<String> handlerUsers = Collections.synchronizedList(new ArrayList<>());
        EscfBulkWriter.ResponseHandler handler = response -> {
            handlerUsers.add(threadContext.getHeader(USER));
            return FAIL_ON_FAILED_ITEM.onResponse(response);
        };
        // every page fills a batch
        EscfBulkWriter.Policy policy = new EscfBulkWriter.Policy(1, 1);
        EscfBulkWriter first;
        EscfBulkWriter second;
        try (var ignored = threadContext.stashContext()) {
            threadContext.putHeader(USER, "first");
            first = service.newWriter(client, breaker, INDEX, policy, handler, recordUser);
        }
        try (var ignored = threadContext.stashContext()) {
            threadContext.putHeader(USER, "second");
            second = service.newWriter(client, breaker, INDEX, policy, handler, recordUser);
        }
        EscfBulkWriter.Producer firstProducer = first.newProducer();
        EscfBulkWriter.Producer secondProducer = second.newProducer();
        firstProducer.add(page(0, 1), false);
        secondProducer.add(page(1, 1), false);
        // each writer has a batch waiting for its slot
        firstProducer.add(page(2, 1), false);
        secondProducer.add(page(3, 1), false);
        assertThat(client.users, equalTo(List.of("first", "second")));

        // the bulks settle from another context
        try (var ignored = threadContext.stashContext()) {
            threadContext.putHeader(USER, "other");
            client.answer();
            taskQueue.runAllRunnableTasks();
            client.answer();
            taskQueue.runAllRunnableTasks();
        }
        assertThat(callbackUsers, equalTo(List.of("first", "second")));
        assertThat(handlerUsers, equalTo(List.of("first", "second")));
        assertThat(client.users, equalTo(List.of("first", "second")));
        assertThat(ids(client.bulks.get(0)), equalTo(List.of("id-2")));
        assertThat(ids(client.bulks.get(1)), equalTo(List.of("id-3")));
        firstProducer.close();
        secondProducer.close();
    }

    public void testIndexingPressureRejectionSendsEveryBatchAndHoldsProducers() {
        EscfBulkWriter.Policy policy = new EscfBulkWriter.Policy(3, MAX_TARGET);
        long oneBatch;
        EscfBulkService unbounded = service(UNBOUNDED);
        try (EscfBulkWriter.Producer producer = writer(unbounded, policy).newProducer()) {
            producer.add(page(0, 1), false);
            oneBatch = coordinatingBytes(unbounded);
        }
        answer();
        // fits two batches, not three
        EscfBulkService service = service(2 * oneBatch + oneBatch / 2);

        EscfBulkWriter writer = writer(service, policy);
        EscfBulkWriter.Producer first = writer.newProducer();
        EscfBulkWriter.Producer second = writer.newProducer();
        first.add(page(0, 1), false);
        // both keep their batch while a bulk is in flight
        second.add(page(1, 1), false);
        first.add(page(2, 1), false);

        // rejected: the rows are counted anyway, and the batches of both producers go
        assertNull(writer.failure());
        assertThat(service.indexingPressure().stats().getCoordinatingRejections(), equalTo(1L));
        assertThat(coordinatingBytes(service), equalTo(3 * oneBatch));
        assertThat(client.bulks, hasSize(3));
        assertThat(ids(client.bulks.get(1)), equalTo(List.of("id-2")));
        assertThat(ids(client.bulks.get(2)), equalTo(List.of("id-1")));
        assertFalse(first.hasCapacity());
        assertFalse(second.hasCapacity());

        // the producers go on once the rows that were rejected are acknowledged
        answer();
        assertFalse(first.hasCapacity());
        answer();
        assertTrue(first.hasCapacity());
        assertTrue(second.hasCapacity());
        first.close();
        second.close();
        answer();
    }

    public void testIndexingPressureRejectionWhileHoldingNoRowsFailsTheWriter() {
        long limit = ONE_MB;
        EscfBulkService service = service(limit);
        // other bulks of the node take the room
        Releasable others = service.indexingPressure().markCoordinatingOperationStarted(1, limit, true);
        EscfBulkWriter writer = writer(service, new EscfBulkWriter.Policy(1, MAX_TARGET));
        try (EscfBulkWriter.Producer producer = writer.newProducer()) {
            producer.add(page(0, 1), false);
        }
        assertThat(writer.failure(), instanceOf(EsRejectedExecutionException.class));
        assertThat(client.bulks, empty());
        // nothing stays charged to the breaker either
        assertThat(breaker.getUsed(), equalTo(0L));
        others.close();
    }

    public void testFailedItemFailsTheWriter() {
        EscfBulkService service = service(UNBOUNDED);
        EscfBulkWriter writer = writer(service, new EscfBulkWriter.Policy(1, 1));
        try (EscfBulkWriter.Producer producer = writer.newProducer()) {
            producer.add(page(0, 1), false);
            producer.add(page(1, 1), false);
            client.answer(0);
            taskQueue.runAllRunnableTasks();

            // the rows not sent are dropped, and the producers go on with their rows dropped too
            assertThat(client.bulks, empty());
            assertThat(writer.failure().getMessage(), equalTo("boom"));
            assertThat(coordinatingBytes(service), equalTo(0L));
            assertTrue(producer.hasCapacity());
            producer.add(page(2, 1), false);
            assertThat(client.bulks, empty());
        }
        PlainActionFuture<Void> done = new PlainActionFuture<>();
        writer.finish(done);
        assertThat(expectThrows(IllegalArgumentException.class, () -> completed(done)).getMessage(), equalTo("boom"));
    }

    public void testResponseHandlerChoosesWhichFailedItemsFailTheWriter() {
        List<Integer> failedItems = Collections.synchronizedList(new ArrayList<>());
        EscfBulkWriter.ResponseHandler handler = response -> {
            for (BulkItemResponse item : response) {
                if (item.isFailed()) {
                    failedItems.add(item.getItemId());
                }
            }
            return failedItems.size() > 1 ? new IllegalStateException("too many failures") : null;
        };
        EscfBulkWriter writer = service(UNBOUNDED).newWriter(
            client,
            breaker,
            INDEX,
            new EscfBulkWriter.Policy(1, 1),
            handler,
            capacitySignals::incrementAndGet
        );
        try (EscfBulkWriter.Producer producer = writer.newProducer()) {
            producer.add(page(0, 2), false);
            producer.add(page(2, 2), false);

            // the first failed item does not fail the writer, which sends the next bulk
            client.answer(1);
            taskQueue.runAllRunnableTasks();
            assertThat(writer.failure(), nullValue());
            assertThat(ids(client.bulks.getFirst()), equalTo(List.of("id-2", "id-3")));

            // the second one does, and the rows not sent are dropped
            client.answer(0);
            taskQueue.runAllRunnableTasks();
            producer.add(page(4, 1), false);
            assertThat(client.bulks, empty());
        }
        PlainActionFuture<Void> done = new PlainActionFuture<>();
        writer.finish(done);
        assertThat(expectThrows(IllegalStateException.class, () -> completed(done)).getMessage(), equalTo("too many failures"));
        assertThat(failedItems, equalTo(List.of(1, 0)));
    }

    public void testThrowingResponseHandlerFailsTheWriter() {
        EscfBulkWriter writer = service(UNBOUNDED).newWriter(client, breaker, INDEX, new EscfBulkWriter.Policy(1, 1), response -> {
            throw new IllegalStateException("handler");
        }, capacitySignals::incrementAndGet);
        try (EscfBulkWriter.Producer producer = writer.newProducer()) {
            producer.add(page(0, 1), false);
            producer.add(page(1, 1), false);
            answer();
        }
        PlainActionFuture<Void> done = new PlainActionFuture<>();
        writer.finish(done);
        assertThat(client.bulks, empty());
        assertThat(expectThrows(IllegalStateException.class, () -> completed(done)).getMessage(), equalTo("handler"));
    }

    public void testBulkFailingRightAwayFailsTheWriter() {
        // as a client that fails a bulk on the calling thread, before sending it
        NoOpClient failing = new NoOpClient(threadPool) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                listener.onFailure(new IllegalStateException("cannot send"));
            }
        };
        EscfBulkWriter writer = service(UNBOUNDED).newWriter(
            failing,
            breaker,
            INDEX,
            new EscfBulkWriter.Policy(1, MAX_TARGET),
            FAIL_ON_FAILED_ITEM,
            capacitySignals::incrementAndGet
        );
        try (EscfBulkWriter.Producer producer = writer.newProducer()) {
            producer.add(page(0, 1), false);
        }
        PlainActionFuture<Void> done = new PlainActionFuture<>();
        writer.finish(done);
        taskQueue.runAllRunnableTasks();
        assertThat(expectThrows(IllegalStateException.class, () -> completed(done)).getMessage(), equalTo("cannot send"));
    }

    public void testRejectedBulkFailsTheWriter() {
        EscfBulkWriter writer = writer(new EscfBulkWriter.Policy(1, 1));
        try (EscfBulkWriter.Producer producer = writer.newProducer()) {
            producer.add(page(0, 1), false);
            producer.add(page(1, 1), false);
        }
        PlainActionFuture<Void> done = new PlainActionFuture<>();
        writer.finish(done);

        // rejected before the bulk action took the batch, which the writer releases
        client.fail(new EsRejectedExecutionException("rejected"));
        taskQueue.runAllRunnableTasks();
        assertThat(client.bulks, empty());
        assertThat(expectThrows(EsRejectedExecutionException.class, () -> completed(done)).getMessage(), equalTo("rejected"));
    }

    public void testClose() {
        EscfBulkService service = service(UNBOUNDED);
        EscfBulkWriter writer = writer(service, new EscfBulkWriter.Policy(1, MAX_TARGET));
        EscfBulkWriter.Producer producer = writer.newProducer();
        producer.add(page(0, 1), false);
        long inFlight = coordinatingBytes(service);
        producer.add(page(1, 1), false);
        PlainActionFuture<Void> done = new PlainActionFuture<>();
        writer.finish(done);

        // drops the producer's batch and calls the producers back
        writer.close();
        taskQueue.runAllRunnableTasks();
        assertThat(coordinatingBytes(service), equalTo(inFlight));
        assertThat(capacitySignals.get(), greaterThan(0));
        assertTrue(producer.hasCapacity());

        answer();
        assertThat(client.bulks, empty());
        assertFalse(done.isDone());
        producer.close();
        expectThrows(IllegalStateException.class, () -> completed(done));
    }

    public void testCloseOnceDoneDoesNothing() {
        EscfBulkWriter writer = writer(new EscfBulkWriter.Policy(1, MAX_TARGET));
        try (EscfBulkWriter.Producer producer = writer.newProducer()) {
            producer.add(page(0, 1), false);
        }
        PlainActionFuture<Void> done = new PlainActionFuture<>();
        writer.finish(done);
        answer();
        assertNull(safeGet(done));
        int signals = capacitySignals.get();

        writer.close();
        taskQueue.runAllRunnableTasks();
        assertThat(capacitySignals.get(), equalTo(signals));
        assertThat(writer.failure(), nullValue());
    }

    public void testThrowingCallbackFailsTheWriter() {
        EscfBulkService service = service(UNBOUNDED);
        EscfBulkWriter writer = service.newWriter(client, breaker, INDEX, new EscfBulkWriter.Policy(1, 1), FAIL_ON_FAILED_ITEM, () -> {
            throw new IllegalStateException("callback");
        });
        try (EscfBulkWriter.Producer producer = writer.newProducer()) {
            producer.add(page(0, 1), false);
            producer.add(page(1, 1), false);
            answer();
        }
        assertThat(writer.failure().getMessage(), equalTo("callback"));
        assertThat(client.bulks, empty());
    }

    public void testTripHoldsProducersUntilTheirRowsAreSent() {
        EscfBulkService service = service(UNBOUNDED);
        EscfBulkWriter.Policy policy = new EscfBulkWriter.Policy(1, 1);
        long oneBatch;
        try (EscfBulkWriter.Producer producer = writer(service, policy).newProducer()) {
            producer.add(page(0, 1), false);
            oneBatch = breaker.getUsed();
        }
        answer();
        // fits two batches, not three, and trips permanently, as a query's own limit does
        breaker = breaker(2 * oneBatch + oneBatch / 2, CircuitBreaker.Durability.PERMANENT);

        EscfBulkWriter writer = writer(service, policy);
        EscfBulkWriter.Producer first = writer.newProducer();
        EscfBulkWriter.Producer second = writer.newProducer();
        first.add(page(0, 1), false);
        second.add(page(1, 1), false);
        // trips: the batch waits behind the second producer's
        first.add(page(2, 1), false);
        assertNull(writer.failure());
        assertFalse(first.hasCapacity());

        // the bulk ahead of the rows that tripped settles, then the second producer's
        answer();
        assertFalse(first.hasCapacity());
        answer();
        assertFalse(first.hasCapacity());
        assertThat(ids(client.bulks.getFirst()), equalTo(List.of("id-2")));
        answer();
        assertTrue(first.hasCapacity());
        first.close();
        second.close();
    }

    public void testTripWhileHoldingNoRowsFailsTheWriter() {
        long limit = ONE_MB;
        breaker = breaker(limit, CircuitBreaker.Durability.TRANSIENT);
        EscfBulkWriter writer = writer(new EscfBulkWriter.Policy(1, MAX_TARGET));
        EscfBulkWriter.Producer producer = writer.newProducer();
        producer.add(page(0, 1), false);
        answer();
        assertNull(writer.failure());

        // the rest of the task takes the memory once the writer holds nothing
        breaker.addWithoutBreaking(limit);
        producer.add(page(1, 1), false);
        assertThat(writer.failure(), instanceOf(CircuitBreakingException.class));
        assertThat(client.bulks, empty());
        producer.close();
        breaker.addWithoutBreaking(-limit);
    }

    public void testProducersTrippingOnEachOthersRowsWait() throws Exception {
        EscfBulkWriter.Policy policy = new EscfBulkWriter.Policy(1, MAX_TARGET);
        long oneBatch;
        try (EscfBulkWriter.Producer producer = writer(policy).newProducer()) {
            producer.add(page(0, 1), false);
            oneBatch = breaker.getUsed();
        }
        answer();
        // fits one batch, and holds the first charge until the second one tripped on it
        CountDownLatch charged = new CountDownLatch(1);
        CountDownLatch tripped = new CountDownLatch(1);
        breaker = new LimitedBreaker(oneBatch + oneBatch / 2, CircuitBreaker.Durability.TRANSIENT) {
            @Override
            public void addEstimateBytesAndMaybeBreak(long bytes, String label) {
                if (charged.getCount() > 0) {
                    super.addEstimateBytesAndMaybeBreak(bytes, label);
                    charged.countDown();
                    safeAwait(tripped);
                    return;
                }
                try {
                    super.addEstimateBytesAndMaybeBreak(bytes, label);
                } finally {
                    tripped.countDown();
                }
            }
        };
        breakers.add(breaker);

        EscfBulkWriter writer = writer(policy);
        EscfBulkWriter.Producer first = writer.newProducer();
        EscfBulkWriter.Producer second = writer.newProducer();
        Thread adding = new Thread(() -> first.add(page(1, 1), false));
        adding.start();
        safeAwait(charged);
        // trips on the rows the first producer is adding, which the writer will send
        second.add(page(2, 1), false);
        adding.join(TimeUnit.SECONDS.toMillis(30));
        assertFalse(adding.isAlive());

        assertNull(writer.failure());
        assertFalse(second.hasCapacity());
        first.close();
        second.close();
        while (client.bulks.isEmpty() == false) {
            answer();
        }
        assertTrue(second.hasCapacity());
        assertNull(writer.failure());
    }

    public void testTripOnRowsBeingAcknowledgedDoesNotFailTheWriter() throws Exception {
        EscfBulkWriter.Policy policy = new EscfBulkWriter.Policy(1, MAX_TARGET);
        long oneBatch;
        try (EscfBulkWriter.Producer producer = writer(policy).newProducer()) {
            producer.add(page(0, 1), false);
            oneBatch = breaker.getUsed();
        }
        answer();
        // fits one batch; the first trip goes on once the bulk it tripped on settled on another thread
        CountDownLatch settled = new CountDownLatch(1);
        AtomicReference<Thread> settling = new AtomicReference<>();
        breaker = new LimitedBreaker(oneBatch + oneBatch / 2, CircuitBreaker.Durability.TRANSIENT) {
            @Override
            public void addEstimateBytesAndMaybeBreak(long bytes, String label) {
                try {
                    super.addEstimateBytesAndMaybeBreak(bytes, label);
                } catch (CircuitBreakingException e) {
                    if (settling.get() == null) {
                        Thread thread = new Thread(client::answer);
                        settling.set(thread);
                        thread.start();
                        safeAwait(settled);
                    }
                    throw e;
                }
            }
        };
        breakers.add(breaker);

        // settles on the thread that acknowledges
        EscfBulkWriter writer = service(Runnable::run, UNBOUNDED).newWriter(
            client,
            breaker,
            INDEX,
            policy,
            FAIL_ON_FAILED_ITEM,
            settled::countDown
        );
        EscfBulkWriter.Producer producer = writer.newProducer();
        producer.add(page(1, 1), false);
        // trips on the rows of the bulk in flight, which are acknowledged before the writer checks what it holds
        producer.add(page(2, 1), false);
        settling.get().join(TimeUnit.SECONDS.toMillis(30));
        assertFalse(settling.get().isAlive());

        assertNull(writer.failure());
        assertTrue(producer.hasCapacity());
        assertThat(ids(client.bulks.getFirst()), equalTo(List.of("id-2")));
        producer.close();
        answer();
    }

    public void testTripDuringAReleaseDoesNotFailTheWriter() throws Exception {
        EscfBulkWriter.Policy policy = new EscfBulkWriter.Policy(1, MAX_TARGET);
        long oneBatch;
        try (EscfBulkWriter.Producer producer = writer(policy).newProducer()) {
            producer.add(page(0, 1), false);
            oneBatch = breaker.getUsed();
        }
        answer();
        // fits one batch; the first trip goes on while the bulk it tripped on is releasing its memory on another thread
        CountDownLatch releasing = new CountDownLatch(1);
        CountDownLatch added = new CountDownLatch(1);
        AtomicReference<Thread> settling = new AtomicReference<>();
        breaker = new LimitedBreaker(oneBatch + oneBatch / 2, CircuitBreaker.Durability.TRANSIENT) {
            @Override
            public void addEstimateBytesAndMaybeBreak(long bytes, String label) {
                try {
                    super.addEstimateBytesAndMaybeBreak(bytes, label);
                } catch (CircuitBreakingException e) {
                    if (settling.get() == null) {
                        Thread thread = new Thread(client::answer);
                        settling.set(thread);
                        thread.start();
                        safeAwait(releasing);
                    }
                    throw e;
                }
            }

            @Override
            public void addWithoutBreaking(long bytes) {
                if (bytes < 0 && releasing.getCount() > 0) {
                    releasing.countDown();
                    safeAwait(added);
                }
                super.addWithoutBreaking(bytes);
            }
        };
        breakers.add(breaker);

        // settles on the thread that acknowledges
        EscfBulkWriter writer = service(Runnable::run, UNBOUNDED).newWriter(client, breaker, INDEX, policy, FAIL_ON_FAILED_ITEM, () -> {});
        EscfBulkWriter.Producer producer = writer.newProducer();
        producer.add(page(1, 1), false);
        // trips on the rows of the bulk in flight, whose memory is not free yet: they still count as held
        producer.add(page(2, 1), false);
        added.countDown();
        settling.get().join(TimeUnit.SECONDS.toMillis(30));
        assertFalse(settling.get().isAlive());

        assertNull(writer.failure());
        assertFalse(producer.hasCapacity());
        assertThat(ids(client.bulks.getFirst()), equalTo(List.of("id-2")));
        client.answer();
        assertTrue(producer.hasCapacity());
        producer.close();
    }

    private CircuitBreaker breaker(long limit, CircuitBreaker.Durability durability) {
        CircuitBreaker b = new LimitedBreaker(limit, durability);
        breakers.add(b);
        return b;
    }

    /**
     * A breaker that trips past {@code limit} with {@code durability}.
     */
    private static class LimitedBreaker extends NoopCircuitBreaker {
        private final long limit;
        private final Durability durability;
        private final AtomicLong used = new AtomicLong();

        LimitedBreaker(long limit, Durability durability) {
            super(CircuitBreaker.REQUEST);
            this.limit = limit;
            this.durability = durability;
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) {
            if (used.addAndGet(bytes) > limit && bytes > 0) {
                used.addAndGet(-bytes);
                throw new CircuitBreakingException("no room", bytes, limit, durability);
            }
        }

        @Override
        public void addWithoutBreaking(long bytes) {
            used.addAndGet(bytes);
        }

        @Override
        public long getUsed() {
            return used.get();
        }

        @Override
        public long getLimit() {
            return limit;
        }
    }

    public void testFailureReleasesProducersHeldByATrip() {
        EscfBulkService service = service(UNBOUNDED);
        EscfBulkWriter.Policy policy = new EscfBulkWriter.Policy(1, 1);
        long oneBatch;
        try (EscfBulkWriter.Producer producer = writer(service, policy).newProducer()) {
            producer.add(page(0, 1), false);
            oneBatch = breaker.getUsed();
        }
        answer();
        breaker = breaker(ByteSizeValue.ofBytes(oneBatch + oneBatch / 2));

        EscfBulkWriter writer = writer(service, policy);
        EscfBulkWriter.Producer producer = writer.newProducer();
        producer.add(page(0, 1), false);
        // trips while the first bulk is in flight
        producer.add(page(1, 1), false);
        assertFalse(producer.hasCapacity());

        writer.close();
        taskQueue.runAllRunnableTasks();
        assertTrue(producer.hasCapacity());
        PlainActionFuture<Void> done = new PlainActionFuture<>();
        writer.finish(done);
        producer.close();
        answer();
        expectThrows(IllegalStateException.class, () -> completed(done));
    }

    public void testBulksStillSettleOnShutdown() {
        Executor shutDown = command -> { throw new EsRejectedExecutionException("shut down", true); };
        EscfBulkWriter writer = writer(service(shutDown, UNBOUNDED), new EscfBulkWriter.Policy(1, MAX_TARGET));
        try (EscfBulkWriter.Producer producer = writer.newProducer()) {
            producer.add(page(0, 1), false);
        }
        PlainActionFuture<Void> done = new PlainActionFuture<>();
        writer.finish(done);
        client.answer();
        assertNull(safeGet(done));
    }

    public void testAppendFailure() {
        EscfBulkService service = service(UNBOUNDED);
        EscfBulkWriter writer = writer(service, new EscfBulkWriter.Policy(1, MAX_TARGET));
        try (EscfBulkWriter.Producer producer = writer.newProducer()) {
            producer.add(page(0, 1), false);
            producer.add(page(1, 1), false);
            producer.add(new EscfBulkWriter.Rows() {
                @Override
                public int count() {
                    return 1;
                }

                @Override
                public void append(EscfBatchBuilder batch) throws IOException {
                    throw new IOException("unwritable");
                }

                @Override
                public IndexRequest request(int row) {
                    throw new AssertionError("no request for a row that failed");
                }
            }, false);
        }
        assertThat(writer.failure().getMessage(), equalTo("unwritable"));
        answer();
        assertThat(client.bulks, empty());
    }

    public void testRequestFailure() {
        EscfBulkService service = service(UNBOUNDED);
        EscfBulkWriter writer = writer(service, new EscfBulkWriter.Policy(1, MAX_TARGET));
        EscfBulkWriter.Rows rows = page(0, 2);
        try (EscfBulkWriter.Producer producer = writer.newProducer()) {
            producer.add(new EscfBulkWriter.Rows() {
                @Override
                public int count() {
                    return rows.count();
                }

                @Override
                public void append(EscfBatchBuilder batch) throws IOException {
                    rows.append(batch);
                }

                @Override
                public IndexRequest request(int row) {
                    if (row == 1) {
                        throw new IllegalStateException("no request");
                    }
                    return rows.request(row);
                }
            }, false);
        }
        assertThat(client.bulks, empty());
        assertThat(writer.failure().getMessage(), equalTo("no request"));
    }

    /**
     * Producers on their own threads, a client that answers on others, and a small breaker and indexing pressure that hold the
     * producers back. Each producer waits the way a caller does: it checks {@code hasCapacity()} under its lock, and
     * {@code onCapacity} takes that lock. Bulks may fail, the writer may be closed at any point, and {@code finish} may be called at
     * any point. Every row is indexed at most once, all of them when nothing fails, in order per producer with one bulk in flight,
     * and every byte is given back.
     */
    public void testConcurrentProducers() {
        ThreadPool realThreadPool = new TestThreadPool(getTestName());
        try {
            runConcurrentProducers(realThreadPool);
        } finally {
            ThreadPool.terminate(realThreadPool, 10, TimeUnit.SECONDS);
        }
    }

    private void runConcurrentProducers(ThreadPool threadPool) {
        boolean failBulks = randomBoolean();
        // the ids in the order they were indexed
        List<Integer> indexed = Collections.synchronizedList(new ArrayList<>());
        // a fake rather than the bulk action, so that the test controls the answers and sees every bulk
        NoOpClient answering = new NoOpClient(threadPool) {
            @Override
            @SuppressWarnings("unchecked")
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                BulkRequest bulk = (BulkRequest) request;
                boolean fail = failBulks && rarely();
                threadPool.generic().execute(() -> ActionListener.run((ActionListener<BulkResponse>) listener, l -> {
                    if (fail == false) {
                        for (DocWriteRequest<?> item : bulk.requests()) {
                            indexed.add(Integer.parseInt(item.id().substring("id-".length())));
                        }
                    }
                    l.onResponse(response(bulk, fail ? 0 : -1));
                }));
            }
        };
        breaker = breaker(ByteSizeValue.ofKb(between(32, 1024)));
        EscfBulkService service = service(threadPool.generic(), ByteSizeValue.ofKb(between(32, 512)).getBytes());
        Object lock = new Object();
        int producers = between(2, 8);
        int pages = between(10, 50);
        int rowsPerPage = between(1, 50);
        int maxBulksInFlight = between(1, producers);
        EscfBulkWriter writer = service.newWriter(
            answering,
            breaker,
            INDEX,
            new EscfBulkWriter.Policy(maxBulksInFlight, between(1, 64 * 1024)),
            FAIL_ON_FAILED_ITEM,
            () -> {
                synchronized (lock) {
                    lock.notifyAll();
                }
            }
        );
        List<EscfBulkWriter.Producer> handles = new ArrayList<>();
        for (int p = 0; p < producers; p++) {
            handles.add(writer.newProducer());
        }
        int closeAfterPage = randomBoolean() ? -1 : between(0, pages - 1);
        CountDownLatch finished = new CountDownLatch(1);
        AtomicReference<Exception> failure = new AtomicReference<>();
        // what is still charged when the writer says it is done, which must be nothing
        AtomicLong chargedWhenDone = new AtomicLong(-1);
        AtomicLong pressureWhenDone = new AtomicLong(-1);
        ActionListener<Void> done = ActionListener.runAfter(ActionListener.wrap(r -> {}, failure::set), () -> {
            chargedWhenDone.set(breaker.getUsed());
            pressureWhenDone.set(coordinatingBytes(service));
            finished.countDown();
        });
        boolean finishFirst = randomBoolean();
        if (finishFirst) {
            writer.finish(done);
        }
        startInParallel(producers, p -> {
            EscfBulkWriter.Producer producer = handles.get(p);
            for (int i = 0; i < pages; i++) {
                awaitCapacity(lock, producer);
                producer.add(page((p * pages + i) * rowsPerPage, rowsPerPage), false);
                if (p == 0 && i == closeAfterPage) {
                    writer.close();
                }
            }
            producer.close();
        });
        if (finishFirst == false) {
            writer.finish(done);
        }
        safeAwait(finished);
        boolean failed = closeAfterPage >= 0 || failBulks;
        if (failed == false) {
            assertNull(failure.get());
        }

        Set<Integer> distinct = new HashSet<>(indexed);
        assertThat("every row indexed at most once", distinct.size(), equalTo(indexed.size()));
        if (failed == false) {
            assertThat(indexed, hasSize(producers * pages * rowsPerPage));
        }
        if (maxBulksInFlight == 1) {
            int rowsPerProducer = pages * rowsPerPage;
            int[] last = new int[producers];
            Arrays.fill(last, -1);
            for (int id : indexed) {
                int p = id / rowsPerProducer;
                assertThat("in order per producer", id, greaterThan(last[p]));
                last[p] = id;
            }
        }
        assertThat(pressureWhenDone.get(), equalTo(0L));
        assertThat(chargedWhenDone.get(), equalTo(0L));
    }

    /**
     * Waits for capacity the way a caller does, bounded so that a writer that never calls back fails the test instead of
     * hanging it.
     */
    private static void awaitCapacity(Object lock, EscfBulkWriter.Producer producer) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        synchronized (lock) {
            while (producer.hasCapacity() == false) {
                long left = deadline - System.nanoTime();
                if (left <= 0) {
                    throw new AssertionError("no capacity and no callback");
                }
                try {
                    lock.wait(TimeUnit.NANOSECONDS.toMillis(left) + 1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
            }
        }
    }
}
