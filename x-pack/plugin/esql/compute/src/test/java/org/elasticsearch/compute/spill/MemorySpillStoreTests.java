/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.spill;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.compute.spill.spi.SpillObject;
import org.elasticsearch.compute.spill.spi.SpillRead;
import org.elasticsearch.compute.spill.spi.SpillReader;
import org.elasticsearch.compute.spill.spi.SpillScope;
import org.elasticsearch.compute.spill.spi.SpillStore;
import org.elasticsearch.compute.spill.spi.SpillWriter;
import org.elasticsearch.compute.test.spill.MemorySpillStore;
import org.elasticsearch.compute.test.spill.SpillStoreConformanceTestCase;
import org.elasticsearch.tasks.TaskCancelledException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executor;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;

/**
 * The conformance suite against the memory store in a random completion mode, plus the cases only a store with held
 * completions can show: overlapping appends, inspection of pending operations and of the memory an append borrows, a
 * close that waits for outstanding operations, random-order delivery, injected failures and a rejecting executor.
 */
public class MemorySpillStoreTests extends SpillStoreConformanceTestCase {

    private MemorySpillStore memoryStore;
    private MemorySpillStore.Completion mode;

    @Override
    protected SpillStore newStore(long limitBytes) {
        if (mode == null) {
            mode = randomFrom(MemorySpillStore.Completion.values());
        }
        memoryStore = new MemorySpillStore(mode, ioExecutor(), limitBytes, LIMIT_SETTING, new Random(random().nextLong()));
        return memoryStore;
    }

    @Override
    protected void pump() {
        if (memoryStore != null) {
            memoryStore.releaseRandom();
        }
    }

    private MemorySpillStore deferredStore() {
        mode = MemorySpillStore.Completion.DEFERRED;
        store();
        return memoryStore;
    }

    public void testOverlappingAppendsAreRejectedAndPoison() {
        MemorySpillStore store = deferredStore();
        SpillScope scope = store.newScope("q1", breaker);
        SpillWriter writer = createWriter(scope, "t");
        byte[] bytes = randomBytes(between(10, 1000));
        try (ReleasableBytesReference first = chunk(bytes, 0, 5); ReleasableBytesReference second = chunk(bytes, 5, 10)) {
            PlainActionFuture<Void> firstAppend = issue("first", l -> writer.append(first, l));
            assertFalse(firstAppend.isDone());
            assertThat(store.pendingOperations(), equalTo(1));
            // the backend holds its own reference and charges it while the append is in flight
            assertThat(breaker.getUsed(), equalTo(5L));
            PlainActionFuture<Void> secondAppend = issue("second", l -> writer.append(second, l));
            store.release(2);
            awaitFuture(firstAppend);
            assertThat(
                unwrapFailure(expectThrows(Exception.class, () -> awaitFuture(secondAppend))),
                instanceOf(IllegalStateException.class)
            );
            assertThat("the borrowed reference went back before the callback", breaker.getUsed(), equalTo(0L));
            assertTrue(first.hasReferences());
            assertThat(awaitFailure(writer::finish), instanceOf(IllegalStateException.class));
        }
        await(scope::close);
    }

    public void testCloseWaitsForOutstandingOperations() {
        MemorySpillStore store = deferredStore();
        SpillScope scope = store.newScope("q1", breaker);
        SpillWriter writer = createWriter(scope, "t");
        byte[] bytes = randomBytes(between(10, 1000));
        try (ReleasableBytesReference chunk = chunk(bytes, 0, bytes.length)) {
            PlainActionFuture<Void> append = issue("append", l -> writer.append(chunk, l));
            PlainActionFuture<Void> close = issue("close", scope::close);
            assertThat(store.heldOperations(), equalTo(1));
            assertFalse(close.isDone());
            // the append completes first, cancelled by the close; only then does the close itself run and deliver
            store.release(1);
            assertTrue(append.isDone());
            assertThat(
                unwrapFailure(expectThrows(Exception.class, () -> awaitFutureNoPump(append))),
                instanceOf(TaskCancelledException.class)
            );
            assertThat(store.liveScopes(), equalTo(0));
            assertFalse(close.isDone());
            store.release(1);
            assertTrue(close.isDone());
            awaitFuture(close);
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    private <T> T awaitFutureNoPump(PlainActionFuture<T> future) {
        return future.actionGet();
    }

    public void testRandomOrderDeliveryCompletesEverything() {
        MemorySpillStore store = deferredStore();
        SpillScope scope = store.newScope("q1", breaker);
        List<PlainActionFuture<?>> futures = new ArrayList<>();
        int writers = between(2, 8);
        List<ReleasableBytesReference> chunks = new ArrayList<>();
        for (int i = 0; i < writers; i++) {
            PlainActionFuture<SpillWriter> created = issue("createWriter-" + i, l -> scope.createWriter("t", l));
            futures.add(created);
        }
        store.releaseAll();
        for (PlainActionFuture<?> future : futures) {
            assertTrue(future.isDone());
        }
        futures.clear();
        List<SpillWriter> writerList = new ArrayList<>();
        for (int i = 0; i < writers; i++) {
            writerList.add(createWriter(scope, "t"));
        }
        byte[] bytes = randomBytes(between(10, 1000));
        for (SpillWriter writer : writerList) {
            ReleasableBytesReference chunk = chunk(bytes, 0, bytes.length);
            chunks.add(chunk);
            PlainActionFuture<Void> appended = issue("append", l -> writer.append(chunk, l));
            futures.add(appended);
        }
        assertThat(store.heldOperations(), equalTo(writers));
        assertThat(breaker.getUsed(), equalTo((long) writers * bytes.length));
        store.releaseAll();
        for (PlainActionFuture<?> future : futures) {
            assertTrue(future.isDone());
        }
        chunks.forEach(ReleasableBytesReference::close);
        assertThat(breaker.getUsed(), equalTo(0L));
        for (SpillWriter writer : writerList) {
            SpillObject object = await(writer::finish);
            assertArrayEquals(bytes, readRange(object, 0, bytes.length));
        }
        await(scope::close);
    }

    public void testAFinishDeliveredAsAFailureTakesTheObjectBack() {
        MemorySpillStore store = deferredStore();
        SpillScope scope = store.newScope("q1", breaker);
        SpillWriter writer = createWriter(scope, "t");
        byte[] bytes = randomBytes(between(10, 1000));
        try (ReleasableBytesReference chunk = chunk(bytes, 0, bytes.length)) {
            PlainActionFuture<Void> appended = issue("append", l -> writer.append(chunk, l));
            store.release(1);
            awaitFutureNoPump(appended);
        }
        PlainActionFuture<SpillObject> finished = issue("finish", writer::finish);
        assertThat("the object exists from the call", scope.bytesInUse(), equalTo((long) bytes.length));
        scope.cancel();
        store.release(1);
        assertThat(
            unwrapFailure(expectThrows(Exception.class, () -> awaitFutureNoPump(finished))),
            instanceOf(TaskCancelledException.class)
        );
        assertThat("the object a cancelled finish had published is gone", scope.bytesInUse(), equalTo(0L));
        assertThat(awaitFailure(writer::finish), instanceOf(IllegalStateException.class));
        await(writer::abort);
        assertThat("an abort after the failed finish returns nothing twice", scope.bytesInUse(), equalTo(0L));
        await(scope::close);
    }

    public void testInjectedFailurePoisonsTheWriter() {
        SpillStore store = store();
        SpillScope scope = store.newScope("q1", breaker);
        SpillWriter writer = createWriter(scope, "t");
        byte[] bytes = randomBytes(between(10, 1000));
        memoryStore.setFailureInjection(op -> op.equals("append") ? new IOException("disk went away") : null);
        try (ReleasableBytesReference chunk = chunk(bytes, 0, bytes.length)) {
            assertThat(failedAppend(writer, chunk).getMessage(), equalTo("disk went away"));
            assertTrue(chunk.hasReferences());
        }
        memoryStore.setFailureInjection(op -> null);
        try (ReleasableBytesReference chunk = chunk(bytes, 0, bytes.length)) {
            assertThat(failedAppend(writer, chunk), instanceOf(IllegalStateException.class));
        }
        await(writer::abort);
        assertThat(breaker.getUsed(), equalTo(0L));
        await(scope::close);
    }

    public void testInjectedReadFailureReleasesTheBuffer() {
        SpillStore store = store();
        SpillScope scope = store.newScope("q1", breaker);
        byte[] bytes = randomBytes(between(10, 1000));
        SpillObject object = write(scope, bytes);
        SpillReader reader = open(object, 0, bytes.length);
        memoryStore.setFailureInjection(op -> op.equals("read") ? new IOException("read failed") : null);
        assertThat(failedRead(reader, 100).getMessage(), equalTo("read failed"));
        memoryStore.setFailureInjection(op -> null);
        assertThat("a failed read poisons the reader", failedRead(reader, 100), instanceOf(IllegalStateException.class));
        assertThat("the buffer the read had decided to return was released", breaker.getUsed(), equalTo(0L));
        memoryStore.setFailureInjection(op -> null);
        await(reader::close);
        await(scope::close);
    }

    public void testRejectingExecutorCompletesEveryListener() {
        mode = MemorySpillStore.Completion.EXECUTOR;
        Executor rejecting = command -> { throw new EsRejectedExecutionException("rejected", false); };
        memoryStore = new MemorySpillStore(mode, rejecting, Long.MAX_VALUE, LIMIT_SETTING, new Random(random().nextLong()));
        SpillScope scope = memoryStore.newScope("q1", breaker);
        assertThat(failedCreateWriter(scope), instanceOf(EsRejectedExecutionException.class));
        assertThat(awaitFailure(scope::close), instanceOf(EsRejectedExecutionException.class));
        assertThat(memoryStore.pendingOperations(), equalTo(0));
        assertThat(memoryStore.operationsAccepted(), greaterThan(0));
        // the store is not registered with the suite, so its shutdown is this test's
        assertThat(awaitFailure(memoryStore::shutdown), instanceOf(EsRejectedExecutionException.class));
        memoryStore = null;
    }

    public void testReadBuffersAreChargedUntilClosed() {
        SpillStore store = store();
        SpillScope scope = store.newScope("q1", breaker);
        byte[] bytes = randomBytes(between(100, 10_000));
        SpillObject object = write(scope, bytes);
        memoryStore.setShortReads(false);
        SpillReader reader = open(object, 0, bytes.length);
        SpillRead read = read(reader, bytes.length);
        assertThat(read.data().length(), equalTo(bytes.length));
        assertThat(breaker.getUsed(), equalTo((long) bytes.length));
        read.data().close();
        assertThat(breaker.getUsed(), equalTo(0L));
        assertTrue(read(reader, 1).isEof());
        await(reader::close);
        await(scope::close);
    }
}
