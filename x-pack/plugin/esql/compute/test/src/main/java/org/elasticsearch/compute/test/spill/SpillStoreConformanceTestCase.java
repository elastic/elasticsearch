/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.test.spill;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.UncategorizedExecutionException;
import org.elasticsearch.compute.spill.spi.SpillLimitExceededException;
import org.elasticsearch.compute.spill.spi.SpillObject;
import org.elasticsearch.compute.spill.spi.SpillRead;
import org.elasticsearch.compute.spill.spi.SpillReader;
import org.elasticsearch.compute.spill.spi.SpillScope;
import org.elasticsearch.compute.spill.spi.SpillStore;
import org.elasticsearch.compute.spill.spi.SpillWriter;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * The invariants of the spill SPI, run against every backend. A backend's test extends this and returns its store from
 * {@link #newStore}; a backend whose completions the test must drive overrides {@link #pump}. Every listener handed to
 * the backend is counted, and the suite asserts at the end of each test that each completed exactly once. The scope
 * breaker is the test's, so a backend buffer not returned fails the test at teardown.
 */
public abstract class SpillStoreConformanceTestCase extends ComputeTestCase {

    protected static final String IO = "spill_io_test";
    protected static final String LIMIT_SETTING = "test.spill.limit";
    private static final long DEADLINE_SECONDS = 30;

    private TestThreadPool threadPool;
    private final List<CountingListener<?>> listeners = new ArrayList<>();
    private SpillStore store;
    /**
     * The breaker every scope of a test charges its buffers to; one per test, since each {@link #blockFactory()} call
     * makes a new one, and checked for zero at teardown.
     */
    protected CircuitBreaker breaker;

    /**
     * A store with the given cap, over {@link #ioExecutor()} if it needs one.
     */
    protected abstract SpillStore newStore(long limitBytes);

    /**
     * Drives completions that need the test's help; nothing by default.
     */
    protected void pump() {}

    /**
     * The setting name a backend reports in {@link SpillLimitExceededException}; the suite's own by default.
     */
    protected String limitSettingName() {
        return LIMIT_SETTING;
    }

    @Before
    public void startPool() {
        breaker = blockFactory().breaker();
        threadPool = new TestThreadPool(
            getTestName(),
            new FixedExecutorBuilder(Settings.EMPTY, IO, between(1, 4), 64, "spill", EsExecutors.TaskTrackingConfig.DEFAULT)
        );
    }

    @After
    public void stopPoolAndCheckListeners() throws Exception {
        try {
            if (store != null) {
                await(store::shutdown);
            }
            for (CountingListener<?> listener : listeners) {
                assertThat(
                    "listener [" + listener.name + "] completed " + listener.completions.get() + " times",
                    listener.completions.get(),
                    equalTo(1)
                );
            }
        } finally {
            if (threadPool != null) {
                ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
            }
        }
    }

    protected Executor ioExecutor() {
        return threadPool.executor(IO);
    }

    protected SpillStore store(long limitBytes) {
        assert store == null : "one store per test";
        store = newStore(limitBytes);
        return store;
    }

    protected SpillStore store() {
        return store(Long.MAX_VALUE);
    }

    // ---- listeners that must complete exactly once

    private final class CountingListener<T> implements ActionListener<T> {
        private final String name;
        private final PlainActionFuture<T> future = new PlainActionFuture<>();
        private final AtomicInteger completions = new AtomicInteger();

        CountingListener(String name) {
            this.name = name;
            listeners.add(this);
        }

        @Override
        public void onResponse(T result) {
            completions.incrementAndGet();
            future.onResponse(result);
        }

        @Override
        public void onFailure(Exception e) {
            completions.incrementAndGet();
            future.onFailure(e);
        }
    }

    protected <T> PlainActionFuture<T> issue(String name, Consumer<ActionListener<T>> operation) {
        CountingListener<T> listener = new CountingListener<>(name);
        operation.accept(listener);
        return listener.future;
    }

    protected <T> T awaitFuture(PlainActionFuture<T> future) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(DEADLINE_SECONDS);
        while (future.isDone() == false) {
            pump();
            Thread.onSpinWait();
            if (System.nanoTime() > deadline) {
                fail("operation did not complete");
            }
        }
        return future.actionGet();
    }

    protected <T> T await(Consumer<ActionListener<T>> operation) {
        return awaitFuture(issue("op", operation));
    }

    protected <T> Throwable awaitFailure(Consumer<ActionListener<T>> operation) {
        PlainActionFuture<T> future = issue("op", operation);
        return unwrapFailure(expectThrows(Exception.class, () -> awaitFuture(future)));
    }

    /**
     * The failure a future failed with: {@code actionGet} rethrows a runtime exception as is and wraps a checked one
     * twice.
     */
    protected static Throwable unwrapFailure(Exception e) {
        Throwable cause = e;
        while ((cause instanceof UncategorizedExecutionException || cause instanceof ExecutionException) && cause.getCause() != null) {
            cause = cause.getCause();
        }
        return ExceptionsHelper.unwrapCause(cause);
    }

    // ---- typed operations, since a lambda in statement position cannot infer the listener's type

    protected SpillWriter createWriter(SpillScope scope, String nameHint) {
        return this.<SpillWriter>await(l -> scope.createWriter(nameHint, l));
    }

    protected Throwable failedCreateWriter(SpillScope scope) {
        return this.<SpillWriter>awaitFailure(l -> scope.createWriter("failing", l));
    }

    protected void append(SpillWriter writer, ReleasableBytesReference chunk) {
        this.<Void>await(l -> writer.append(chunk, l));
    }

    protected Throwable failedAppend(SpillWriter writer, ReleasableBytesReference chunk) {
        return this.<Void>awaitFailure(l -> writer.append(chunk, l));
    }

    protected SpillReader open(SpillObject object, long offset, long length) {
        return this.<SpillReader>await(l -> object.openRange(offset, length, l));
    }

    protected Throwable failedOpen(SpillObject object, long offset, long length) {
        return this.<SpillReader>awaitFailure(l -> object.openRange(offset, length, l));
    }

    protected SpillRead read(SpillReader reader, int maxBytes) {
        return this.<SpillRead>await(l -> reader.read(maxBytes, l));
    }

    protected Throwable failedRead(SpillReader reader, int maxBytes) {
        return this.<SpillRead>awaitFailure(l -> reader.read(maxBytes, l));
    }

    // ---- helpers

    protected static ReleasableBytesReference chunk(byte[] bytes, int from, int to) {
        return ReleasableBytesReference.wrap(new BytesArray(bytes, from, to - from));
    }

    protected SpillObject write(SpillScope scope, byte[] bytes) {
        SpillWriter writer = createWriter(scope, "test");
        int from = 0;
        while (from < bytes.length) {
            int to = Math.min(bytes.length, from + between(1, Math.max(1, bytes.length / 4)));
            try (ReleasableBytesReference chunk = chunk(bytes, from, to)) {
                append(writer, chunk);
                assertTrue("the caller's reference is untouched by the backend", chunk.hasReferences());
            }
            from = to;
        }
        return await(writer::finish);
    }

    protected byte[] readAll(SpillReader reader) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        while (true) {
            SpillRead read = read(reader, between(1, 4096));
            if (read.isEof()) {
                // the end is sticky
                assertTrue(read(reader, 1).isEof());
                return out.toByteArray();
            }
            try (ReleasableBytesReference data = read.data()) {
                assertThat("empty data is not how a backend says end", data.length(), greaterThan(0));
                byte[] bytes = BytesReference.toBytes(data);
                out.write(bytes, 0, bytes.length);
            }
        }
    }

    protected byte[] readRange(SpillObject object, long offset, long length) {
        SpillReader reader = open(object, offset, length);
        byte[] bytes = readAll(reader);
        await(reader::close);
        return bytes;
    }

    protected byte[] randomBytes(int length) {
        byte[] bytes = new byte[length];
        random().nextBytes(bytes);
        return bytes;
    }

    // ---- the invariants

    public void testRoundTrip() {
        SpillStore store = store();
        SpillScope scope = store.newScope("q1", breaker);
        byte[] bytes = randomBytes(between(0, 200_000));
        SpillObject object = write(scope, bytes);
        assertThat(object.sizeInBytes(), equalTo((long) bytes.length));
        assertThat(scope.bytesInUse(), equalTo((long) bytes.length));
        assertThat(store.bytesInUse(), equalTo((long) bytes.length));
        assertArrayEquals(bytes, readRange(object, 0, bytes.length));
        await(object::delete);
        assertThat(scope.bytesInUse(), equalTo(0L));
        await(scope::close);
    }

    public void testRangesAndShortReads() {
        SpillStore store = store();
        SpillScope scope = store.newScope("q1", breaker);
        byte[] bytes = randomBytes(between(1000, 100_000));
        SpillObject object = write(scope, bytes);
        List<SpillReader> readers = new ArrayList<>();
        List<int[]> ranges = new ArrayList<>();
        for (int i = 0; i < between(1, 5); i++) {
            int from = between(0, bytes.length - 1);
            int to = between(from, bytes.length);
            ranges.add(new int[] { from, to });
            readers.add(open(object, from, to - from));
        }
        for (int i = 0; i < readers.size(); i++) {
            byte[] expected = new byte[ranges.get(i)[1] - ranges.get(i)[0]];
            System.arraycopy(bytes, ranges.get(i)[0], expected, 0, expected.length);
            assertArrayEquals(expected, readAll(readers.get(i)));
        }
        for (SpillReader reader : readers) {
            await(reader::close);
            await(reader::close); // idempotent
        }
        await(scope::close);
    }

    public void testRangeAndReadSizeAreValidatedBeforeAnythingIsAllocated() {
        SpillStore store = store();
        SpillScope scope = store.newScope("q1", breaker);
        byte[] bytes = randomBytes(between(10, 1000));
        SpillObject object = write(scope, bytes);
        long used = breaker.getUsed();
        assertThat(failedOpen(object, -1, 1), instanceOf(IllegalArgumentException.class));
        assertThat(failedOpen(object, 0, bytes.length + 1), instanceOf(IllegalArgumentException.class));
        assertThat(failedOpen(object, bytes.length, 1), instanceOf(IllegalArgumentException.class));
        assertThat(failedOpen(object, 1, -1), instanceOf(IllegalArgumentException.class));
        SpillReader reader = open(object, 0, bytes.length);
        assertThat(failedRead(reader, 0), instanceOf(IllegalArgumentException.class));
        assertThat(failedRead(reader, -5), instanceOf(IllegalArgumentException.class));
        assertThat("validation allocated nothing", breaker.getUsed(), equalTo(used));
        assertArrayEquals(bytes, readAll(reader));
        await(reader::close);
        await(scope::close);
    }

    public void testEmptyObjectEndsAtOnce() {
        SpillStore store = store();
        SpillScope scope = store.newScope("q1", breaker);
        SpillObject object = write(scope, new byte[0]);
        assertThat(object.sizeInBytes(), equalTo(0L));
        assertArrayEquals(new byte[0], readRange(object, 0, 0));
        await(scope::close);
    }

    public void testDeleteFailsLaterReadsAndReturnsBytes() {
        SpillStore store = store();
        SpillScope scope = store.newScope("q1", breaker);
        byte[] bytes = randomBytes(between(100, 10_000));
        SpillObject object = write(scope, bytes);
        SpillReader first = open(object, 0, bytes.length);
        SpillReader second = open(object, 0, bytes.length);
        SpillRead partial = read(first, 16);
        assertFalse(partial.isEof());
        partial.data().close();
        await(object::delete);
        await(object::delete); // idempotent
        assertThat(scope.bytesInUse(), equalTo(0L));
        assertThat(store.bytesInUse(), equalTo(0L));
        assertThat(failedRead(first, 16), instanceOf(IllegalStateException.class));
        assertThat(failedRead(second, 16), instanceOf(IllegalStateException.class));
        assertThat(failedOpen(object, 0, 1), instanceOf(IllegalStateException.class));
        await(first::close);
        await(second::close);
        await(scope::close);
    }

    public void testAbortDiscards() {
        SpillStore store = store();
        SpillScope scope = store.newScope("q1", breaker);
        SpillWriter writer = createWriter(scope, "test");
        byte[] bytes = randomBytes(between(1, 10_000));
        try (ReleasableBytesReference chunk = chunk(bytes, 0, bytes.length)) {
            append(writer, chunk);
        }
        assertThat(scope.bytesInUse(), equalTo((long) bytes.length));
        await(writer::abort);
        assertThat(scope.bytesInUse(), equalTo(0L));
        await(writer::abort); // idempotent
        assertThat(awaitFailure(writer::finish), instanceOf(IllegalStateException.class));
        await(scope::close);
    }

    public void testFinishPublishesExactlyOnce() {
        SpillStore store = store();
        SpillScope scope = store.newScope("q1", breaker);
        byte[] bytes = randomBytes(between(1, 10_000));
        SpillWriter writer = createWriter(scope, "test");
        try (ReleasableBytesReference chunk = chunk(bytes, 0, bytes.length)) {
            append(writer, chunk);
        }
        SpillObject object = await(writer::finish);
        assertThat(awaitFailure(writer::finish), instanceOf(IllegalStateException.class));
        try (ReleasableBytesReference chunk = chunk(bytes, 0, 1)) {
            assertThat(failedAppend(writer, chunk), instanceOf(IllegalStateException.class));
        }
        await(writer::abort); // a no-op after finish
        assertThat(scope.bytesInUse(), equalTo((long) bytes.length));
        assertArrayEquals(bytes, readRange(object, 0, bytes.length));
        await(scope::close);
    }

    public void testLimitFailsTheAppendAndPoisonsTheWriter() {
        long limit = between(100, 1000);
        SpillStore store = store(limit);
        assertThat(store.bytesLimit(), equalTo(limit));
        SpillScope scope = store.newScope("q1", breaker);
        SpillWriter writer = createWriter(scope, "test");
        byte[] within = randomBytes((int) (limit / 2));
        try (ReleasableBytesReference chunk = chunk(within, 0, within.length)) {
            append(writer, chunk);
        }
        byte[] beyond = randomBytes((int) limit);
        Throwable e;
        try (ReleasableBytesReference chunk = chunk(beyond, 0, beyond.length)) {
            e = failedAppend(writer, chunk);
        }
        assertThat(e, instanceOf(SpillLimitExceededException.class));
        SpillLimitExceededException limitExceeded = (SpillLimitExceededException) e;
        assertThat(limitExceeded.status(), equalTo(RestStatus.INSUFFICIENT_STORAGE));
        assertThat(limitExceeded.settingName(), equalTo(limitSettingName()));
        assertThat(limitExceeded.limitBytes(), equalTo(limit));
        assertThat(limitExceeded.getMessage(), containsString(limitSettingName()));
        // poisoned
        try (ReleasableBytesReference chunk = chunk(within, 0, 1)) {
            assertThat(failedAppend(writer, chunk), instanceOf(IllegalStateException.class));
        }
        assertThat(awaitFailure(writer::finish), instanceOf(IllegalStateException.class));
        await(writer::abort);
        assertThat(scope.bytesInUse(), equalTo(0L));
        assertThat(store.bytesInUse(), lessThanOrEqualTo(limit));
        await(scope::close);
    }

    public void testScopeCloseDeletesEverythingAndIsIdempotent() {
        SpillStore store = store();
        SpillScope scope = store.newScope("q1", breaker);
        SpillScope other = store.newScope("q2", breaker);
        byte[] bytes = randomBytes(between(100, 10_000));
        SpillObject finished = write(scope, bytes);
        SpillObject elsewhere = write(other, bytes);
        SpillWriter unfinished = createWriter(scope, "unfinished");
        try (ReleasableBytesReference chunk = chunk(bytes, 0, bytes.length)) {
            append(unfinished, chunk);
        }
        SpillReader reader = open(finished, 0, bytes.length);
        assertThat(store.bytesInUse(), equalTo(3L * bytes.length));
        await(scope::close);
        await(scope::close);
        assertThat(scope.bytesInUse(), equalTo(0L));
        assertThat(store.bytesInUse(), equalTo((long) bytes.length));
        assertThat(failedRead(reader, 1), instanceOf(IllegalStateException.class));
        assertThat(failedCreateWriter(scope), instanceOf(IllegalStateException.class));
        await(reader::close);
        assertArrayEquals(bytes, readRange(elsewhere, 0, bytes.length));
        await(other::close);
        assertThat(store.bytesInUse(), equalTo(0L));
    }

    public void testCancelThenCloseCompletes() {
        SpillStore store = store();
        SpillScope scope = store.newScope("q1", breaker);
        byte[] bytes = randomBytes(between(100, 10_000));
        SpillWriter writer = createWriter(scope, "test");
        List<PlainActionFuture<Void>> appends = new ArrayList<>();
        try (ReleasableBytesReference chunk = chunk(bytes, 0, bytes.length)) {
            appends.add(issue("append", l -> writer.append(chunk, l)));
            scope.cancel();
            scope.cancel();
            PlainActionFuture<Void> closed = issue("close", scope::close);
            for (PlainActionFuture<Void> append : appends) {
                try {
                    awaitFuture(append);
                } catch (Exception e) {
                    // a cancelled append may fail; it must complete
                }
            }
            awaitFuture(closed);
        }
        assertThat(scope.bytesInUse(), equalTo(0L));
    }

    public void testCallbacksMayIssueTheNextOperation() {
        SpillStore store = store();
        SpillScope scope = store.newScope("q1", breaker);
        byte[] bytes = randomBytes(between(100, 50_000));
        int chunks = between(2, 20);
        int chunkSize = Math.max(1, bytes.length / chunks);
        SpillWriter writer = createWriter(scope, "chain");
        PlainActionFuture<SpillObject> finished = issue("chain", l -> appendFrom(writer, bytes, 0, chunkSize, l));
        SpillObject object = awaitFuture(finished);
        assertArrayEquals(bytes, readRange(object, 0, bytes.length));
        await(scope::close);
    }

    /**
     * Appends the chunk at {@code from} and, from inside its callback, the next one; finishes after the last.
     */
    private void appendFrom(SpillWriter writer, byte[] bytes, int from, int chunkSize, ActionListener<SpillObject> listener) {
        if (from >= bytes.length) {
            writer.finish(listener);
            return;
        }
        int to = Math.min(bytes.length, from + chunkSize);
        ReleasableBytesReference chunk = chunk(bytes, from, to);
        writer.append(
            chunk,
            ActionListener.runBefore(
                listener.delegateFailureAndWrap((l, ignored) -> appendFrom(writer, bytes, to, chunkSize, l)),
                chunk::close
            )
        );
    }

    public void testShutdownClosesOpenScopes() {
        SpillStore store = store();
        SpillScope scope = store.newScope("q1", breaker);
        byte[] bytes = randomBytes(between(100, 10_000));
        write(scope, bytes);
        assertThat(store.bytesInUse(), equalTo((long) bytes.length));
        await(store::shutdown);
        assertThat(store.bytesInUse(), equalTo(0L));
        assertThat(scope.bytesInUse(), equalTo(0L));
        await(store::shutdown); // idempotent
    }

    public void testWritersOfOneScopeFromSeveralThreads() throws Exception {
        SpillStore store = store();
        SpillScope scope = store.newScope("q1", breaker);
        int writers = between(2, 6);
        List<byte[]> contents = new ArrayList<>();
        List<PlainActionFuture<SpillObject>> objects = new ArrayList<>();
        for (int i = 0; i < writers; i++) {
            byte[] bytes = randomBytes(between(100, 20_000));
            contents.add(bytes);
            // inline completion recurses once per chunk on the issuing thread, so the chunk count stays bounded
            int chunkSize = between(Math.max(1, bytes.length / 32), bytes.length);
            PlainActionFuture<SpillObject> object = issue(
                "writer-" + i,
                l -> ioExecutor().execute(
                    () -> scope.createWriter("t", l.delegateFailureAndWrap((ll, writer) -> appendFrom(writer, bytes, 0, chunkSize, ll)))
                )
            );
            objects.add(object);
        }
        for (int i = 0; i < writers; i++) {
            SpillObject object = awaitFuture(objects.get(i));
            assertArrayEquals(contents.get(i), readRange(object, 0, contents.get(i).length));
        }
        await(scope::close);
    }
}
