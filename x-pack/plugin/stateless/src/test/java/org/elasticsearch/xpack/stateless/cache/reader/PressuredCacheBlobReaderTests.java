/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache.reader;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class PressuredCacheBlobReaderTests extends ESTestCase {

    private TestThreadPool threadPool;

    @Before
    public void createThreadPool() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void terminateThreadPool() {
        terminate(threadPool);
    }

    private FillCacheMemoryPressure pressureWithLimit(long limitBytes) {
        return new FillCacheMemoryPressure(
            Settings.builder().put(FillCacheMemoryPressure.FILL_BYTES_LIMIT.getKey(), ByteSizeValue.ofBytes(limitBytes)).build(),
            MeterRegistry.NOOP,
            threadPool
        );
    }

    /** In-memory delegate: the budget lifecycle lives entirely in the wrapper, so a real reader isn't needed. */
    private static CacheBlobReader byteArrayReader() {
        return new CacheBlobReader() {
            @Override
            public ByteRange getRange(long position, int length, long remainingFileLength) {
                return ByteRange.of(position, position + length);
            }

            @Override
            public void getRangeInputStream(long position, int length, ActionListener<InputStream> listener) {
                listener.onResponse(new ByteArrayInputStream(new byte[length]));
            }

            @Override
            public String executorName() {
                return "test";
            }
        };
    }

    public void testBudgetHeldUntilStreamClosed() throws IOException {
        var pressure = pressureWithLimit(100);
        var reader = new PressuredCacheBlobReader(byteArrayReader(), pressure, threadPool);
        PlainActionFuture<InputStream> future = new PlainActionFuture<>();
        reader.getRangeInputStream(0, 40, future);
        try (InputStream stream = safeGet(future)) {
            stream.readAllBytes();
            assertThat(pressure.getCurrentBytes(), equalTo(40L));
        }
        assertThat(pressure.getCurrentBytes(), equalTo(0L));
    }

    public void testBudgetReleasedOnDelegateFailure() {
        var pressure = pressureWithLimit(100);
        var failure = new IOException("simulated read failure");
        var reader = new PressuredCacheBlobReader(new CacheBlobReader() {
            @Override
            public ByteRange getRange(long position, int length, long remainingFileLength) {
                return ByteRange.of(position, position + length);
            }

            @Override
            public void getRangeInputStream(long position, int length, ActionListener<InputStream> listener) {
                if (randomBoolean()) {
                    listener.onFailure(failure);
                } else {
                    // simulate a delegate that throws instead of failing the listener
                    throw new RuntimeException(failure);
                }
            }

            @Override
            public String executorName() {
                return "test";
            }
        }, pressure, threadPool);
        PlainActionFuture<InputStream> future = new PlainActionFuture<>();
        reader.getRangeInputStream(0, 40, future);
        expectThrows(Exception.class, () -> future.actionGet(TEST_REQUEST_TIMEOUT));
        assertThat(pressure.getCurrentBytes(), equalTo(0L));
        assertThat(pressure.getWaiterCount(), equalTo(0));
    }

    public void testReadsWaitForBudgetAndProceedWhenReleased() throws IOException {
        var pressure = pressureWithLimit(100);
        var reader = new PressuredCacheBlobReader(byteArrayReader(), pressure, threadPool);

        PlainActionFuture<InputStream> first = new PlainActionFuture<>();
        reader.getRangeInputStream(0, 80, first);
        assertTrue(first.isDone());

        AtomicInteger granted = new AtomicInteger();
        PlainActionFuture<InputStream> second = new PlainActionFuture<>();
        reader.getRangeInputStream(80, 50, ActionListener.runBefore(second, granted::incrementAndGet));
        assertThat("second read must wait for budget", granted.get(), equalTo(0));
        assertFalse(second.isDone());

        // closing the first stream releases its budget and unblocks the second read
        safeGet(first).close();
        assertThat(granted.get(), equalTo(1));
        try (InputStream stream = safeGet(second)) {
            stream.readAllBytes();
            assertThat(pressure.getCurrentBytes(), equalTo(50L));
        }
        assertThat(pressure.getCurrentBytes(), equalTo(0L));
    }

    /**
     * A deferred read must resume on the invoking thread's pool, not the releaser's: fill handlers assert specific pools and
     * direct-executor delegates run the cache-file write on the resuming thread.
     */
    public void testDeferredReadResumesOnAcquiringThreadsPool() throws Exception {
        var pressure = pressureWithLimit(100);
        List<String> readThreadNames = new CopyOnWriteArrayList<>();
        var reader = new PressuredCacheBlobReader(new CacheBlobReader() {
            @Override
            public ByteRange getRange(long position, int length, long remainingFileLength) {
                return ByteRange.of(position, position + length);
            }

            @Override
            public void getRangeInputStream(long position, int length, ActionListener<InputStream> listener) {
                readThreadNames.add(Thread.currentThread().getName());
                listener.onResponse(new ByteArrayInputStream(new byte[length]));
            }

            @Override
            public String executorName() {
                return "test";
            }
        }, pressure, threadPool);

        // exhaust the budget from the test thread
        PlainActionFuture<InputStream> first = new PlainActionFuture<>();
        reader.getRangeInputStream(0, 80, first);
        assertTrue(first.isDone());

        // issue the second read from a generic-pool thread so that pool is captured as the read's home
        PlainActionFuture<InputStream> second = new PlainActionFuture<>();
        PlainActionFuture<Void> issued = new PlainActionFuture<>();
        threadPool.generic().execute(() -> {
            reader.getRangeInputStream(80, 50, second);
            issued.onResponse(null);
        });
        safeGet(issued);
        assertFalse(second.isDone());

        // release from the test thread: the deferred read must resume on the generic pool, not here
        safeGet(first).close();
        try (InputStream stream = safeGet(second)) {
            stream.readAllBytes();
        }
        assertThat(readThreadNames, hasSize(2));
        assertThat(readThreadNames.get(1), containsString("[" + ThreadPool.Names.GENERIC + "]"));
        assertThat(pressure.getCurrentBytes(), equalTo(0L));
    }

    public void testDoubleCloseReleasesBudgetOnce() throws IOException {
        var pressure = pressureWithLimit(100);
        var reader = new PressuredCacheBlobReader(byteArrayReader(), pressure, threadPool);
        PlainActionFuture<InputStream> future = new PlainActionFuture<>();
        reader.getRangeInputStream(0, 40, future);
        InputStream stream = safeGet(future);
        stream.close();
        stream.close();
        assertThat(pressure.getCurrentBytes(), equalTo(0L));
    }
}
