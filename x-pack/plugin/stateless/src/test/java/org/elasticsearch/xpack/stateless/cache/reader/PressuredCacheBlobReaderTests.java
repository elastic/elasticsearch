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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;

public class PressuredCacheBlobReaderTests extends ESTestCase {

    private static FillCacheMemoryPressure pressureWithLimit(long limitBytes) {
        return new FillCacheMemoryPressure(
            Settings.builder().put(FillCacheMemoryPressure.FILL_BYTES_LIMIT.getKey(), ByteSizeValue.ofBytes(limitBytes)).build(),
            MeterRegistry.NOOP,
            Runnable::run
        );
    }

    /**
     * Simple in-memory delegate; a real reader (transport or object store) is unnecessary to exercise the budget lifecycle, which is
     * entirely in the wrapper.
     */
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
        var reader = new PressuredCacheBlobReader(byteArrayReader(), pressure);
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
                    // simulate a delegate that throws instead of notifying the listener
                    throw new RuntimeException(failure);
                }
            }

            @Override
            public String executorName() {
                return "test";
            }
        }, pressure);
        PlainActionFuture<InputStream> future = new PlainActionFuture<>();
        reader.getRangeInputStream(0, 40, future);
        expectThrows(Exception.class, () -> future.actionGet(TEST_REQUEST_TIMEOUT));
        assertThat(pressure.getCurrentBytes(), equalTo(0L));
        assertThat(pressure.getWaiterCount(), equalTo(0));
    }

    public void testReadsWaitForBudgetAndProceedWhenReleased() throws IOException {
        var pressure = pressureWithLimit(100);
        var reader = new PressuredCacheBlobReader(byteArrayReader(), pressure);

        PlainActionFuture<InputStream> first = new PlainActionFuture<>();
        reader.getRangeInputStream(0, 80, first);
        assertTrue(first.isDone());

        AtomicInteger granted = new AtomicInteger();
        PlainActionFuture<InputStream> second = new PlainActionFuture<>();
        reader.getRangeInputStream(80, 50, ActionListener.runBefore(second, granted::incrementAndGet));
        assertThat("second read must wait for budget", granted.get(), equalTo(0));
        assertFalse(second.isDone());

        // draining the first stream (closing it) releases its budget and unblocks the second read
        safeGet(first).close();
        assertThat(granted.get(), equalTo(1));
        try (InputStream stream = safeGet(second)) {
            stream.readAllBytes();
            assertThat(pressure.getCurrentBytes(), equalTo(50L));
        }
        assertThat(pressure.getCurrentBytes(), equalTo(0L));
    }

    public void testDoubleCloseReleasesBudgetOnce() throws IOException {
        var pressure = pressureWithLimit(100);
        var reader = new PressuredCacheBlobReader(byteArrayReader(), pressure);
        PlainActionFuture<InputStream> future = new PlainActionFuture<>();
        reader.getRangeInputStream(0, 40, future);
        InputStream stream = safeGet(future);
        stream.close();
        stream.close();
        assertThat(pressure.getCurrentBytes(), equalTo(0L));
    }
}
