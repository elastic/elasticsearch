/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.arrow.memory.BufferAllocator;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObjectMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The metrics-delegation test uses the {@link TestStorageObjects} real-class fixture per AGENTS.md.
 * The remaining tests retain Mockito because they simulate stream lifecycle, async listener callbacks
 * (via {@code doAnswer}), and exception-throwing I/O — a real-class subclass would have to reimplement
 * each, which is what AGENTS.md calls out as "the real class is complex". Tracked as follow-up to
 * incrementally extend {@code TestStorageObjects} with builders for those shapes.
 */
public class ConcurrencyLimitedStorageObjectTests extends ESTestCase {

    // Hold a strong reference to the BlockFactory so the JVM Cleaner does not close the
    // arrow root allocator mid-test (BlockFactory.arrowAllocator() registers a cleaner action
    // on its own BlockFactory instance, which is otherwise unreachable from ALLOCATOR alone).
    private static final BlockFactory BLOCK_FACTORY = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("test"))
        .build();
    private static final BufferAllocator ALLOCATOR = BLOCK_FACTORY.arrowAllocator();
    private static final DirectBufferFactory FACTORY = DirectBufferFactory.forAllocator(ALLOCATOR);

    public void testStreamCloseReleasesPermit() throws Exception {
        ConcurrencyLimiter limiter = new ConcurrencyLimiter(3);
        StorageObject delegate = mock(StorageObject.class);
        when(delegate.newStream()).thenReturn(new ByteArrayInputStream("hello".getBytes(StandardCharsets.UTF_8)));
        when(delegate.path()).thenReturn(StoragePath.of("s3://bucket/key"));

        ConcurrencyLimitedStorageObject obj = new ConcurrencyLimitedStorageObject(delegate, limiter);
        assertEquals(3, limiter.availablePermits());

        InputStream stream = obj.newStream();
        assertEquals(2, limiter.availablePermits());

        stream.close();
        assertEquals(3, limiter.availablePermits());
    }

    public void testStreamDoubleCloseIsSafe() throws Exception {
        ConcurrencyLimiter limiter = new ConcurrencyLimiter(3);
        StorageObject delegate = mock(StorageObject.class);
        when(delegate.newStream()).thenReturn(new ByteArrayInputStream("hello".getBytes(StandardCharsets.UTF_8)));
        when(delegate.path()).thenReturn(StoragePath.of("s3://bucket/key"));

        ConcurrencyLimitedStorageObject obj = new ConcurrencyLimitedStorageObject(delegate, limiter);
        InputStream stream = obj.newStream();
        stream.close();
        stream.close();
        assertEquals(3, limiter.availablePermits());
    }

    public void testReadBytesReleasesOnException() throws Exception {
        ConcurrencyLimiter limiter = new ConcurrencyLimiter(3);
        StorageObject delegate = mock(StorageObject.class);
        when(delegate.readBytes(anyLong(), any(ByteBuffer.class))).thenThrow(new IOException("read error"));
        when(delegate.path()).thenReturn(StoragePath.of("s3://bucket/key"));

        ConcurrencyLimitedStorageObject obj = new ConcurrencyLimitedStorageObject(delegate, limiter);
        expectThrows(IOException.class, () -> obj.readBytes(0, ByteBuffer.allocate(10)));
        assertEquals(3, limiter.availablePermits());
    }

    public void testLengthBypassesPermit() throws Exception {
        ConcurrencyLimiter limiter = new ConcurrencyLimiter(3);
        StorageObject delegate = mock(StorageObject.class);
        when(delegate.length()).thenReturn(42L);

        ConcurrencyLimitedStorageObject obj = new ConcurrencyLimitedStorageObject(delegate, limiter);
        assertEquals(42L, obj.length());
        assertEquals(3, limiter.availablePermits());
    }

    /**
     * Starvation regression for #1151: metadata ops (length/lastModified/exists) must not acquire the
     * concurrency permit. With a single permit already held by an open stream, a permit-taking metadata
     * call would block forever (single-threaded here, so it would deadlock/time out). It must instead
     * return immediately without touching the permit count.
     */
    public void testMetadataOpsBypassPermitWhileStreamHoldsOnlyPermit() throws Exception {
        ConcurrencyLimiter limiter = new ConcurrencyLimiter(1);
        StorageObject delegate = mock(StorageObject.class);
        when(delegate.newStream()).thenReturn(new ByteArrayInputStream("hello".getBytes(StandardCharsets.UTF_8)));
        when(delegate.path()).thenReturn(StoragePath.of("s3://bucket/key"));
        when(delegate.length()).thenReturn(42L);
        when(delegate.lastModified()).thenReturn(Instant.ofEpochMilli(123L));
        when(delegate.exists()).thenReturn(true);

        ConcurrencyLimitedStorageObject obj = new ConcurrencyLimitedStorageObject(delegate, limiter);
        InputStream stream = obj.newStream();
        assertEquals(0, limiter.availablePermits());

        assertEquals(42L, obj.length());
        assertEquals(Instant.ofEpochMilli(123L), obj.lastModified());
        assertTrue(obj.exists());
        assertEquals("metadata ops must not take the held stream's permit", 0, limiter.availablePermits());

        stream.close();
        assertEquals(1, limiter.availablePermits());
    }

    @SuppressWarnings("unchecked")
    public void testAsyncReadReleasesPermitOnSuccess() throws Exception {
        ConcurrencyLimiter limiter = new ConcurrencyLimiter(3);
        StorageObject delegate = mock(StorageObject.class);
        when(delegate.path()).thenReturn(StoragePath.of("s3://bucket/key"));
        DirectReadBuffer result = new DirectReadBuffer(ByteBuffer.wrap("data".getBytes(StandardCharsets.UTF_8)), () -> {});
        doAnswer(inv -> {
            ActionListener<DirectReadBuffer> listener = inv.getArgument(4);
            listener.onResponse(result);
            return null;
        }).when(delegate).readBytesAsync(anyLong(), anyLong(), any(), any(), any(ActionListener.class));

        ConcurrencyLimitedStorageObject obj = new ConcurrencyLimitedStorageObject(delegate, limiter);
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<DirectReadBuffer> response = new AtomicReference<>();

        obj.readBytesAsync(0, 4, FACTORY, Runnable::run, ActionListener.wrap(r -> {
            response.set(r);
            latch.countDown();
        }, e -> latch.countDown()));

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertSame(result, response.get());
        assertEquals(3, limiter.availablePermits());
    }

    @SuppressWarnings("unchecked")
    public void testAsyncReadReleasesPermitOnFailure() throws Exception {
        ConcurrencyLimiter limiter = new ConcurrencyLimiter(3);
        StorageObject delegate = mock(StorageObject.class);
        when(delegate.path()).thenReturn(StoragePath.of("s3://bucket/key"));
        doAnswer(inv -> {
            ActionListener<DirectReadBuffer> listener = inv.getArgument(4);
            listener.onFailure(new IOException("async error"));
            return null;
        }).when(delegate).readBytesAsync(anyLong(), anyLong(), any(), any(), any(ActionListener.class));

        ConcurrencyLimitedStorageObject obj = new ConcurrencyLimitedStorageObject(delegate, limiter);
        CountDownLatch latch = new CountDownLatch(1);

        obj.readBytesAsync(0, 4, FACTORY, Runnable::run, ActionListener.wrap(r -> latch.countDown(), e -> latch.countDown()));

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(3, limiter.availablePermits());
    }

    public void testMetricsDelegatesToWrapped() {
        ConcurrencyLimiter limiter = new ConcurrencyLimiter(3);
        StorageObjectMetrics snapshot = new StorageObjectMetrics(11, 2222, 8192, 3);
        StorageObject delegate = TestStorageObjects.metricsOnly(snapshot);

        ConcurrencyLimitedStorageObject obj = new ConcurrencyLimitedStorageObject(delegate, limiter);
        assertSame(snapshot, obj.metrics());
    }

    public void testNewStreamReleasesPermitOnDelegateException() throws Exception {
        ConcurrencyLimiter limiter = new ConcurrencyLimiter(3);
        StorageObject delegate = mock(StorageObject.class);
        when(delegate.newStream()).thenThrow(new IOException("connection refused"));

        ConcurrencyLimitedStorageObject obj = new ConcurrencyLimitedStorageObject(delegate, limiter);
        expectThrows(IOException.class, obj::newStream);
        assertEquals(3, limiter.availablePermits());
    }

    /**
     * Regression guard: {@code abortStream} must (a) unwrap the {@code PermitReleasingInputStream}
     * and forward the abort to the delegate with the inner stream, and (b) release the global
     * concurrency permit. Forwarding the wrapper would cascade through {@code FilterInputStream
     * .close()} and trigger close-time drain on providers like S3.
     */
    public void testAbortStreamForwardsInnerStreamAndReleasesPermit() throws Exception {
        ConcurrencyLimiter limiter = new ConcurrencyLimiter(3);
        StorageObject delegate = mock(StorageObject.class);
        InputStream inner = new ByteArrayInputStream("hello".getBytes(StandardCharsets.UTF_8));
        when(delegate.newStream()).thenReturn(inner);
        when(delegate.path()).thenReturn(StoragePath.of("s3://bucket/key"));

        ConcurrencyLimitedStorageObject obj = new ConcurrencyLimitedStorageObject(delegate, limiter);
        InputStream wrapper = obj.newStream();
        assertEquals(2, limiter.availablePermits());

        obj.abortStream(wrapper);

        verify(delegate).abortStream(inner);
        assertEquals("permit must be released by abortStream", 3, limiter.availablePermits());
    }

    /**
     * Regression guard: {@code close()} on a wrapper that was already aborted must not
     * re-release the global permit. Two releases for one acquire would over-grant capacity
     * and break the global hard cap.
     */
    public void testCloseAfterAbortDoesNotDoubleReleasePermit() throws Exception {
        ConcurrencyLimiter limiter = new ConcurrencyLimiter(3);
        StorageObject delegate = mock(StorageObject.class);
        when(delegate.newStream()).thenReturn(new ByteArrayInputStream("hello".getBytes(StandardCharsets.UTF_8)));
        when(delegate.path()).thenReturn(StoragePath.of("s3://bucket/key"));

        ConcurrencyLimitedStorageObject obj = new ConcurrencyLimitedStorageObject(delegate, limiter);
        InputStream wrapper = obj.newStream();
        assertEquals(2, limiter.availablePermits());

        obj.abortStream(wrapper);
        assertEquals(3, limiter.availablePermits());

        wrapper.close();
        assertEquals("close after abort must not re-release the permit", 3, limiter.availablePermits());
    }

    /**
     * Regression guard: if the delegate's {@code abortStream} throws, the permit must still
     * be released — otherwise a single I/O failure during abort would permanently shrink the
     * global concurrency cap.
     */
    public void testAbortStreamReleasesPermitEvenIfDelegateThrows() throws Exception {
        ConcurrencyLimiter limiter = new ConcurrencyLimiter(3);
        StorageObject delegate = mock(StorageObject.class);
        when(delegate.newStream()).thenReturn(new ByteArrayInputStream("hello".getBytes(StandardCharsets.UTF_8)));
        when(delegate.path()).thenReturn(StoragePath.of("s3://bucket/key"));
        doAnswer(inv -> { throw new IOException("abort failed"); }).when(delegate).abortStream(any(InputStream.class));

        ConcurrencyLimitedStorageObject obj = new ConcurrencyLimitedStorageObject(delegate, limiter);
        InputStream wrapper = obj.newStream();
        assertEquals(2, limiter.availablePermits());

        expectThrows(IOException.class, () -> obj.abortStream(wrapper));
        assertEquals("permit must be released even if delegate.abortStream throws", 3, limiter.availablePermits());
        verify(delegate, times(1)).abortStream(any(InputStream.class));
    }
}
