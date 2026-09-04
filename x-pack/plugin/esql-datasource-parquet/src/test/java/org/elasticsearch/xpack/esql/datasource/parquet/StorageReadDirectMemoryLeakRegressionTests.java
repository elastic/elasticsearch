/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Regression test: every
 * {@link StorageObject#readBytesAsync(long, long, DirectBufferFactory, java.util.concurrent.Executor, ActionListener)}
 * call must allocate through the supplied {@link DirectBufferFactory} so the caller can release
 * the memory deterministically.
 *
 * <p>The checks below pin the contract from both sides:
 * <ul>
 *   <li>When the caller closes the returned {@link DirectReadBuffer}, the circuit breaker drops
 *       back to baseline — proving the charge is released.</li>
 *   <li>When the caller does <b>not</b> close the {@link DirectReadBuffer}, the breaker grows
 *       monotonically — proving that the memory was routed through the factory (not a hidden
 *       {@code allocateDirect}).</li>
 * </ul>
 *
 * <p>The storage backend is a trivial in-memory stub that exercises the default
 * {@link StorageObject#readBytesAsync} implementation; it is the same code path used by every
 * backend that does not override that method.
 */
public class StorageReadDirectMemoryLeakRegressionTests extends ESTestCase {

    private static final int PAYLOAD_SIZE = 8 * 1024;
    private static final int CYCLES = 256;

    public void testReadBytesAsyncReleasesBreakerOnClose() throws Exception {
        byte[] payload = randomByteArrayOfLength(PAYLOAD_SIZE);
        StorageObject storage = new InMemoryStorageObject(payload);

        CircuitBreaker breaker = new LimitedBreaker("test", ByteSizeValue.ofMb(64));
        DirectBufferFactory factory = DirectBufferFactory.forBreaker(breaker);
        assertEquals("breaker starts empty", 0L, breaker.getUsed());

        for (int i = 0; i < CYCLES; i++) {
            PlainActionFuture<DirectReadBuffer> future = new PlainActionFuture<>();
            storage.readBytesAsync(0, PAYLOAD_SIZE, factory, Runnable::run, future);
            DirectReadBuffer result = future.actionGet();
            try {
                assertEquals(PAYLOAD_SIZE, result.buffer().remaining());
                assertFalse("readBytesAsync must return a heap buffer", result.buffer().isDirect());
                assertEquals("breaker must hold exactly the in-flight payload", PAYLOAD_SIZE, breaker.getUsed());
            } finally {
                result.close();
            }
            assertEquals("breaker must be empty after each cycle once the DirectReadBuffer is closed", 0L, breaker.getUsed());
        }
    }

    public void testReadBytesAsyncLeaksWithoutClose() throws Exception {
        byte[] payload = randomByteArrayOfLength(PAYLOAD_SIZE);
        StorageObject storage = new InMemoryStorageObject(payload);

        // We deliberately do NOT close the DirectReadBuffers within the loop — the test asserts
        // that bytes are actually routed through the factory (and therefore visible as an
        // outstanding breaker charge), as opposed to being allocated behind the factory's back
        // via ByteBuffer.allocateDirect. The DirectReadBuffers are closed in the finally block.
        CircuitBreaker breaker = new LimitedBreaker("test", ByteSizeValue.ofMb(64));
        DirectBufferFactory factory = DirectBufferFactory.forBreaker(breaker);
        int cyclesBeforeLeakCheck = 16;
        List<DirectReadBuffer> kept = new ArrayList<>(cyclesBeforeLeakCheck);
        try {
            long previous = 0L;
            for (int i = 0; i < cyclesBeforeLeakCheck; i++) {
                PlainActionFuture<DirectReadBuffer> future = new PlainActionFuture<>();
                storage.readBytesAsync(0, PAYLOAD_SIZE, factory, Runnable::run, future);
                DirectReadBuffer result = future.actionGet();
                kept.add(result);
                assertEquals(PAYLOAD_SIZE, result.buffer().remaining());

                long current = breaker.getUsed();
                assertTrue(
                    "Charge must grow with each cycle when nothing is released; previous=" + previous + ", current=" + current,
                    current > previous
                );
                previous = current;
            }
            assertTrue(
                "Total outstanding charge must be at least cycles * payload size",
                breaker.getUsed() >= (long) cyclesBeforeLeakCheck * PAYLOAD_SIZE
            );
        } finally {
            for (DirectReadBuffer r : kept) {
                r.close();
            }
            assertEquals("breaker must be empty after closing kept buffers", 0L, breaker.getUsed());
        }
    }

    /**
     * In-memory {@link StorageObject} that uses the default {@code readBytesAsync} implementation
     * supplied by {@link StorageObject}. This stub deliberately does not override it so the test
     * exercises exactly that code path.
     */
    private static final class InMemoryStorageObject implements StorageObject {
        private final byte[] data;

        InMemoryStorageObject(byte[] data) {
            this.data = data;
        }

        @Override
        public InputStream newStream() {
            return new ByteArrayInputStream(data);
        }

        @Override
        public InputStream newStream(long position, long length) {
            int pos = (int) position;
            int len = (int) Math.min(length, data.length - position);
            return new ByteArrayInputStream(data, pos, len);
        }

        @Override
        public long length() {
            return data.length;
        }

        @Override
        public Instant lastModified() {
            return Instant.EPOCH;
        }

        @Override
        public boolean exists() {
            return true;
        }

        @Override
        public StoragePath path() {
            return StoragePath.of("memory://leak-regression");
        }
    }
}
