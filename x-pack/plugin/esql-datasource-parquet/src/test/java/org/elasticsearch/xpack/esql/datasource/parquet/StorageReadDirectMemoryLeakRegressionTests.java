/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
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
 * Regression test for the direct-memory leak fixed in esql-planning#851: every
 * {@link StorageObject#readBytesAsync(long, long, DirectBufferFactory, java.util.concurrent.Executor, ActionListener)}
 * call must allocate through the supplied {@link DirectBufferFactory} (which is backed by a
 * {@link BufferAllocator}) so the caller can release the memory
 * deterministically.
 *
 * <p>The checks below pin the contract from both sides:
 * <ul>
 *   <li>When the caller closes the returned {@link DirectReadBuffer}, the parent allocator's
 *       {@code getAllocatedMemory()} drops back to baseline — proving the reference count
 *       reaches zero and memory is returned.</li>
 *   <li>When the caller does <b>not</b> close the {@link DirectReadBuffer}, the parent
 *       allocator's {@code getAllocatedMemory()} grows monotonically — proving that the
 *       memory was routed through the allocator (not a hidden {@code allocateDirect}), and
 *       that closing the allocator alone is not what releases it.</li>
 * </ul>
 *
 * <p>The storage backend is a trivial in-memory stub that exercises the default
 * {@link StorageObject#readBytesAsync} implementation; it is the same code path used by every
 * backend that does not override that method.
 */
public class StorageReadDirectMemoryLeakRegressionTests extends ESTestCase {

    private static final int PAYLOAD_SIZE = 8 * 1024;
    private static final int CYCLES = 256;

    public void testReadBytesAsyncReleasesAllocatorMemoryOnClose() throws Exception {
        byte[] payload = randomByteArrayOfLength(PAYLOAD_SIZE);
        StorageObject storage = new InMemoryStorageObject(payload);

        try (RootAllocator root = new RootAllocator(Long.MAX_VALUE)) {
            assertEquals("RootAllocator starts empty", 0L, root.getAllocatedMemory());
            DirectBufferFactory factory = DirectBufferFactory.forAllocator(root);

            for (int i = 0; i < CYCLES; i++) {
                PlainActionFuture<DirectReadBuffer> future = new PlainActionFuture<>();
                storage.readBytesAsync(0, PAYLOAD_SIZE, factory, Runnable::run, future);
                DirectReadBuffer result = future.actionGet();
                try {
                    assertEquals(PAYLOAD_SIZE, result.buffer().remaining());
                    assertTrue("readBytesAsync must return a direct buffer", result.buffer().isDirect());
                    assertEquals("RootAllocator must hold exactly the in-flight payload", PAYLOAD_SIZE, root.getAllocatedMemory());
                } finally {
                    result.close();
                }
                assertEquals(
                    "RootAllocator must be empty after each cycle once the DirectReadBuffer is closed",
                    0L,
                    root.getAllocatedMemory()
                );
            }
        }
    }

    public void testReadBytesAsyncLeaksWithoutClose() throws Exception {
        byte[] payload = randomByteArrayOfLength(PAYLOAD_SIZE);
        StorageObject storage = new InMemoryStorageObject(payload);

        // We deliberately do NOT close the DirectReadBuffers within the loop — the test asserts
        // that direct memory is actually routed through the allocator (and therefore visible as
        // an outstanding allocation), as opposed to being allocated behind the allocator's back
        // via ByteBuffer.allocateDirect (which is what the original bug did). The DirectReadBuffers
        // are closed in the finally block so the RootAllocator can shut down cleanly.
        try (RootAllocator root = new RootAllocator(Long.MAX_VALUE)) {
            DirectBufferFactory factory = DirectBufferFactory.forAllocator(root);
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

                    long current = root.getAllocatedMemory();
                    assertTrue(
                        "Allocation must grow with each cycle when nothing is released; previous=" + previous + ", current=" + current,
                        current > previous
                    );
                    previous = current;
                }
                assertTrue(
                    "Total outstanding memory must be at least cycles * payload size",
                    root.getAllocatedMemory() >= (long) cyclesBeforeLeakCheck * PAYLOAD_SIZE
                );
            } finally {
                for (DirectReadBuffer r : kept) {
                    r.close();
                }
            }
        }
    }

    /**
     * Sanity check that the production {@link BlockFactory#arrowAllocator()} also routes the
     * allocation correctly — production code paths use this allocator rather than a bare
     * {@link RootAllocator}.
     */
    public void testReadBytesAsyncThroughBlockFactoryAllocator() throws Exception {
        byte[] payload = randomByteArrayOfLength(PAYLOAD_SIZE);
        StorageObject storage = new InMemoryStorageObject(payload);

        BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("test")).build();
        BufferAllocator allocator = blockFactory.arrowAllocator();
        DirectBufferFactory factory = DirectBufferFactory.forAllocator(allocator);
        long baseline = allocator.getAllocatedMemory();

        for (int i = 0; i < CYCLES; i++) {
            PlainActionFuture<DirectReadBuffer> future = new PlainActionFuture<>();
            storage.readBytesAsync(0, PAYLOAD_SIZE, factory, Runnable::run, future);
            DirectReadBuffer result = future.actionGet();
            try {
                assertEquals(PAYLOAD_SIZE, result.buffer().remaining());
                assertTrue(result.buffer().isDirect());
            } finally {
                result.close();
            }
            assertEquals(
                "BlockFactory's arrow allocator must be back at baseline after each cycle",
                baseline,
                allocator.getAllocatedMemory()
            );
        }
    }

    /**
     * In-memory {@link StorageObject} that uses the default {@code readBytesAsync} implementation
     * supplied by {@link StorageObject}. The default impl is the one that allocates through the
     * supplied {@link BufferAllocator}; this stub deliberately does not override it so the test
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
