/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;

/**
 * Verifies that closing a {@link DirectReadBuffer} severs ownership of its payload while retaining
 * deterministic diagnostics for invalid lifecycle use.
 */
public class DirectReadBufferTests extends ESTestCase {

    public void testCloseDetachesBackingBuffer() throws Exception {
        DirectReadBuffer owner = new DirectReadBuffer(ByteBuffer.allocate(1 << 20), () -> {});
        WeakReference<byte[]> backing = closeAndForgetBacking(owner);

        try {
            assertBusy(() -> {
                System.gc();
                assertNull("a closed owner must not retain its backing array", backing.get());
            });
        } finally {
            Reference.reachabilityFence(owner);
        }
    }

    public void testBufferAfterClose() {
        DirectReadBuffer owner = new DirectReadBuffer(ByteBuffer.allocate(8), () -> {});
        owner.close();

        IllegalStateException error = expectThrows(IllegalStateException.class, owner::buffer);
        assertThat(error.getMessage(), containsString("DirectReadBuffer"));
        assertThat(error.getMessage(), containsString("closed"));
        if (DirectMemoryDebug.trackingEnabled()) {
            assertEquals(2, error.getSuppressed().length);
            assertThat(error.getSuppressed()[0].getMessage(), containsString("allocated here"));
            assertThat(error.getSuppressed()[1].getMessage(), containsString("freed here"));
        }
    }

    public void testRequireWritableWindowNormalizesOverAllocatedBuffer() throws Exception {
        ByteBuffer destination = ByteBuffer.allocate(32);
        destination.limit(1);
        DirectReadBuffer owner = new DirectReadBuffer(destination, () -> {});
        owner.requireWritableWindow(8);
        assertEquals(8, owner.buffer().remaining());
        assertEquals(32, owner.buffer().capacity());
    }

    public void testAllocateWritableWindowRejectsInvalidFactoryBuffers() {
        AtomicInteger closeCalls = new AtomicInteger();
        DirectBufferFactory undersized = ignored -> new DirectReadBuffer(ByteBuffer.allocate(15), closeCalls::incrementAndGet);
        IOException undersizedError = expectThrows(IOException.class, () -> undersized.allocateWritableWindow(16));
        assertThat(undersizedError.getMessage(), containsString("DirectBufferFactory"));
        assertEquals(1, closeCalls.get());

        DirectBufferFactory readOnly = ignored -> new DirectReadBuffer(
            ByteBuffer.allocateDirect(16).asReadOnlyBuffer(),
            closeCalls::incrementAndGet
        );
        IOException readOnlyError = expectThrows(IOException.class, () -> readOnly.allocateWritableWindow(16));
        assertThat(readOnlyError.getMessage(), containsString("DirectBufferFactory"));
        assertEquals(2, closeCalls.get());
    }

    public void testDoubleCloseWithTracking() {
        assumeTrue("requires debug tracking", DirectMemoryDebug.trackingEnabled());
        DirectReadBuffer owner = new DirectReadBuffer(ByteBuffer.allocate(8), () -> {});
        owner.close();

        AssertionError error = expectThrows(AssertionError.class, owner::close);
        assertThat(error.getMessage(), containsString("called twice"));
        assertEquals(2, error.getSuppressed().length);
    }

    public void testConcurrentCloseReleasesBackingBufferOnce() throws InterruptedException {
        AtomicInteger closeCalls = new AtomicInteger();
        AtomicReference<AssertionError> doubleClose = new AtomicReference<>();
        DirectReadBuffer owner = new DirectReadBuffer(ByteBuffer.allocate(8), closeCalls::incrementAndGet);

        startInParallel(2, ignored -> {
            try {
                owner.close();
            } catch (AssertionError e) {
                if (doubleClose.compareAndSet(null, e) == false) {
                    throw e;
                }
            }
        });

        assertEquals(1, closeCalls.get());
        if (DirectMemoryDebug.trackingEnabled()) {
            assertNotNull(doubleClose.get());
            assertThat(doubleClose.get().getMessage(), containsString("called twice"));
        } else {
            assertNull(doubleClose.get());
        }
    }

    public void testClosePoisonsDetachedDirectBufferSnapshot() {
        // Header poisoning also runs without tracking, but tracking poisons the whole buffer and
        // therefore verifies that close retained a snapshot before detaching the owner.
        assumeTrue("requires full-buffer poison", DirectMemoryDebug.trackingEnabled());
        ByteBuffer buffer = ByteBuffer.allocateDirect(257);
        ByteBuffer alias = buffer.duplicate();
        DirectReadBuffer owner = new DirectReadBuffer(buffer, () -> {});

        owner.close();

        alias.clear();
        while (alias.remaining() >= Integer.BYTES) {
            assertEquals(0xBADBADBA, alias.getInt());
        }
        while (alias.hasRemaining()) {
            assertEquals((byte) 0xDE, alias.get());
        }
    }

    private static WeakReference<byte[]> closeAndForgetBacking(DirectReadBuffer owner) {
        WeakReference<byte[]> backing = new WeakReference<>(owner.buffer().array());
        owner.close();
        return backing;
    }
}
