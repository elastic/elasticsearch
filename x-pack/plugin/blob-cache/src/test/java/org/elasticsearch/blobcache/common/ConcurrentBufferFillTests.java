/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache.common;

import org.elasticsearch.core.Assertions;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ConcurrentBufferFillTests extends ESTestCase {

    private static final int COMPLETE = -1;

    public void testHeapBuffer() {
        final byte[] data = randomByteArrayOfLength(randomIntBetween(1, 1024));
        final ByteBuffer buffer = ByteBuffer.allocate(data.length + randomIntBetween(0, 16));
        final ConcurrentBufferFill fill = new ConcurrentBufferFill(buffer);

        final int written = fill.fillSlice(0, data.length, COMPLETE, slice -> {
            slice.put(data);
            return data.length;
        });
        assertThat(written, equalTo(data.length));
        fill.complete(written);

        assertThat(buffer.position(), equalTo(data.length));
        assertArrayEquals(data, Arrays.copyOf(buffer.array(), data.length));
        assertComplete(fill);
    }

    // A direct buffer that is not backed by a segment is filled by other threads
    // like any other, starting from its position.
    public void testDirectBuffer() throws Exception {
        final byte[] data = randomByteArrayOfLength(randomIntBetween(2, 1024));
        final int prefix = randomIntBetween(0, data.length - 1);
        final int offset = randomIntBetween(0, 16);
        final ByteBuffer buffer = ByteBuffer.allocateDirect(offset + data.length + randomIntBetween(0, 16));
        buffer.position(offset);
        final ConcurrentBufferFill fill = new ConcurrentBufferFill(buffer);

        writeOnOtherThread(fill, 0, data, 0, prefix);
        writeOnOtherThread(fill, prefix, data, prefix, data.length - prefix);
        fill.complete(data.length);

        assertThat(buffer.position(), equalTo(offset + data.length));
        final byte[] actual = new byte[data.length];
        buffer.get(offset, actual);
        assertArrayEquals(data, actual);
        assertComplete(fill);
        onOtherThread(() -> assertComplete(fill));
    }

    // The scratch buffer of a confined arena is filled by another thread, as a cache read
    // completed by a fetch thread would do, and the owner then sees the data and may close
    // the arena.
    public void testConfinedArenaFilledByOtherThread() throws Exception {
        final byte[] data = randomByteArrayOfLength(randomIntBetween(2, 4096));
        final int prefix = randomIntBetween(0, data.length - 1);
        final ConcurrentBufferFill fill;
        try (Arena arena = Arena.ofConfined()) {
            final MemorySegment scratch = arena.allocate(data.length);
            final ByteBuffer buffer = scratch.asByteBuffer();
            fill = new ConcurrentBufferFill(buffer);

            // two writers, each with its own slice, as the cache does when a read spans regions
            writeOnOtherThread(fill, 0, data, 0, prefix);
            writeOnOtherThread(fill, prefix, data, prefix, data.length - prefix);
            fill.complete(data.length);
            assertThat(buffer.position(), equalTo(data.length));
            for (int i = 0; i < data.length; i++) {
                assertThat("byte mismatch at " + i, scratch.get(ValueLayout.JAVA_BYTE, i), equalTo(data[i]));
            }
            // after the defined point nothing can be done with the buffer, so nothing can reach the memory that is about to be freed
            assertComplete(fill);
            onOtherThread(() -> assertComplete(fill));
        }
        // and still nothing once the arena is closed
        assertComplete(fill);
        onOtherThread(() -> assertComplete(fill));
    }

    // {@link ConcurrentBufferFill#complete} does not return while a filler is running,
    // so the owner cannot free memory in use.
    public void testCompleteWaitsForFillerInUse() throws Exception {
        final byte[] data = randomByteArrayOfLength(randomIntBetween(1, 1024));
        final Thread owner = Thread.currentThread();
        final CountDownLatch inAction = new CountDownLatch(1);
        try (Arena arena = Arena.ofConfined()) {
            final MemorySegment scratch = arena.allocate(data.length);
            final ConcurrentBufferFill fill = new ConcurrentBufferFill(scratch.asByteBuffer());
            final AtomicReference<Throwable> writerFailure = new AtomicReference<>();

            final Thread writer = new Thread(() -> {
                try {
                    fill.fillSlice(0, data.length, COMPLETE, slice -> {
                        inAction.countDown();
                        // only write once the owner is blocked in complete, which must therefore be waiting for this action
                        assertBusy(
                            () -> assertThat(owner.getState(), anyOf(equalTo(Thread.State.WAITING), equalTo(Thread.State.TIMED_WAITING)))
                        );
                        slice.put(data);
                        return data.length;
                    });
                } catch (Throwable t) {
                    writerFailure.set(t);
                }
            }, "test-writer");
            writer.start();
            safeAwait(inAction);

            // as when a read failed or was abandoned, which is when a slice can still be in use: no bytes are accounted for
            fill.complete(0);

            // complete returned, so the write is complete and visible
            assertThat(writerFailure.get(), nullValue());
            for (int i = 0; i < data.length; i++) {
                assertThat("byte mismatch at " + i, scratch.get(ValueLayout.JAVA_BYTE, i), equalTo(data[i]));
            }
            writer.join();
        }
    }

    public void testOutOfBoundsSliceDoesNotLeakPermit() throws Exception {
        final int size = randomIntBetween(1, 64);
        for (boolean direct : new boolean[] { false, true }) {
            try (Arena arena = Arena.ofConfined()) {
                final ByteBuffer buffer = direct ? arena.allocate(size).asByteBuffer() : ByteBuffer.allocate(size);
                final ConcurrentBufferFill fill = new ConcurrentBufferFill(buffer);
                final AtomicBoolean ran = new AtomicBoolean();
                expectThrows(IndexOutOfBoundsException.class, () -> fill.fillSlice(randomIntBetween(1, size), size, COMPLETE, slice -> {
                    ran.set(true);
                    return 0;
                }));
                expectThrows(IndexOutOfBoundsException.class, () -> fill.fillSlice(-1, 1, COMPLETE, slice -> {
                    ran.set(true);
                    return 0;
                }));
                assertFalse("the filler must not run for a slice that is out of bounds", ran.get());
                // a leaked permit would make this block forever
                fill.complete(0);
                assertComplete(fill);
            }
        }
    }

    /**
     * Fillers racing with {@link ConcurrentBufferFill#complete} either run to completion before it returns, and are then visible to the
     * owner, or do not run at all. The arena is closed as soon as {@code complete} returns, and the fillers are only joined afterwards, so
     * a filler that wrote after that point would be writing to freed memory.
     */
    public void testFillersRacingCompleteNeverRunAfterItReturns() throws Exception {
        final int fillers = 8;
        final int chunk = 64;
        for (int iteration = 0; iteration < 50; iteration++) {
            final Thread[] threads = new Thread[fillers];
            final AtomicIntegerArray ran = new AtomicIntegerArray(fillers);
            final AtomicInteger running = new AtomicInteger();
            final AtomicBoolean completed = new AtomicBoolean();
            final AtomicReference<Throwable> failure = new AtomicReference<>();
            final CountDownLatch start = new CountDownLatch(1);
            final byte[] filled = new byte[fillers * chunk];
            try (Arena arena = Arena.ofConfined()) {
                final MemorySegment scratch = arena.allocate(fillers * chunk);
                final ConcurrentBufferFill fill = new ConcurrentBufferFill(scratch.asByteBuffer());
                for (int i = 0; i < fillers; i++) {
                    final int index = i;
                    threads[i] = new Thread(() -> {
                        try {
                            start.await();
                            fill.fillSlice(index * chunk, chunk, COMPLETE, slice -> {
                                running.incrementAndGet();
                                // never runs once complete has returned, which sets this before the arena is closed
                                assertFalse("a filler started after complete returned", completed.get());
                                while (slice.hasRemaining()) {
                                    slice.put((byte) (index + 1));
                                }
                                ran.set(index, 1);
                                running.decrementAndGet();
                                return chunk;
                            });
                        } catch (Throwable t) {
                            failure.compareAndSet(null, t);
                        }
                    }, "test-filler-" + i);
                    threads[i].start();
                }
                start.countDown();
                for (int spin = randomIntBetween(0, 2000); spin > 0; spin--) {
                    Thread.onSpinWait();
                }

                fill.complete(0);
                assertThat("no filler may be running once complete returned", running.get(), equalTo(0));
                completed.set(true);
                // the fillers that ran are visible, and the ones that did not left their region alone
                scratch.asByteBuffer().get(0, filled);
                for (int i = 0; i < fillers; i++) {
                    final byte expected = ran.get(i) == 1 ? (byte) (i + 1) : 0;
                    for (int j = 0; j < chunk; j++) {
                        assertThat("filler " + i + " byte " + j, filled[i * chunk + j], equalTo(expected));
                    }
                }
            }
            for (Thread thread : threads) {
                thread.join();
            }
            assertThat(failure.get(), nullValue());
        }
    }

    public void testOverlappingRegionsAreRejected() throws Exception {
        assumeTrue("the check needs assertions enabled", Assertions.ENABLED);
        final ConcurrentBufferFill fill = new ConcurrentBufferFill(ByteBuffer.allocate(16));
        fillWith(fill, 4, 4);
        // adjacent and disjoint regions are fine, and so is an empty one wherever it is
        fillWith(fill, 0, 4);
        fillWith(fill, 8, 8);
        fillWith(fill, 5, 0);
        // overlapping from the left, from the right, within, around, and exactly the same
        for (int[] region : new int[][] { { 2, 3 }, { 7, 2 }, { 5, 2 }, { 3, 6 }, { 4, 4 }, { 0, 16 } }) {
            final AtomicBoolean ran = new AtomicBoolean();
            expectThrows(AssertionError.class, () -> fill.fillSlice(region[0], region[1], COMPLETE, slice -> {
                ran.set(true);
                return 0;
            }));
            assertFalse("the filler must not run for an overlapping region " + Arrays.toString(region), ran.get());
        }
        // a rejected region does not leak a permit
        fill.complete(0);
        assertComplete(fill);
    }

    // Starting over with a new fill of the same buffer, as the bypass read does, may fill regions that the first fill did.
    public void testNewFillAfterCompleteMayRefillSameRegions() throws Exception {
        final ByteBuffer buffer = ByteBuffer.allocate(16);
        final ConcurrentBufferFill first = new ConcurrentBufferFill(buffer);
        fillWith(first, 0, 8);
        first.complete(0);
        final ConcurrentBufferFill second = new ConcurrentBufferFill(buffer);
        fillWith(second, 0, 16);
        // the first fill is over, so a late filler of the failed attempt does not write, and does not claim anything from the second
        assertComplete(first);
        second.complete(16);
        assertThat(buffer.position(), equalTo(16));
    }

    public void testCompleteRejectsMoreBytesThanClaimed() {
        assumeTrue("the check needs assertions enabled", Assertions.ENABLED);
        final ConcurrentBufferFill fill = new ConcurrentBufferFill(ByteBuffer.allocate(16));
        fillWith(fill, 0, 4);
        fillWith(fill, 8, 4);
        expectThrows(AssertionError.class, () -> fill.complete(9));
    }

    // The owner must not be able to free the buffer while another thread is still completing the fill.
    public void testCompleteOnAnotherThreadIsRejected() throws Exception {
        assumeTrue("the check needs assertions enabled", Assertions.ENABLED);
        final ConcurrentBufferFill fill = new ConcurrentBufferFill(ByteBuffer.allocate(16));
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        onOtherThread(() -> {
            try {
                fill.complete(0);
            } catch (AssertionError e) {
                failure.set(e);
            }
        });
        assertThat(failure.get(), instanceOf(AssertionError.class));
        // the rejected call left the fill alone, so the owner can still complete it
        fill.complete(0);
        assertComplete(fill);
    }

    public void testFailingActionDoesNotLeakPermit() throws Exception {
        try (Arena arena = Arena.ofConfined()) {
            final ConcurrentBufferFill fill = new ConcurrentBufferFill(arena.allocate(8).asByteBuffer());
            expectThrows(IOException.class, () -> fill.fillSlice(0, 8, COMPLETE, slice -> { throw new IOException("simulated"); }));
            // a leaked permit would make this block forever
            fill.complete(0);
            assertComplete(fill);
        }
    }

    /** Interrupting the owner must not let it stop waiting for fillers in use and go on to free the memory. */
    public void testCompleteIgnoresInterrupt() throws Exception {
        try (Arena arena = Arena.ofConfined()) {
            final ConcurrentBufferFill fill = new ConcurrentBufferFill(arena.allocate(8).asByteBuffer());
            Thread.currentThread().interrupt();
            try {
                fill.complete(0);
                assertTrue("interrupt status is preserved", Thread.currentThread().isInterrupted());
            } finally {
                // clear the status so that it does not leak to other tests
                Thread.interrupted();
            }
            assertComplete(fill);
        }
    }

    public void testScopedBufferOfConfinedArenaCannotBeUsedDirectlyByOtherThreads() throws Exception {
        try (Arena arena = Arena.ofConfined()) {
            final ByteBuffer buffer = arena.allocate(8).asByteBuffer();
            final AtomicReference<Throwable> failure = new AtomicReference<>();
            onOtherThread(() -> {
                try {
                    buffer.put((byte) 1);
                } catch (Throwable t) {
                    failure.set(t);
                }
            });
            assertThat(failure.get(), instanceOf(WrongThreadException.class));
        }
    }

    // Asserts that the fill is complete, so that it no longer lets anything touch
    // the buffer: the filler is not run and the fallback value is returned.
    private static void assertComplete(ConcurrentBufferFill fill) {
        final int result = fill.fillSlice(0, 1, COMPLETE, slice -> {
            throw new AssertionError("the filler must not run once the fill is complete");
        });
        assertThat(result, equalTo(COMPLETE));
    }

    private static void fillWith(ConcurrentBufferFill fill, int position, int length) {
        fill.fillSlice(position, length, COMPLETE, slice -> {
            assertThat(slice.remaining(), equalTo(length));
            return length;
        });
    }

    private static void writeOnOtherThread(ConcurrentBufferFill fill, int position, byte[] data, int from, int length) {
        onOtherThread(() -> {
            final int written = fill.fillSlice(position, length, COMPLETE, slice -> {
                slice.put(data, from, length);
                return length;
            });
            assertThat(written, notNullValue());
            assertThat(written, equalTo(length));
        });
    }
}
