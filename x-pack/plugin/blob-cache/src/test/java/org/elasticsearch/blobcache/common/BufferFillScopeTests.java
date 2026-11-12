/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache.common;

import com.carrotsearch.randomizedtesting.annotations.Timeout;

import org.apache.lucene.util.Constants;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class BufferFillScopeTests extends ESTestCase {

    /** Bound for tests where a bug in the scope, such as a leaked permit, would make {@code complete} wait forever. */
    private static final int TIMEOUT_MILLIS = 30_000;

    public void testHeapBuffer() {
        final byte[] data = randomByteArrayOfLength(randomIntBetween(1, 1024));
        final ByteBuffer buffer = ByteBuffer.allocate(data.length + randomIntBetween(0, 16));
        final BufferFillScope scope = BufferFillScope.of(buffer);

        final int written = scope.fillSlice(0, data.length, slice -> {
            slice.put(data);
            return data.length;
        }).orElseThrow();
        assertThat(written, equalTo(data.length));
        scope.complete(written);

        assertThat(buffer.position(), equalTo(data.length));
        assertArrayEquals(data, Arrays.copyOf(buffer.array(), data.length));
        assertComplete(scope);
    }

    // A direct buffer that is not backed by a segment is filled by other threads
    // like any other, starting from its position.
    public void testDirectBuffer() throws Exception {
        final byte[] data = randomByteArrayOfLength(randomIntBetween(2, 1024));
        final int prefix = randomIntBetween(0, data.length - 1);
        final int offset = randomIntBetween(0, 16);
        final ByteBuffer buffer = ByteBuffer.allocateDirect(offset + data.length + randomIntBetween(0, 16));
        buffer.position(offset);
        final BufferFillScope scope = BufferFillScope.of(buffer);

        writeOnOtherThread(scope, 0, data, 0, prefix);
        writeOnOtherThread(scope, prefix, data, prefix, data.length - prefix);
        scope.complete(data.length);

        assertThat(buffer.position(), equalTo(offset + data.length));
        final byte[] actual = new byte[data.length];
        buffer.get(offset, actual);
        assertArrayEquals(data, actual);
        assertComplete(scope);
        onOtherThread(() -> assertComplete(scope));
    }

    // The scratch buffer of a confined arena is filled by another thread, as a cache read
    // completed by a fetch thread would do, and the owner then sees the data and may close
    // the arena.
    @Timeout(millis = TIMEOUT_MILLIS)
    public void testConfinedArenaFilledByOtherThread() throws Exception {
        final byte[] data = randomByteArrayOfLength(randomIntBetween(2, 4096));
        final int prefix = randomIntBetween(0, data.length - 1);
        final BufferFillScope scope;
        try (Arena arena = Arena.ofConfined()) {
            final MemorySegment scratch = arena.allocate(data.length);
            final ByteBuffer buffer = scratch.asByteBuffer();
            scope = BufferFillScope.of(buffer);

            // two writers, each with its own slice, as the cache does when a read spans regions
            writeOnOtherThread(scope, 0, data, 0, prefix);
            writeOnOtherThread(scope, prefix, data, prefix, data.length - prefix);
            scope.complete(data.length);
            assertThat(buffer.position(), equalTo(data.length));
            for (int i = 0; i < data.length; i++) {
                assertThat("byte mismatch at " + i, scratch.get(ValueLayout.JAVA_BYTE, i), equalTo(data[i]));
            }
            // after the defined point nothing can be done with the buffer, so nothing can reach the memory that is about to be freed
            assertComplete(scope);
            onOtherThread(() -> assertComplete(scope));
        }
        // and still nothing once the arena is closed
        assertComplete(scope);
        onOtherThread(() -> assertComplete(scope));
    }

    // {@link BufferFillScope#complete} does not return while a filler is running,
    // so the owner cannot free memory in use.
    @Timeout(millis = TIMEOUT_MILLIS)
    public void testCompleteWaitsForFillerInUse() throws Exception {
        final byte[] data = randomByteArrayOfLength(randomIntBetween(1, 1024));
        final Thread owner = Thread.currentThread();
        final CountDownLatch inAction = new CountDownLatch(1);
        try (Arena arena = Arena.ofConfined()) {
            final MemorySegment scratch = arena.allocate(data.length);
            final BufferFillScope scope = BufferFillScope.of(scratch.asByteBuffer());
            final AtomicReference<Throwable> writerFailure = new AtomicReference<>();

            final Thread writer = new Thread(() -> {
                try {
                    scope.fillSlice(0, data.length, slice -> {
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
            scope.complete(0);

            // complete returned, so the write is complete and visible
            assertThat(writerFailure.get(), nullValue());
            for (int i = 0; i < data.length; i++) {
                assertThat("byte mismatch at " + i, scratch.get(ValueLayout.JAVA_BYTE, i), equalTo(data[i]));
            }
            writer.join();
        }
    }

    @Timeout(millis = TIMEOUT_MILLIS)
    public void testOutOfBoundsSliceDoesNotLeakPermit() throws Exception {
        final int size = randomIntBetween(1, 64);
        for (boolean direct : new boolean[] { false, true }) {
            try (Arena arena = Arena.ofConfined()) {
                final ByteBuffer buffer = direct ? arena.allocate(size).asByteBuffer() : ByteBuffer.allocate(size);
                final BufferFillScope scope = BufferFillScope.of(buffer);
                final AtomicBoolean ran = new AtomicBoolean();
                expectThrows(IndexOutOfBoundsException.class, () -> scope.fillSlice(randomIntBetween(1, size), size, slice -> {
                    ran.set(true);
                    return 0;
                }));
                expectThrows(IndexOutOfBoundsException.class, () -> scope.fillSlice(-1, 1, slice -> {
                    ran.set(true);
                    return 0;
                }));
                assertFalse("the filler must not run for a slice that is out of bounds", ran.get());
                // a leaked permit would make this block forever
                scope.complete(0);
                assertComplete(scope);
            }
        }
    }

    /**
     * Fillers racing with {@link BufferFillScope#complete} either run to completion before it returns, and are then visible to the
     * owner, or do not run at all. The arena is closed as soon as {@code complete} returns, and the fillers are only joined afterwards, so
     * a filler that wrote after that point would be writing to freed memory.
     */
    @Timeout(millis = TIMEOUT_MILLIS)
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
                final BufferFillScope scope = BufferFillScope.of(scratch.asByteBuffer());
                for (int i = 0; i < fillers; i++) {
                    final int index = i;
                    threads[i] = new Thread(() -> {
                        try {
                            start.await();
                            scope.fillSlice(index * chunk, chunk, slice -> {
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

                scope.complete(0);
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

    @Timeout(millis = TIMEOUT_MILLIS)
    public void testOverlappingRegionsAreRejected() throws Exception {
        assumeTrue("the check needs assertions enabled", Assertions.ENABLED);
        final BufferFillScope scope = BufferFillScope.of(ByteBuffer.allocate(16));
        fillWith(scope, 4, 4);
        // adjacent and disjoint regions are fine, and so is an empty one wherever it is
        fillWith(scope, 0, 4);
        fillWith(scope, 8, 8);
        fillWith(scope, 5, 0);
        // overlapping from the left, from the right, within, around, and exactly the same
        for (int[] region : new int[][] { { 2, 3 }, { 7, 2 }, { 5, 2 }, { 3, 6 }, { 4, 4 }, { 0, 16 } }) {
            final AtomicBoolean ran = new AtomicBoolean();
            expectThrows(AssertionError.class, () -> scope.fillSlice(region[0], region[1], slice -> {
                ran.set(true);
                return 0;
            }));
            assertFalse("the filler must not run for an overlapping region " + Arrays.toString(region), ran.get());
        }
        // a rejected region does not leak a permit
        scope.complete(0);
        assertComplete(scope);
    }

    // Starting over with a new scope of the same buffer, as the bypass read does, may fill regions that the first scope did.
    public void testNewFillAfterCompleteMayRefillSameRegions() throws Exception {
        final ByteBuffer buffer = ByteBuffer.allocate(16);
        final BufferFillScope first = BufferFillScope.of(buffer);
        fillWith(first, 0, 8);
        first.complete(0);
        final BufferFillScope second = BufferFillScope.of(buffer);
        fillWith(second, 0, 16);
        // the first scope is over, so a late filler of the failed attempt does not write, and does not claim anything from the second
        assertComplete(first);
        second.complete(16);
        assertThat(buffer.position(), equalTo(16));
    }

    public void testCompleteRejectsMoreBytesThanClaimed() {
        assumeTrue("the check needs assertions enabled", Assertions.ENABLED);
        final BufferFillScope scope = BufferFillScope.of(ByteBuffer.allocate(16));
        fillWith(scope, 0, 4);
        fillWith(scope, 8, 4);
        expectThrows(AssertionError.class, () -> scope.complete(9));
    }

    // The owner must not be able to free the buffer while another thread is still completing the scope.
    public void testCompleteOnAnotherThreadIsRejected() throws Exception {
        assumeTrue("the check needs assertions enabled", Assertions.ENABLED);
        final BufferFillScope scope = BufferFillScope.of(ByteBuffer.allocate(16));
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        onOtherThread(() -> {
            try {
                scope.complete(0);
            } catch (AssertionError e) {
                failure.set(e);
            }
        });
        assertThat(failure.get(), instanceOf(AssertionError.class));
        // the rejected call left the scope alone, so the owner can still complete it
        scope.complete(0);
        assertComplete(scope);
    }

    @Timeout(millis = TIMEOUT_MILLIS)
    public void testFailingActionDoesNotLeakPermit() throws Exception {
        try (Arena arena = Arena.ofConfined()) {
            final BufferFillScope scope = BufferFillScope.of(arena.allocate(8).asByteBuffer());
            expectThrows(IOException.class, () -> scope.fillSlice(0, 8, slice -> { throw new IOException("simulated"); }));
            // a leaked permit would make this block forever
            scope.complete(0);
            assertComplete(scope);
        }
    }

    /** Interrupting the owner must not let it stop waiting for fillers in use and go on to free the memory. */
    @Timeout(millis = TIMEOUT_MILLIS)
    public void testCompleteIgnoresInterrupt() throws Exception {
        try (Arena arena = Arena.ofConfined()) {
            final BufferFillScope scope = BufferFillScope.of(arena.allocate(8).asByteBuffer());
            Thread.currentThread().interrupt();
            try {
                scope.complete(0);
                assertTrue("interrupt status is preserved", Thread.currentThread().isInterrupted());
            } finally {
                // clear the status so that it does not leak to other tests
                Thread.interrupted();
            }
            assertComplete(scope);
        }
    }

    /**
     * Writing through the address of freed memory would silently corrupt it, so when assertions are enabled a filler that starts after
     * the owner freed the memory without calling {@link BufferFillScope#complete} fails instead, without running.
     */
    public void testFillAfterArenaClosedWithoutCompleteIsRejected() {
        assumeTrue("the check needs assertions enabled", Assertions.ENABLED);
        final Arena arena = Arena.ofConfined();
        final BufferFillScope scope = BufferFillScope.of(arena.allocate(8).asByteBuffer());
        // breaks the protocol: the memory is freed without completing the scope
        arena.close();
        final AtomicBoolean ran = new AtomicBoolean();
        expectThrows(AssertionError.class, () -> scope.fillSlice(0, 8, slice -> {
            ran.set(true);
            return 8;
        }));
        assertFalse("the filler must not run against freed memory", ran.get());
    }

    /** As above, for a filler that is already running when the owner frees the memory. The filler here never touches the memory. */
    @Timeout(millis = TIMEOUT_MILLIS)
    public void testArenaClosedWhileFillerRunningIsDetected() throws Exception {
        assumeTrue("the check needs assertions enabled", Assertions.ENABLED);
        final Arena arena = Arena.ofConfined();
        final BufferFillScope scope = BufferFillScope.of(arena.allocate(8).asByteBuffer());
        final CountDownLatch inFiller = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final Thread filler = new Thread(() -> {
            try {
                scope.fillSlice(0, 8, slice -> {
                    inFiller.countDown();
                    safeAwait(release);
                    return 8;
                });
            } catch (Throwable t) {
                failure.set(t);
            }
        }, "test-filler");
        filler.start();
        safeAwait(inFiller);
        // breaks the protocol: the memory is freed, without completing the scope, while a filler holds a permit
        arena.close();
        release.countDown();
        filler.join();
        assertThat(failure.get(), instanceOf(AssertionError.class));
    }

    /** The scope of a confined arena's buffer must be created by the owner thread, which is also the one that must complete it. */
    public void testScopeCreatedOnAnotherThreadThanTheOwnerIsRejected() throws Exception {
        assumeTrue("the check needs assertions enabled", Assertions.ENABLED);
        try (Arena arena = Arena.ofConfined()) {
            final ByteBuffer buffer = arena.allocate(8).asByteBuffer();
            final AtomicReference<Throwable> failure = new AtomicReference<>();
            onOtherThread(() -> {
                try {
                    BufferFillScope.of(buffer);
                } catch (AssertionError e) {
                    failure.set(e);
                }
            });
            assertThat(failure.get(), instanceOf(AssertionError.class));
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

    /** The kinds of buffer that a scope may be given. Only the memory of a confined arena is filled through its address. */
    private enum BufferKind {
        HEAP,
        HEAP_SLICE,
        HEAP_SEGMENT,
        DIRECT,
        DIRECT_SLICE,
        MAPPED,
        CONFINED_ARENA,
        CONFINED_ARENA_SLICE,
        SHARED_ARENA,
        SHARED_ARENA_SLICE,
        AUTO_ARENA
    }

    // The slices are at an offset in the buffer, which is not the start of its array or memory, for the kinds that can be.
    private static ByteBuffer newBuffer(BufferKind kind, int size, Arena confined, Arena shared, Path dir) throws IOException {
        return switch (kind) {
            case HEAP -> ByteBuffer.allocate(size);
            case HEAP_SLICE -> ByteBuffer.allocate(size + 7).slice(3, size);
            case HEAP_SEGMENT -> MemorySegment.ofArray(new byte[size]).asByteBuffer();
            case DIRECT -> ByteBuffer.allocateDirect(size);
            case DIRECT_SLICE -> ByteBuffer.allocateDirect(size + 7).slice(3, size);
            case MAPPED -> {
                try (FileChannel channel = FileChannel.open(dir.resolve("mapped"), READ, WRITE, CREATE)) {
                    yield channel.map(READ_WRITE, 0, size);
                }
            }
            case CONFINED_ARENA -> confined.allocate(size).asByteBuffer();
            case CONFINED_ARENA_SLICE -> confined.allocate(size + 7).asSlice(3, size).asByteBuffer();
            case SHARED_ARENA -> shared.allocate(size).asByteBuffer();
            case SHARED_ARENA_SLICE -> shared.allocate(size + 7).asSlice(3, size).asByteBuffer();
            case AUTO_ARENA -> Arena.ofAuto().allocate(size).asByteBuffer();
        };
    }

    // Whatever the buffer is, other threads can fill disjoint slices of it, starting from its position, and the owner sees the bytes.
    public void testFilledByOtherThreadsForEveryKindOfBuffer() throws Exception {
        final Path dir = createTempDir();
        for (BufferKind kind : BufferKind.values()) {
            if (kind == BufferKind.MAPPED && Constants.WINDOWS) {
                continue; // a file cannot be deleted while it is mapped, which the cleanup of the test directory would try to do
            }
            final byte[] data = randomByteArrayOfLength(randomIntBetween(2, 4096));
            final int offset = randomIntBetween(0, 16);
            final int prefix = randomIntBetween(1, data.length - 1);
            try (Arena confined = Arena.ofConfined(); Arena shared = Arena.ofShared()) {
                final ByteBuffer buffer = newBuffer(kind, offset + data.length + randomIntBetween(0, 16), confined, shared, dir);
                buffer.position(offset);
                final BufferFillScope scope = BufferFillScope.of(buffer);

                writeOnOtherThread(scope, 0, data, 0, prefix);
                writeOnOtherThread(scope, prefix, data, prefix, data.length - prefix);
                scope.complete(data.length);

                assertThat(kind.toString(), buffer.position(), equalTo(offset + data.length));
                final byte[] actual = new byte[data.length];
                buffer.get(offset, actual);
                assertArrayEquals(kind.toString(), data, actual);
                assertComplete(scope);
                onOtherThread(() -> assertComplete(scope));
            }
        }
    }

    // A read-only buffer cannot be filled whatever it is backed by, and the failure does not stop the scope from completing.
    public void testReadOnlyBuffersCannotBeFilled() {
        try (Arena confined = Arena.ofConfined(); Arena shared = Arena.ofShared()) {
            final ByteBuffer[] buffers = {
                ByteBuffer.allocate(8).asReadOnlyBuffer(),
                ByteBuffer.allocateDirect(8).asReadOnlyBuffer(),
                confined.allocate(8).asReadOnly().asByteBuffer(),
                shared.allocate(8).asReadOnly().asByteBuffer() };
            for (ByteBuffer buffer : buffers) {
                final BufferFillScope scope = BufferFillScope.of(buffer);
                expectThrows(ReadOnlyBufferException.class, () -> scope.fillSlice(0, 8, slice -> {
                    slice.put((byte) 1);
                    return 1;
                }));
                // a leaked permit would make this block forever
                scope.complete(0);
                assertComplete(scope);
            }
        }
    }

    // Asserts that the scope is complete, so that it no longer lets anything touch
    // the buffer: the filler is not run and an empty result is returned.
    private static void assertComplete(BufferFillScope scope) {
        final Optional<Integer> result = scope.fillSlice(0, 1, slice -> {
            throw new AssertionError("the filler must not run once the scope is complete");
        });
        assertFalse(result.isPresent());
    }

    private static void fillWith(BufferFillScope scope, int position, int length) {
        scope.fillSlice(position, length, slice -> {
            assertThat(slice.remaining(), equalTo(length));
            return length;
        });
    }

    private static void writeOnOtherThread(BufferFillScope scope, int position, byte[] data, int from, int length) {
        onOtherThread(() -> {
            final int written = scope.fillSlice(position, length, slice -> {
                slice.put(data, from, length);
                return length;
            }).orElseThrow();
            assertThat(written, equalTo(length));
        });
    }
}
