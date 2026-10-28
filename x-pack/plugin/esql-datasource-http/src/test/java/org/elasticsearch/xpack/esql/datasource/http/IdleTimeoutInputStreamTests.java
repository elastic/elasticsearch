/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.http;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.sameInstance;

public class IdleTimeoutInputStreamTests extends ESTestCase {

    private static final StoragePath PATH = StoragePath.of("https://example.com/file.csv");

    public void testZeroTimeoutIsNoOpWrap() {
        InputStream body = new ByteArrayInputStream(new byte[] { 1, 2, 3 });
        assertThat(IdleTimeoutInputStream.wrap(body, Duration.ZERO, PATH, null), sameInstance(body));
    }

    public void testNullSchedulerIsNoOpWrap() {
        InputStream body = new ByteArrayInputStream(new byte[] { 1, 2, 3 });
        assertThat(IdleTimeoutInputStream.wrap(body, Duration.ofSeconds(30), PATH, null), sameInstance(body));
    }

    public void testFastReadCompletes() throws Exception {
        byte[] payload = randomByteArrayOfLength(between(1, 4096));
        ScheduledExecutorService scheduler = newScheduler();
        try {
            try (InputStream in = IdleTimeoutInputStream.wrap(new ByteArrayInputStream(payload), Duration.ofSeconds(5), PATH, scheduler)) {
                assertArrayEquals(payload, in.readAllBytes());
            }
        } finally {
            scheduler.shutdownNow();
            assertTrue(scheduler.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    /**
     * A peer that never sends a body byte must unblock the caller with {@link SocketTimeoutException}
     * rather than hang. Closing the delegate is how the timer aborts the blocked {@code read}.
     */
    public void testIdleReadTimesOut() throws Exception {
        BlockingInputStream blocking = new BlockingInputStream();
        ScheduledExecutorService scheduler = newScheduler();
        try {
            try (InputStream in = IdleTimeoutInputStream.wrap(blocking, Duration.ofMillis(50), PATH, scheduler)) {
                SocketTimeoutException thrown = expectThrows(SocketTimeoutException.class, in::read);
                assertThat(thrown.getMessage(), containsString("Idle timeout"));
                assertThat(thrown.getMessage(), containsString(PATH.toString()));
            }
            assertTrue("the timer must close the delegate so the blocked read unblocks", blocking.closed.await(5, TimeUnit.SECONDS));
        } finally {
            scheduler.shutdownNow();
            assertTrue(scheduler.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    /**
     * A stuck {@code skip} (200-fallback prefix discard) must time out the same way as a stuck
     * {@code read}. The watchdog still closes the delegate.
     */
    public void testIdleSkipTimesOut() throws Exception {
        BlockingInputStream blocking = new BlockingInputStream();
        ScheduledExecutorService scheduler = newScheduler();
        try {
            try (InputStream in = IdleTimeoutInputStream.wrap(blocking, Duration.ofMillis(50), PATH, scheduler)) {
                SocketTimeoutException thrown = expectThrows(SocketTimeoutException.class, () -> in.skip(1024));
                assertThat(thrown.getMessage(), containsString("Idle timeout"));
            }
            assertTrue(blocking.closed.await(5, TimeUnit.SECONDS));
        } finally {
            scheduler.shutdownNow();
            assertTrue(scheduler.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    /**
     * {@code skip(position)} on the 200-fallback path is a long discard. The idle timer must reset
     * per chunk so a live slow body is not aborted just because the whole prefix takes longer than
     * {@code idleTimeout}.
     */
    public void testSkipResetsIdlePerChunk() throws Exception {
        int total = IdleTimeoutInputStream.SKIP_CHUNK * 8;
        // 8 × 80ms = 640ms unchunked, above the 250ms idle timeout; each chunk stays inside it.
        ThrottledSkipStream body = new ThrottledSkipStream(total, IdleTimeoutInputStream.SKIP_CHUNK, 80);
        ScheduledExecutorService scheduler = newScheduler();
        try {
            try (InputStream in = IdleTimeoutInputStream.wrap(body, Duration.ofMillis(250), PATH, scheduler)) {
                assertEquals(total, in.skip(total));
            }
            assertEquals("idle skip must bound each delegate skip, not the whole prefix", 8, body.skipCalls.get());
        } finally {
            scheduler.shutdownNow();
            assertTrue(scheduler.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    /**
     * JDK {@code InputStream.skip} may return 0 without EOF. The wrapper must fall back to
     * {@code read} in the same chunk size so a 200-fallback prefix still makes per-buffer progress.
     */
    public void testSkipFallsBackToReadWhenSkipReturnsZero() throws Exception {
        int total = IdleTimeoutInputStream.SKIP_CHUNK * 2;
        SkipAlwaysZeroStream body = new SkipAlwaysZeroStream(total);
        ScheduledExecutorService scheduler = newScheduler();
        try {
            try (InputStream in = IdleTimeoutInputStream.wrap(body, Duration.ofSeconds(5), PATH, scheduler)) {
                assertEquals(total, in.skip(total));
            }
            assertTrue("skip(0) must fall back to read", body.reads.get() >= 2);
        } finally {
            scheduler.shutdownNow();
            assertTrue(scheduler.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    public void testNegativeTimeoutRejected() {
        InputStream body = new ByteArrayInputStream(new byte[] { 1 });
        expectThrows(IllegalArgumentException.class, () -> IdleTimeoutInputStream.wrap(body, Duration.ofMillis(-1), PATH, null));
    }

    private static ScheduledExecutorService newScheduler() {
        return Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "idle-timeout-test");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Blocks {@code read} until {@link #close()} — models a live HTTP connection that delivers no body
     * bytes. {@code close} counts down so the blocked reader unblocks; subsequent reads throw.
     */
    private static final class BlockingInputStream extends InputStream {
        private final CountDownLatch closed = new CountDownLatch(1);
        private final AtomicInteger reads = new AtomicInteger();

        @Override
        public int read() throws IOException {
            reads.incrementAndGet();
            blockUntilClosed();
            throw new IOException("closed");
        }

        @Override
        public long skip(long n) throws IOException {
            blockUntilClosed();
            throw new IOException("closed");
        }

        private void blockUntilClosed() throws IOException {
            try {
                if (closed.await(30, TimeUnit.SECONDS) == false) {
                    throw new IOException("test stream was not closed");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
        }

        @Override
        public void close() {
            closed.countDown();
        }
    }

    /**
     * {@code skip(n)} discards all {@code n} bytes in one call and sleeps in proportion — models a
     * body whose skip implementation loops internally. Chunked wrapping must finish each slice
     * inside the idle timeout even though the whole prefix would not.
     */
    private static final class ThrottledSkipStream extends InputStream {
        private int remaining;
        private final int bytesPerSleep;
        private final long sleepMs;
        private final AtomicInteger skipCalls = new AtomicInteger();

        ThrottledSkipStream(int size, int bytesPerSleep, long sleepMs) {
            this.remaining = size;
            this.bytesPerSleep = bytesPerSleep;
            this.sleepMs = sleepMs;
        }

        @Override
        public int read() throws IOException {
            if (remaining <= 0) {
                return -1;
            }
            sleepFor(1);
            remaining--;
            return 1;
        }

        @Override
        public long skip(long n) throws IOException {
            skipCalls.incrementAndGet();
            int can = (int) Math.min(n, remaining);
            if (can <= 0) {
                return 0;
            }
            sleepFor(can);
            remaining -= can;
            return can;
        }

        private void sleepFor(int bytes) throws IOException {
            long sleeps = (bytes + bytesPerSleep - 1) / (long) bytesPerSleep;
            try {
                Thread.sleep(sleeps * sleepMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
        }
    }

    /**
     * {@code skip} always returns 0; discard happens only through {@code read}.
     */
    private static final class SkipAlwaysZeroStream extends InputStream {
        private int remaining;
        private final AtomicInteger reads = new AtomicInteger();

        SkipAlwaysZeroStream(int size) {
            this.remaining = size;
        }

        @Override
        public int read() {
            if (remaining <= 0) {
                return -1;
            }
            remaining--;
            reads.incrementAndGet();
            return 1;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if (remaining <= 0) {
                return -1;
            }
            int n = Math.min(len, remaining);
            remaining -= n;
            reads.incrementAndGet();
            return n;
        }

        @Override
        public long skip(long n) {
            return 0;
        }
    }
}
