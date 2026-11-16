/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.lessThan;

/**
 * Self-tests for {@link DrainSimulatingStorageObject}: per-stream drain-vs-abort and the optional
 * drain latch that abort-on-close must not wait on.
 */
public class DrainSimulatingStorageObjectTests extends ESTestCase {

    public void testAbortFirstStreamDoesNotSkipSecondStreamDrainOrAbort() throws IOException {
        byte[] bytes = filled(DecompressingStorageObject.MAX_TRAILING_DRAIN_BYTES * 4);
        DrainSimulatingStorageObject.Tracking tracking = new DrainSimulatingStorageObject.Tracking();
        StorageObject object = DrainSimulatingStorageObject.create(bytes, tracking);

        DrainSimulatingStorageObject.DrainTrackingInputStream first = (DrainSimulatingStorageObject.DrainTrackingInputStream) object
            .newStream();
        assertEquals(1, first.read());
        object.abortStream(first);
        assertTrue(first.isAborted());
        assertTrue(tracking.aborted.get());
        assertEquals(1, tracking.abortCalls.get());
        long afterFirst = tracking.bytesConsumed.get();
        assertThat(afterFirst, lessThan((long) bytes.length / 2));

        DrainSimulatingStorageObject.DrainTrackingInputStream second = (DrainSimulatingStorageObject.DrainTrackingInputStream) object
            .newStream();
        assertEquals(1, second.read());
        second.close();
        assertTrue("second large leftover must abort independently of the first stream", second.isAborted());
        assertTrue("object-level aborted is a sticky OR", tracking.aborted.get());
        assertEquals("close-abort of the second stream is not abortStream", 1, tracking.abortCalls.get());
        assertEquals("second remainder must not drain", afterFirst + 1, tracking.bytesConsumed.get());
    }

    public void testAbortFirstStreamDoesNotSuppressDrainOfSecondSmallTail() throws IOException {
        byte[] bytes = filled(1024);
        DrainSimulatingStorageObject.Tracking tracking = new DrainSimulatingStorageObject.Tracking();
        StorageObject object = DrainSimulatingStorageObject.create(bytes, tracking);

        DrainSimulatingStorageObject.DrainTrackingInputStream first = (DrainSimulatingStorageObject.DrainTrackingInputStream) object
            .newStream();
        object.abortStream(first);
        assertTrue(first.isAborted());
        assertEquals(1, tracking.abortCalls.get());

        DrainSimulatingStorageObject.DrainTrackingInputStream second = (DrainSimulatingStorageObject.DrainTrackingInputStream) object
            .newStream();
        assertEquals(1, second.read());
        second.close();
        assertFalse("second small tail drains; per-stream abort is independent of the sticky object flag", second.isAborted());
        assertTrue("object-level aborted stays set after the first abort", tracking.aborted.get());
        assertEquals(
            "small leftover on the second stream must still drain after the first abort",
            bytes.length,
            tracking.bytesConsumed.get()
        );
    }

    public void testCloseAbortsWhenLeftoverExceedsTrailingDrainBytes() throws IOException {
        byte[] bytes = filled(DecompressingStorageObject.MAX_TRAILING_DRAIN_BYTES + 2);
        DrainSimulatingStorageObject.Tracking tracking = new DrainSimulatingStorageObject.Tracking();
        StorageObject object = DrainSimulatingStorageObject.create(bytes, tracking);

        try (InputStream in = object.newStream()) {
            assertEquals(1, in.read());
        }
        assertTrue(tracking.aborted.get());
        assertEquals(0, tracking.abortCalls.get());
        assertEquals(1, tracking.bytesConsumed.get());
    }

    public void testCloseDrainsWhenLeftoverAtMostTrailingDrainBytes() throws IOException {
        byte[] bytes = filled(DecompressingStorageObject.MAX_TRAILING_DRAIN_BYTES);
        DrainSimulatingStorageObject.Tracking tracking = new DrainSimulatingStorageObject.Tracking();
        StorageObject object = DrainSimulatingStorageObject.create(bytes, tracking);

        try (InputStream in = object.newStream()) {
            assertEquals(1, in.read());
        }
        assertFalse(tracking.aborted.get());
        assertEquals(bytes.length, tracking.bytesConsumed.get());
    }

    public void testDrainLatchBlocksDrainButNotAbortOnClose() throws Exception {
        byte[] small = filled(1024);
        DrainSimulatingStorageObject.Tracking drainTracking = new DrainSimulatingStorageObject.Tracking();
        CountDownLatch drainLatch = new CountDownLatch(1);
        drainTracking.drainLatch = drainLatch;
        StorageObject drainObject = DrainSimulatingStorageObject.create(small, drainTracking);
        InputStream drainStream = drainObject.newStream();
        assertEquals(1, drainStream.read());
        Thread drainer = new Thread(() -> {
            try {
                drainStream.close();
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        });
        drainer.start();
        try {
            drainer.join(TimeUnit.MILLISECONDS.toMillis(250));
            assertTrue("drain must wait on the latch", drainer.isAlive());
            assertEquals("latch is before the first drain read", 1, drainTracking.bytesConsumed.get());
        } finally {
            drainLatch.countDown();
            drainer.join(TimeUnit.SECONDS.toMillis(5));
        }
        assertFalse(drainer.isAlive());
        assertEquals(small.length, drainTracking.bytesConsumed.get());

        byte[] large = filled(DecompressingStorageObject.MAX_TRAILING_DRAIN_BYTES * 2);
        DrainSimulatingStorageObject.Tracking abortTracking = new DrainSimulatingStorageObject.Tracking();
        CountDownLatch abortLatch = new CountDownLatch(1);
        abortTracking.drainLatch = abortLatch;
        StorageObject abortObject = DrainSimulatingStorageObject.create(large, abortTracking);
        InputStream abortStream = abortObject.newStream();
        assertEquals(1, abortStream.read());
        AtomicBoolean closed = new AtomicBoolean();
        Thread aborter = new Thread(() -> {
            try {
                abortStream.close();
                closed.set(true);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        });
        try {
            aborter.start();
            aborter.join(TimeUnit.SECONDS.toMillis(5));
            assertFalse("abort-on-close must not wait on the drain latch", aborter.isAlive());
            assertTrue(closed.get());
            assertTrue(abortTracking.aborted.get());
            assertEquals(1, abortTracking.bytesConsumed.get());
        } finally {
            abortLatch.countDown();
            aborter.join(TimeUnit.SECONDS.toMillis(5));
        }
    }

    private static byte[] filled(int length) {
        byte[] bytes = new byte[length];
        Arrays.fill(bytes, (byte) 1);
        return bytes;
    }
}
