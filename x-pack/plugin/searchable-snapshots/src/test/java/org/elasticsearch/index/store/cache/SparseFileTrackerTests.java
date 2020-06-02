/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.index.store.cache;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.coordination.DeterministicTaskQueue;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentSet;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class SparseFileTrackerTests extends ESTestCase {

    // these tests model the file as a byte[] which starts out entirely unavailable and becomes available over time on a byte-by-byte basis
    private static final byte UNAVAILABLE = (byte) 0x00;
    private static final byte AVAILABLE = (byte) 0xff;

    public void testInvalidLength() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new SparseFileTracker("test", -1L));
        assertThat(e.getMessage(), containsString("Length [-1] must be equal to or greater than 0 for [test]"));
    }

    public void testInvalidRange() {
        final byte[] fileContents = new byte[between(0, 1000)];
        final long length = fileContents.length;
        final SparseFileTracker sparseFileTracker = new SparseFileTracker("test", length);

        final AtomicBoolean invoked = new AtomicBoolean(false);
        final ActionListener<Void> listener = ActionListener.wrap(() -> invoked.set(true));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> sparseFileTracker.waitForRange(-1L, randomLongBetween(0L, length), listener)
        );
        assertThat("start must not be negative", e.getMessage(), containsString("invalid range"));
        assertThat(invoked.get(), is(false));

        e = expectThrows(
            IllegalArgumentException.class,
            () -> sparseFileTracker.waitForRange(randomLongBetween(0L, Math.max(0L, length - 1L)), length + 1L, listener)
        );
        assertThat("end must not be greater than length", e.getMessage(), containsString("invalid range"));
        assertThat(invoked.get(), is(false));

        if (length > 1L) {
            e = expectThrows(IllegalArgumentException.class, () -> {
                long start = randomLongBetween(1L, Math.max(1L, length - 1L));
                long end = randomLongBetween(0L, start - 1L);
                sparseFileTracker.waitForRange(start, end, listener);
            });
            assertThat("end must not be greater than length", e.getMessage(), containsString("invalid range"));
            assertThat(invoked.get(), is(false));
        }
    }

    public void testCallsListenerWhenWholeRangeIsAvailable() {
        final byte[] fileContents = new byte[between(0, 1000)];
        final SparseFileTracker sparseFileTracker = new SparseFileTracker("test", fileContents.length);

        final Set<AtomicBoolean> listenersCalled = new HashSet<>();
        for (int i = between(0, 10); i > 0; i--) {
            waitForRandomRange(fileContents, sparseFileTracker, listenersCalled::add, gap -> processGap(fileContents, gap));
            assertTrue(listenersCalled.stream().allMatch(AtomicBoolean::get));
        }

        final long start = randomLongBetween(0L, Math.max(0L, fileContents.length - 1));
        final long end = randomLongBetween(start, fileContents.length);
        boolean pending = false;
        for (long i = start; i < end; i++) {
            if (fileContents[Math.toIntExact(i)] == UNAVAILABLE) {
                pending = true;
                break;
            }
        }

        if (pending) {
            final AtomicBoolean expectNotification = new AtomicBoolean();
            final AtomicBoolean wasNotified = new AtomicBoolean();
            final List<SparseFileTracker.Gap> gaps = sparseFileTracker.waitForRange(start, end, ActionListener.wrap(ignored -> {
                assertTrue(expectNotification.get());
                assertTrue(wasNotified.compareAndSet(false, true));
            }, e -> { throw new AssertionError(e); }));
            for (int gapIndex = 0; gapIndex < gaps.size(); gapIndex++) {
                final SparseFileTracker.Gap gap = gaps.get(gapIndex);
                assertThat(gap.start, greaterThanOrEqualTo(start));
                assertThat(gap.end, lessThanOrEqualTo(end));
                for (long i = gap.start; i < gap.end; i++) {
                    assertThat(fileContents[Math.toIntExact(i)], equalTo(UNAVAILABLE));
                    fileContents[Math.toIntExact(i)] = AVAILABLE;
                }
                assertFalse(wasNotified.get());
                if (gapIndex == gaps.size() - 1) {
                    expectNotification.set(true);
                }
                gap.onResponse(null);
            }
            assertTrue(wasNotified.get());
        }

        final AtomicBoolean wasNotified = new AtomicBoolean();
        final List<SparseFileTracker.Gap> gaps = sparseFileTracker.waitForRange(
            start,
            end,
            ActionListener.wrap(ignored -> assertTrue(wasNotified.compareAndSet(false, true)), e -> { throw new AssertionError(e); })
        );
        assertThat(gaps, empty());
        assertTrue(wasNotified.get());
    }

    public void testDeterministicSafety() {
        final byte[] fileContents = new byte[between(0, 1000)];
        final SparseFileTracker sparseFileTracker = new SparseFileTracker("test", fileContents.length);
        final Set<AtomicBoolean> listenersCalled = new HashSet<>();

        final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue(Settings.EMPTY, random());

        deterministicTaskQueue.setExecutionDelayVariabilityMillis(1000);

        for (int i = between(1, 1000); i > 0; i--) {
            if (rarely() && fileContents.length > 0) {
                deterministicTaskQueue.scheduleNow(() -> checkRandomAbsentRange(fileContents, sparseFileTracker, true));
            } else {
                deterministicTaskQueue.scheduleNow(
                    () -> waitForRandomRange(
                        fileContents,
                        sparseFileTracker,
                        listenersCalled::add,
                        gap -> deterministicTaskQueue.scheduleNow(() -> processGap(fileContents, gap))
                    )
                );
            }
        }

        deterministicTaskQueue.runAllTasks();
        assertTrue(listenersCalled.stream().allMatch(AtomicBoolean::get));
    }

    public void testThreadSafety() throws InterruptedException {
        final byte[] fileContents = new byte[between(0, 1000)];
        final Thread[] threads = new Thread[between(1, 5)];
        final SparseFileTracker sparseFileTracker = new SparseFileTracker("test", fileContents.length);

        final CountDownLatch startLatch = new CountDownLatch(1);
        final Semaphore countDown = new Semaphore(between(1, 1000));
        final Set<AtomicBoolean> listenersCalled = newConcurrentSet();
        for (int threadIndex = 0; threadIndex < threads.length; threadIndex++) {
            threads[threadIndex] = new Thread(() -> {
                try {
                    startLatch.await();
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }

                while (countDown.tryAcquire()) {
                    waitForRandomRange(fileContents, sparseFileTracker, listenersCalled::add, gap -> processGap(fileContents, gap));
                }
            });
        }

        for (Thread thread : threads) {
            thread.start();
        }

        final Thread checkThread = new Thread(() -> {
            while (countDown.availablePermits() > 0 && fileContents.length > 0) {
                checkRandomAbsentRange(fileContents, sparseFileTracker, false);
            }
        });
        checkThread.start();

        startLatch.countDown();

        for (Thread thread : threads) {
            thread.join();
        }

        assertThat(countDown.availablePermits(), equalTo(0));
        assertTrue(listenersCalled.stream().allMatch(AtomicBoolean::get));

        checkThread.join();
    }

    private static void checkRandomAbsentRange(byte[] fileContents, SparseFileTracker sparseFileTracker, boolean expectExact) {
        final long checkStart = randomLongBetween(0, fileContents.length - 1);
        final long checkEnd = randomLongBetween(0, fileContents.length);

        final Tuple<Long, Long> freeRange = sparseFileTracker.getAbsentRangeWithin(checkStart, checkEnd);
        if (freeRange == null) {
            for (long i = checkStart; i < checkEnd; i++) {
                assertThat(fileContents[Math.toIntExact(i)], equalTo(AVAILABLE));
            }
        } else {
            assertThat(freeRange.v1(), greaterThanOrEqualTo(checkStart));
            assertTrue(freeRange.toString(), freeRange.v1() < freeRange.v2());
            assertThat(freeRange.v2(), lessThanOrEqualTo(checkEnd));
            for (long i = checkStart; i < freeRange.v1(); i++) {
                assertThat(fileContents[Math.toIntExact(i)], equalTo(AVAILABLE));
            }
            for (long i = freeRange.v2(); i < checkEnd; i++) {
                assertThat(fileContents[Math.toIntExact(i)], equalTo(AVAILABLE));
            }
            if (expectExact) {
                // without concurrent activity, the returned range is as small as possible
                assertThat(fileContents[Math.toIntExact(freeRange.v1())], equalTo(UNAVAILABLE));
                assertThat(fileContents[Math.toIntExact(freeRange.v2() - 1)], equalTo(UNAVAILABLE));
            }
        }
    }

    private static void waitForRandomRange(
        byte[] fileContents,
        SparseFileTracker sparseFileTracker,
        Consumer<AtomicBoolean> listenerCalledConsumer,
        Consumer<SparseFileTracker.Gap> gapConsumer
    ) {
        final long start = randomLongBetween(0L, Math.max(0L, fileContents.length - 1));
        final long end = randomLongBetween(start, fileContents.length);
        final AtomicBoolean listenerCalled = new AtomicBoolean();
        listenerCalledConsumer.accept(listenerCalled);

        final List<SparseFileTracker.Gap> gaps = sparseFileTracker.waitForRange(start, end, new ActionListener<>() {
            @Override
            public void onResponse(Void aVoid) {
                for (long i = start; i < end; i++) {
                    assertThat(fileContents[Math.toIntExact(i)], equalTo(AVAILABLE));
                }
                assertTrue(listenerCalled.compareAndSet(false, true));
            }

            @Override
            public void onFailure(Exception e) {
                assertTrue(listenerCalled.compareAndSet(false, true));
            }
        });

        for (final SparseFileTracker.Gap gap : gaps) {
            for (long i = gap.start; i < gap.end; i++) {
                assertThat(Long.toString(i), fileContents[Math.toIntExact(i)], equalTo(UNAVAILABLE));
            }
            gapConsumer.accept(gap);
        }
    }

    private static void processGap(byte[] fileContents, SparseFileTracker.Gap gap) {
        for (long i = gap.start; i < gap.end; i++) {
            assertThat(fileContents[Math.toIntExact(i)], equalTo(UNAVAILABLE));
        }

        if (randomBoolean()) {
            gap.onFailure(new ElasticsearchException("simulated"));
        } else {
            for (long i = gap.start; i < gap.end; i++) {
                fileContents[Math.toIntExact(i)] = AVAILABLE;
            }
            gap.onResponse(null);
        }
    }
}
