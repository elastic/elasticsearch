/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.searchablesnapshots.cache.common;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentSet;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsUtils.toIntBytes;
import static org.elasticsearch.xpack.searchablesnapshots.cache.common.TestUtils.mergeContiguousRanges;
import static org.elasticsearch.xpack.searchablesnapshots.cache.common.TestUtils.randomRanges;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;

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
        assertThat(invoked.get(), is(false));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> sparseFileTracker.waitForRange(
                ByteRange.of(randomLongBetween(0L, Math.max(0L, length - 1L)), length + 1L),
                null,
                listener
            )
        );
        assertThat("end must not be greater than length", e.getMessage(), containsString("invalid range"));
        assertThat(invoked.get(), is(false));

        if (length > 1L) {
            e = expectThrows(IllegalArgumentException.class, () -> {
                long start = randomLongBetween(1L, Math.max(1L, length - 1L));
                long end = randomLongBetween(length + 1, length + 1000L);
                sparseFileTracker.waitForRange(ByteRange.of(start, end), null, listener);
            });
            assertThat("end must not be greater than length", e.getMessage(), containsString("invalid range"));
            assertThat(invoked.get(), is(false));

            final long start = randomLongBetween(0L, length - 1L);
            final long end = randomLongBetween(start + 1L, length);

            if (start > 0L) {
                e = expectThrows(
                    IllegalArgumentException.class,
                    () -> sparseFileTracker.waitForRange(ByteRange.of(start, end), ByteRange.of(start - 1L, end), listener)
                );
                assertThat(
                    "listener range start must not be smaller than range start",
                    e.getMessage(),
                    containsString("unable to listen to range")
                );
                assertThat(invoked.get(), is(false));
            }

            if (end < length) {
                e = expectThrows(
                    IllegalArgumentException.class,
                    () -> sparseFileTracker.waitForRange(ByteRange.of(start, end), ByteRange.of(start, end + 1L), listener)
                );
                assertThat(
                    "listener range end must not be greater than range end",
                    e.getMessage(),
                    containsString("unable to listen to range")
                );
                assertThat(invoked.get(), is(false));
            } else {
                e = expectThrows(
                    IllegalArgumentException.class,
                    () -> sparseFileTracker.waitForRange(ByteRange.of(start, end), ByteRange.of(start, end + 1L), listener)
                );
                assertThat(
                    "listener range end must not be greater than length",
                    e.getMessage(),
                    containsString("invalid range to listen to")
                );
                assertThat(invoked.get(), is(false));
            }
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
            if (fileContents[toIntBytes(i)] == UNAVAILABLE) {
                pending = true;
                break;
            }
        }

        final ByteRange range = ByteRange.of(start, end);
        if (pending) {
            final AtomicBoolean expectNotification = new AtomicBoolean();
            final AtomicBoolean wasNotified = new AtomicBoolean();
            final List<SparseFileTracker.Gap> gaps = sparseFileTracker.waitForRange(range, range, ActionListener.wrap(ignored -> {
                assertTrue(expectNotification.get());
                assertTrue(wasNotified.compareAndSet(false, true));
            }, e -> { throw new AssertionError(e); }));
            for (int gapIndex = 0; gapIndex < gaps.size(); gapIndex++) {
                final SparseFileTracker.Gap gap = gaps.get(gapIndex);
                assertThat(gap.start(), greaterThanOrEqualTo(start));
                assertThat(gap.end(), lessThanOrEqualTo(end));
                // listener is notified when the last gap is completed
                final AtomicBoolean shouldNotifyListener = new AtomicBoolean();
                for (long i = gap.start(); i < gap.end(); i++) {
                    assertThat(fileContents[toIntBytes(i)], equalTo(UNAVAILABLE));
                    fileContents[toIntBytes(i)] = AVAILABLE;
                    // listener is notified when the progress reached the last byte of the last gap
                    if ((gapIndex == gaps.size() - 1) && (i == gap.end() - 1L)) {
                        assertTrue(shouldNotifyListener.compareAndSet(false, true));
                        expectNotification.set(true);
                    }
                    gap.onProgress(i + 1L);
                    assertThat(wasNotified.get(), equalTo(shouldNotifyListener.get()));
                }
                assertThat(wasNotified.get(), equalTo(shouldNotifyListener.get()));
                gap.onCompletion();
            }
            assertTrue(wasNotified.get());
        }

        final AtomicBoolean wasNotified = new AtomicBoolean();
        final List<SparseFileTracker.Gap> gaps = sparseFileTracker.waitForRange(
            range,
            range,
            ActionListener.wrap(ignored -> assertTrue(wasNotified.compareAndSet(false, true)), e -> { throw new AssertionError(e); })
        );
        assertThat(gaps, empty());
        assertTrue(wasNotified.get());
    }

    public void testCallsListenerWhenRangeIsAvailable() {
        final byte[] fileContents = new byte[between(0, 1000)];
        final SparseFileTracker sparseFileTracker = new SparseFileTracker("test", fileContents.length);

        final Set<AtomicBoolean> listenersCalled = new HashSet<>();
        for (int i = between(0, 10); i > 0; i--) {
            waitForRandomRange(fileContents, sparseFileTracker, listenersCalled::add, gap -> processGap(fileContents, gap));
            assertTrue(listenersCalled.stream().allMatch(AtomicBoolean::get));
        }

        final ByteRange range;
        {
            final long start = randomLongBetween(0L, Math.max(0L, fileContents.length - 1));
            range = ByteRange.of(start, randomLongBetween(start, fileContents.length));
        }

        final ByteRange subRange;
        {
            if (range.length() > 1L) {
                final long start = randomLongBetween(range.start(), range.end() - 1L);
                subRange = ByteRange.of(start, randomLongBetween(start + 1L, range.end()));
            } else {
                subRange = ByteRange.of(range.start(), range.end());
            }
        }

        boolean pending = false;
        for (long i = subRange.start(); i < subRange.end(); i++) {
            if (fileContents[toIntBytes(i)] == UNAVAILABLE) {
                pending = true;
            }
        }

        if (pending == false) {
            final AtomicBoolean wasNotified = new AtomicBoolean();
            final ActionListener<Void> listener = ActionListener.wrap(
                ignored -> assertTrue(wasNotified.compareAndSet(false, true)),
                e -> { throw new AssertionError(e); }
            );
            final List<SparseFileTracker.Gap> gaps = sparseFileTracker.waitForRange(range, subRange, listener);

            assertTrue(
                "All bytes of the sub range " + subRange + " are available, listener must be executed immediately",
                wasNotified.get()
            );

            wasNotified.set(false);
            assertTrue(sparseFileTracker.waitForRangeIfPending(subRange, listener));
            assertTrue(wasNotified.get());

            for (final SparseFileTracker.Gap gap : gaps) {
                assertThat(gap.start(), greaterThanOrEqualTo(range.start()));
                assertThat(gap.end(), lessThanOrEqualTo(range.end()));

                for (long i = gap.start(); i < gap.end(); i++) {
                    assertThat(fileContents[toIntBytes(i)], equalTo(UNAVAILABLE));
                    fileContents[toIntBytes(i)] = AVAILABLE;
                    gap.onProgress(i + 1L);
                }
                gap.onCompletion();
            }

        } else {
            final AtomicBoolean waitIfPendingWasNotified = new AtomicBoolean();
            final ActionListener<Void> waitIfPendingListener = ActionListener.wrap(
                ignored -> assertTrue(waitIfPendingWasNotified.compareAndSet(false, true)),
                e -> { throw new AssertionError(e); }
            );
            assertFalse(sparseFileTracker.waitForRangeIfPending(subRange, waitIfPendingListener));

            final AtomicBoolean wasNotified = new AtomicBoolean();
            final AtomicBoolean expectNotification = new AtomicBoolean();
            final List<SparseFileTracker.Gap> gaps = sparseFileTracker.waitForRange(range, subRange, ActionListener.wrap(ignored -> {
                assertTrue(expectNotification.get());
                assertTrue(wasNotified.compareAndSet(false, true));
            }, e -> { throw new AssertionError(e); }));

            assertFalse("Listener should not have been executed yet", wasNotified.get());

            assertTrue(sparseFileTracker.waitForRangeIfPending(subRange, waitIfPendingListener));
            assertFalse(waitIfPendingWasNotified.get());

            long triggeringProgress = -1L;
            for (long i = subRange.start(); i < subRange.end(); i++) {
                if (fileContents[toIntBytes(i)] == UNAVAILABLE) {
                    triggeringProgress = i;
                }
            }
            assertThat(triggeringProgress, greaterThanOrEqualTo(0L));

            for (final SparseFileTracker.Gap gap : gaps) {
                assertThat(gap.start(), greaterThanOrEqualTo(range.start()));
                assertThat(gap.end(), lessThanOrEqualTo(range.end()));

                for (long i = gap.start(); i < gap.end(); i++) {
                    assertThat(fileContents[toIntBytes(i)], equalTo(UNAVAILABLE));
                    fileContents[toIntBytes(i)] = AVAILABLE;
                    if (triggeringProgress == i) {
                        assertFalse(expectNotification.getAndSet(true));
                    }
                    assertThat(
                        "Listener should not have been called before ["
                            + triggeringProgress
                            + "] is reached, but it was triggered after progress got updated to ["
                            + i
                            + ']',
                        wasNotified.get() && waitIfPendingWasNotified.get(),
                        equalTo(triggeringProgress < i)
                    );

                    gap.onProgress(i + 1L);

                    assertThat(
                        "Listener should not have been called before ["
                            + triggeringProgress
                            + "] is reached, but it was triggered after progress got updated to ["
                            + i
                            + ']',
                        wasNotified.get() && waitIfPendingWasNotified.get(),
                        equalTo(triggeringProgress < i + 1L)
                    );
                }
                gap.onCompletion();

                assertThat(
                    "Listener should not have been called before ["
                        + triggeringProgress
                        + "] is reached, but it was triggered once gap ["
                        + gap
                        + "] was completed",
                    wasNotified.get(),
                    equalTo(triggeringProgress < gap.end())
                );
                assertThat(waitIfPendingWasNotified.get(), equalTo(triggeringProgress < gap.end()));
            }
            assertTrue(wasNotified.get());
            assertTrue(waitIfPendingWasNotified.get());
        }

        final AtomicBoolean wasNotified = new AtomicBoolean();
        final List<SparseFileTracker.Gap> gaps = sparseFileTracker.waitForRange(
            range,
            subRange,
            ActionListener.wrap(ignored -> assertTrue(wasNotified.compareAndSet(false, true)), e -> { throw new AssertionError(e); })
        );
        assertThat(gaps, empty());
        assertTrue(wasNotified.get());
    }

    public void testDeterministicSafety() {
        final byte[] fileContents = new byte[between(0, 1000)];
        final SparseFileTracker sparseFileTracker = new SparseFileTracker("test", fileContents.length);
        final Set<AtomicBoolean> listenersCalled = new HashSet<>();

        final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue();

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

    public void testSparseFileTrackerCreatedWithCompletedRanges() {
        final long fileLength = between(0, 1000);
        final SortedSet<ByteRange> completedRanges = randomRanges(fileLength);

        final SparseFileTracker sparseFileTracker = new SparseFileTracker("test", fileLength, completedRanges);
        assertThat(sparseFileTracker.getCompletedRanges(), equalTo(completedRanges));

        for (ByteRange completedRange : completedRanges) {
            assertThat(sparseFileTracker.getAbsentRangeWithin(completedRange), nullValue());

            final AtomicBoolean listenerCalled = new AtomicBoolean();
            assertThat(sparseFileTracker.waitForRange(completedRange, completedRange, new ActionListener<>() {
                @Override
                public void onResponse(Void aVoid) {
                    listenerCalled.set(true);
                }

                @Override
                public void onFailure(Exception e) {
                    throw new AssertionError(e);
                }
            }), hasSize(0));
            assertThat(listenerCalled.get(), is(true));
        }
    }

    public void testGetCompletedRanges() {
        final byte[] fileContents = new byte[between(0, 1000)];
        final SparseFileTracker sparseFileTracker = new SparseFileTracker("test", fileContents.length);

        final Set<AtomicBoolean> listenersCalled = new HashSet<>();
        final SortedSet<ByteRange> gapsProcessed = Collections.synchronizedNavigableSet(new TreeSet<>());
        for (int i = between(0, 10); i > 0; i--) {
            waitForRandomRange(fileContents, sparseFileTracker, listenersCalled::add, gap -> {
                if (processGap(fileContents, gap)) {
                    gapsProcessed.add(ByteRange.of(gap.start(), gap.end()));
                }
            });
            assertTrue(listenersCalled.stream().allMatch(AtomicBoolean::get));
        }

        // merge adjacent processed ranges as the SparseFileTracker does internally when a gap is completed
        // in order to check that SparseFileTracker.getCompletedRanges() returns the expected values
        final SortedSet<ByteRange> expectedCompletedRanges = mergeContiguousRanges(gapsProcessed);

        final SortedSet<ByteRange> completedRanges = sparseFileTracker.getCompletedRanges();
        assertThat(completedRanges, hasSize(expectedCompletedRanges.size()));
        assertThat(completedRanges, equalTo(expectedCompletedRanges));
    }

    private static void checkRandomAbsentRange(byte[] fileContents, SparseFileTracker sparseFileTracker, boolean expectExact) {
        final long checkStart = randomLongBetween(0, fileContents.length - 1);
        final long checkEnd = randomLongBetween(checkStart, fileContents.length);

        final ByteRange freeRange = sparseFileTracker.getAbsentRangeWithin(ByteRange.of(checkStart, checkEnd));
        if (freeRange == null) {
            for (long i = checkStart; i < checkEnd; i++) {
                assertThat(fileContents[toIntBytes(i)], equalTo(AVAILABLE));
            }
        } else {
            assertThat(freeRange.start(), greaterThanOrEqualTo(checkStart));
            assertTrue(freeRange.toString(), freeRange.start() < freeRange.end());
            assertThat(freeRange.end(), lessThanOrEqualTo(checkEnd));
            for (long i = checkStart; i < freeRange.start(); i++) {
                assertThat(fileContents[toIntBytes(i)], equalTo(AVAILABLE));
            }
            for (long i = freeRange.end(); i < checkEnd; i++) {
                assertThat(fileContents[toIntBytes(i)], equalTo(AVAILABLE));
            }
            if (expectExact) {
                // without concurrent activity, the returned range is as small as possible
                assertThat(fileContents[toIntBytes(freeRange.start())], equalTo(UNAVAILABLE));
                assertThat(fileContents[toIntBytes(freeRange.end() - 1)], equalTo(UNAVAILABLE));
            }
        }
    }

    private static void waitForRandomRange(
        byte[] fileContents,
        SparseFileTracker sparseFileTracker,
        Consumer<AtomicBoolean> listenerCalledConsumer,
        Consumer<SparseFileTracker.Gap> gapConsumer
    ) {
        final long rangeStart = randomLongBetween(0L, Math.max(0L, fileContents.length - 1));
        final long rangeEnd = randomLongBetween(rangeStart, fileContents.length);
        final AtomicBoolean listenerCalled = new AtomicBoolean();
        listenerCalledConsumer.accept(listenerCalled);

        final boolean fillInGaps = randomBoolean();
        final boolean useSubRange = fillInGaps && randomBoolean();
        final long subRangeStart = useSubRange ? randomLongBetween(rangeStart, rangeEnd) : rangeStart;
        final long subRangeEnd = useSubRange ? randomLongBetween(subRangeStart, rangeEnd) : rangeEnd;

        final ActionListener<Void> actionListener = new ActionListener<>() {
            @Override
            public void onResponse(Void aVoid) {
                for (long i = subRangeStart; i < subRangeEnd; i++) {
                    assertThat(fileContents[toIntBytes(i)], equalTo(AVAILABLE));
                }
                assertTrue(listenerCalled.compareAndSet(false, true));
            }

            @Override
            public void onFailure(Exception e) {
                assertTrue(listenerCalled.compareAndSet(false, true));
            }
        };

        if (randomBoolean()) {
            final List<SparseFileTracker.Gap> gaps = sparseFileTracker.waitForRange(
                ByteRange.of(rangeStart, rangeEnd),
                ByteRange.of(subRangeStart, subRangeEnd),
                actionListener
            );

            for (final SparseFileTracker.Gap gap : gaps) {
                for (long i = gap.start(); i < gap.end(); i++) {
                    assertThat(Long.toString(i), fileContents[toIntBytes(i)], equalTo(UNAVAILABLE));
                }
                gapConsumer.accept(gap);
            }
        } else {
            final boolean listenerRegistered = sparseFileTracker.waitForRangeIfPending(ByteRange.of(rangeStart, rangeEnd), actionListener);
            if (listenerRegistered == false) {
                assertTrue(listenerCalled.compareAndSet(false, true));
            }
        }
    }

    private static boolean processGap(byte[] fileContents, SparseFileTracker.Gap gap) {
        for (long i = gap.start(); i < gap.end(); i++) {
            assertThat(fileContents[toIntBytes(i)], equalTo(UNAVAILABLE));
        }

        if (randomBoolean()) {
            gap.onFailure(new ElasticsearchException("simulated"));
            return false;
        } else {
            for (long i = gap.start(); i < gap.end(); i++) {
                fileContents[toIntBytes(i)] = AVAILABLE;
                gap.onProgress(i + 1L);
            }
            gap.onCompletion();
            return true;
        }
    }
}
