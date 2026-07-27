/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache.common;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.blobcache.BlobCacheUtils;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.LongStream;

import static org.elasticsearch.blobcache.BlobCacheTestUtils.mergeContiguousRanges;
import static org.elasticsearch.blobcache.BlobCacheTestUtils.randomRanges;
import static org.elasticsearch.blobcache.BlobCacheUtils.toIntBytes;
import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentSet;
import static org.hamcrest.Matchers.containsString;
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
        final ActionListener<Void> listener = ActionListener.running(() -> invoked.set(true));
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

    public void testWaitForEmptySubRangeReturnsImmediately() {
        final byte[] fileContents = new byte[between(0, 1000)];
        final long length = fileContents.length;
        final SparseFileTracker sparseFileTracker = new SparseFileTracker("test", length);

        if (randomBoolean()) {
            final long rangeStart = randomLongBetween(0, length - 1);
            final long rangeEnd = randomLongBetween(rangeStart + 1, length);
            final var range = ByteRange.of(rangeStart, rangeEnd);
            final PlainActionFuture<Void> listener = new PlainActionFuture<>();
            final var gapsOpt = sparseFileTracker.waitForRange(range, range, listener);
            assertThat(gapsOpt.isPresent(), is(true));
            var gapsList = gapsOpt.get().claim();
            assertThat(gapsList, hasSize(1));
            fillGap(fileContents, gapsList.getFirst());
            safeGet(listener);
        }

        final long subRangeStartAndEnd = randomLongBetween(0, length);
        final ByteRange subRange = ByteRange.of(subRangeStartAndEnd, subRangeStartAndEnd);
        final var range = ByteRange.of(randomLongBetween(0, subRangeStartAndEnd), randomLongBetween(subRangeStartAndEnd, length));
        final var listener = new PlainActionFuture<Void>();
        final var gapsOpt = sparseFileTracker.waitForRange(range, subRange, listener);
        assertThat(gapsOpt.isEmpty(), is(true));
        safeGet(listener);
    }

    public void testListenerCompletedImmediatelyWhenSubRangeIsAvailable() {
        final byte[] bytes = new byte[randomIntBetween(8, 1024)];
        final var tracker = new SparseFileTracker(getTestName(), bytes.length);

        // wraps a future to assert that the sub range bytes are available
        BiFunction<ByteRange, PlainActionFuture<Void>, ActionListener<Void>> wrapper = (range, future) -> ActionListener.runBefore(
            future,
            () -> LongStream.range(range.start(), range.end())
                .forEach(pos -> assertThat(bytes[BlobCacheUtils.toIntBytes(pos)], equalTo(AVAILABLE)))
        );

        var completeUpTo = randomIntBetween(2, bytes.length);
        {
            long subRangeStart = randomLongBetween(0, completeUpTo - 2);
            long subRangeEnd = randomLongBetween(subRangeStart + 1, completeUpTo - 1);
            var subRange = ByteRange.of(subRangeStart, subRangeEnd);
            var range = ByteRange.of(0, completeUpTo);
            var future = new PlainActionFuture<Void>();

            var gaps = tracker.waitForRange(range, subRange, wrapper.apply(subRange, future));
            assertThat(future.isDone(), equalTo(false));
            assertThat(gaps.isPresent(), is(true));
            var gapsList = gaps.get().claim();
            assertThat(gapsList, hasSize(1));

            fillGap(bytes, gapsList.get(0));

            assertThat(future.isDone(), equalTo(true));
        }
        {
            long subRangeStart = randomLongBetween(0L, Math.max(0L, completeUpTo - 1));
            long subRangeEnd = randomLongBetween(subRangeStart, completeUpTo);
            var subRange = ByteRange.of(subRangeStart, subRangeEnd);

            var range = ByteRange.of(randomLongBetween(0L, subRangeStart), randomLongBetween(subRangeEnd, bytes.length));
            var future = new PlainActionFuture<Void>();

            var gaps = tracker.waitForRange(range, subRange, wrapper.apply(subRange, future));
            assertThat(future.isDone(), equalTo(true));
            assertThat(gaps.isEmpty(), is(true));
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
            final List<SparseFileTracker.Gap> gaps = sparseFileTracker.waitForRange(
                range,
                range,
                ActionTestUtils.assertNoFailureListener(ignored -> {
                    assertTrue(expectNotification.get());
                    assertTrue(wasNotified.compareAndSet(false, true));
                })
            ).get().claim();
            for (int gapIndex = 0; gapIndex < gaps.size(); gapIndex++) {
                final SparseFileTracker.Gap gap = gaps.get(gapIndex);
                assertThat(gap.start(), greaterThanOrEqualTo(start));
                assertThat(gap.end(), lessThanOrEqualTo(end));
                for (long i = gap.start(); i < gap.end(); i++) {
                    assertThat(fileContents[toIntBytes(i)], equalTo(UNAVAILABLE));
                    fileContents[toIntBytes(i)] = AVAILABLE;
                    gap.onProgress(i + 1L);
                    assertThat(wasNotified.get(), equalTo(false));
                }
                // listener is notified when the last gap is completed
                if (gapIndex == gaps.size() - 1) {
                    expectNotification.set(true);
                }
                assertThat(wasNotified.get(), equalTo(false));
                gap.onCompletion();
                assertThat(wasNotified.get(), equalTo(expectNotification.get()));
            }
            assertTrue(wasNotified.get());
        }

        final AtomicBoolean wasNotified = new AtomicBoolean();
        assertThat(
            sparseFileTracker.waitForRange(
                range,
                range,
                ActionTestUtils.assertNoFailureListener(ignored -> assertTrue(wasNotified.compareAndSet(false, true)))
            ).isEmpty(),
            is(true)
        );
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
            final ActionListener<Void> listener = ActionTestUtils.assertNoFailureListener(
                ignored -> assertTrue(wasNotified.compareAndSet(false, true))
            );
            final List<SparseFileTracker.Gap> gaps = sparseFileTracker.waitForRange(range, subRange, listener)
                .map(SparseFileTracker.Gaps::claim)
                .orElse(List.of());

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
            final ActionListener<Void> waitIfPendingListener = ActionTestUtils.assertNoFailureListener(
                ignored -> assertTrue(waitIfPendingWasNotified.compareAndSet(false, true))
            );
            assertFalse(sparseFileTracker.waitForRangeIfPending(subRange, waitIfPendingListener));

            final AtomicBoolean wasNotified = new AtomicBoolean();
            final AtomicBoolean expectNotification = new AtomicBoolean();
            final var gaps = sparseFileTracker.waitForRange(range, subRange, ActionTestUtils.assertNoFailureListener(ignored -> {
                assertTrue(expectNotification.get());
                assertTrue(wasNotified.compareAndSet(false, true));
            }));

            assertFalse("Listener should not have been executed yet", wasNotified.get());
            assertTrue("Expected gaps since subRange has unavailable bytes", gaps.isPresent());
            final List<SparseFileTracker.Gap> gapsList = gaps.get().claim();

            assertTrue(sparseFileTracker.waitForRangeIfPending(subRange, waitIfPendingListener));
            assertFalse(waitIfPendingWasNotified.get());

            long triggeringProgress = -1L;
            for (long i = subRange.start(); i < subRange.end(); i++) {
                if (fileContents[toIntBytes(i)] == UNAVAILABLE) {
                    triggeringProgress = i;
                }
            }
            assertThat(triggeringProgress, greaterThanOrEqualTo(0L));

            for (final SparseFileTracker.Gap gap : gapsList) {
                assertThat(gap.start(), greaterThanOrEqualTo(range.start()));
                assertThat(gap.end(), lessThanOrEqualTo(range.end()));

                final boolean completeBeforeEndOfGap = triggeringProgress < gap.end() - 1L; // gap.end is exclusive
                long from = gap.start();
                long written = 0L;

                for (long i = gap.start(); i < gap.end(); i++) {
                    assertThat(fileContents[toIntBytes(i)], equalTo(UNAVAILABLE));
                    fileContents[toIntBytes(i)] = AVAILABLE;
                    written += 1L;
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

                    long progress = from + written;
                    gap.onProgress(progress);

                    if (completeBeforeEndOfGap) {
                        assertThat(
                            "Listener should not have been called before ["
                                + triggeringProgress
                                + "] is reached, but it was triggered after progress got updated to ["
                                + i
                                + ']',
                            wasNotified.get() && waitIfPendingWasNotified.get(),
                            equalTo(triggeringProgress < progress)
                        );
                    } else {
                        assertThat(
                            "Listener should not have been called before gap  ["
                                + gap
                                + "] is completed, but it was triggered after progress got updated to ["
                                + i
                                + ']',
                            wasNotified.get() && waitIfPendingWasNotified.get(),
                            equalTo(false)
                        );
                    }

                    if (progress == gap.end()) {
                        gap.onCompletion();
                    }
                }

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
        assertThat(
            sparseFileTracker.waitForRange(
                range,
                subRange,
                ActionTestUtils.assertNoFailureListener(ignored -> assertTrue(wasNotified.compareAndSet(false, true)))
            ).isEmpty(),
            is(true)
        );
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

    /**
     * When a second caller requests a range that partially overlaps an existing unclaimed pending range, the tracker
     * splits the overlapping range at the boundary so that each caller receives only gaps that lie within its own
     * requested range.
     */
    public void testClaimedGapsDoNotExtendBeyondRequestedRangeWhenCaller1FillsFirst() {
        final SparseFileTracker tracker = new SparseFileTracker("test", 100);

        // Caller 1 creates a pending range [0, 50) but only needs [0, 40) — subRange ends inside the
        // split-off upper half, so its listener is wired to only fire when completes and B reaches 40
        final PlainActionFuture<Void> future1 = new PlainActionFuture<>();
        final var gaps1 = tracker.waitForRange(ByteRange.of(0, 50), ByteRange.of(0, 40), future1);
        assertTrue(gaps1.isPresent());

        // Caller 2 requests [30, 80) with subRange [30, 70) — its listener threshold on the gap [50,80) is
        // capped to 70 rather than 80, exercising the Math.min path in subscribeToCompletionListeners.
        final PlainActionFuture<Void> future2 = new PlainActionFuture<>();
        final var gaps2 = tracker.waitForRange(ByteRange.of(30, 80), ByteRange.of(30, 70), future2);
        assertTrue(gaps2.isPresent());

        // Caller 2 claims: all gaps must be within [30, 80)
        final List<SparseFileTracker.Gap> gaps2List = gaps2.get().claim();
        assertThat(gaps2List.size(), equalTo(2)); // [30, 50) and [50, 80)
        assertThat(gaps2List.get(0).start(), equalTo(30L));
        assertThat(gaps2List.get(0).end(), equalTo(50L));
        assertThat(gaps2List.get(1).start(), equalTo(50L));
        assertThat(gaps2List.get(1).end(), equalTo(80L));

        // Caller 1 claims: gets the [0, 30) split-off portion (still within [0, 50))
        final List<SparseFileTracker.Gap> gaps1List = gaps1.get().claim();
        assertThat(gaps1List.size(), equalTo(1)); // [0, 30)
        assertThat(gaps1List.get(0).start(), equalTo(0L));
        assertThat(gaps1List.get(0).end(), equalTo(30L));

        // Fill caller 1's gap [0, 30): future1 needs subRange [0,40) — A is done but B has not yet
        // reached 40, so the bothFiredRef for future1 has not closed.
        for (SparseFileTracker.Gap gap : gaps1List) {
            for (long i = gap.start(); i < gap.end(); i++) {
                if (randomBoolean()) {
                    gap.onProgress(i + 1);
                }
            }
            gap.onCompletion();
        }

        // future1 is not done yet: B=[30,50) has not yet reached 40 (end of subRange)
        assertFalse(future1.isDone());

        // Fill caller 2's gaps [30, 50) and [50, 80): future1 fires when B reaches 40;
        // future2 fires when the gap [50,80) reaches 70 (end of its subRange).
        boolean future1ShouldBeDone = false;
        for (SparseFileTracker.Gap gap : gaps2List) {
            for (long i = gap.start(); i < gap.end(); i++) {
                if (randomBoolean()) {
                    gap.onProgress(i + 1);
                    // future1 is completed when gaps are filled to its upper boundary of 40. However, if the gap.onProgress is called
                    // with the gap's end, i.e. 50, the gap's listeners are not invoked until gap.onCompletion is called. So we include
                    // that in the condition check.
                    if (i + 1 >= 40 && i + 1 != 50L) {
                        future1ShouldBeDone = true;
                    }
                    assertThat(future1.isDone(), equalTo(future1ShouldBeDone));
                }
            }
            gap.onCompletion();
        }

        // Both listeners must have fired now
        assertTrue(future1.isDone());
        assertTrue(future2.isDone());
    }

    /**
     * When caller 2 claims before caller 1 in the overlapping case, caller 2 still only gets gaps within its own
     * range and caller 1 picks up the remainder. Using range != subRange verifies that the bothFiredRef on the split
     * correctly requires both halves before firing the listener regardless of fill order.
     */
    public void testClaimedGapsDoNotExtendBeyondRequestedRangeWhenCaller2FillsFirst() {
        final SparseFileTracker tracker = new SparseFileTracker("test", 100);

        // Caller 1 range [0,50), subRange [0,40): listener threshold 40 lies inside B=[30,50), so the
        // bothFiredRef from splitRange gates future1 on A completing AND B reaching 40.
        final PlainActionFuture<Void> future1 = new PlainActionFuture<>();
        final var gaps1 = tracker.waitForRange(ByteRange.of(0, 50), ByteRange.of(0, 40), future1);
        assertTrue(gaps1.isPresent());

        // Caller 2 range [30, 80), subRange [30, 70)
        final PlainActionFuture<Void> future2 = new PlainActionFuture<>();
        final var gaps2 = tracker.waitForRange(ByteRange.of(30, 80), ByteRange.of(30, 70), future2);
        assertTrue(gaps2.isPresent());

        // Caller 2 claims first
        final List<SparseFileTracker.Gap> gaps2List = gaps2.get().claim();
        assertThat(gaps2List.size(), equalTo(2)); // [30, 50) and [50, 80)
        assertThat(gaps2List.get(0).start(), equalTo(30L));
        assertThat(gaps2List.get(0).end(), equalTo(50L));
        assertThat(gaps2List.get(1).start(), equalTo(50L));
        assertThat(gaps2List.get(1).end(), equalTo(80L));

        // Caller 1 then claims its portion
        final List<SparseFileTracker.Gap> gaps1List = gaps1.get().claim();
        assertThat(gaps1List.size(), equalTo(1)); // [0, 30)
        assertThat(gaps1List.get(0).start(), equalTo(0L));
        assertThat(gaps1List.get(0).end(), equalTo(30L));

        // Fill caller 2's gaps first: when B=[30,50) passes 40 the listener is not yet invoked
        boolean future2ShouldBeDone = false;
        for (SparseFileTracker.Gap gap : gaps2List) {
            for (long i = gap.start(); i < gap.end(); i++) {
                if (randomBoolean()) {
                    gap.onProgress(i + 1);
                    // future2 is completed when gaps are filled to its upper boundary of 70. However, if the gap.onProgress is called
                    // with the gap's end, i.e. 80, the gap's listeners are not invoked until gap.onCompletion is called. So we include
                    // that in the condition check.
                    if (i + 1 >= 70 && i + 1 != 80L) {
                        future2ShouldBeDone = true;
                    }
                    assertThat(future2.isDone(), equalTo(future2ShouldBeDone));
                }
            }
            gap.onCompletion();
        }

        assertFalse(future1.isDone()); // A=[0,30) not done; botFiredRef still open
        assertTrue(future2.isDone());

        for (SparseFileTracker.Gap gap : gaps1List) {
            for (long i = gap.start(); i < gap.end(); i++) {
                if (randomBoolean()) {
                    gap.onProgress(i + 1);
                }
            }
            gap.onCompletion();
        }

        assertTrue(future1.isDone());
        assertTrue(future2.isDone());
    }

    /**
     * When an existing unclaimed pending range spans entirely beyond the requested range (starts before and ends
     * after), the returned gap covers exactly the intersection. Using subRange=[0,80) for caller 1 places its
     * listener threshold at 80, which after the double-split lands on D=[60,100) and is redistributed correctly
     * by the second splitRange call.
     */
    public void testGapClippedWhenExistingRangeSpansEntireRequestedRange() {
        final SparseFileTracker tracker = new SparseFileTracker("test", 100);

        // Caller 1 range [0, 100), subRange [0, 80): threshold 80 will be redistributed to D=[60,100)
        // by the second split, so future1 fires when [0,80) is fully available.
        final PlainActionFuture<Void> future1 = new PlainActionFuture<>();
        final var gaps1 = tracker.waitForRange(ByteRange.of(0, 100), ByteRange.of(0, 80), future1);
        assertTrue(gaps1.isPresent());

        // Caller 2 requests a narrow inner range [20, 60)
        final PlainActionFuture<Void> future2 = new PlainActionFuture<>();
        final var gaps2 = tracker.waitForRange(ByteRange.of(20, 60), ByteRange.of(20, 60), future2);
        assertTrue(gaps2.isPresent());

        // Caller 2 gets exactly [20, 60)
        final List<SparseFileTracker.Gap> gaps2List = gaps2.get().claim();
        assertThat(gaps2List.size(), equalTo(1));
        assertThat(gaps2List.get(0).start(), equalTo(20L));
        assertThat(gaps2List.get(0).end(), equalTo(60L));

        // Caller 1 claims [0, 20) and [60, 100)
        final List<SparseFileTracker.Gap> gaps1List = gaps1.get().claim();
        assertThat(gaps1List.size(), equalTo(2));
        assertThat(gaps1List.get(0).start(), equalTo(0L));
        assertThat(gaps1List.get(0).end(), equalTo(20L));
        assertThat(gaps1List.get(1).start(), equalTo(60L));
        assertThat(gaps1List.get(1).end(), equalTo(100L));

        // Fill caller 2's gap [20, 60): future2 fires; future1 still needs A=[0,20) and D to reach 80
        for (SparseFileTracker.Gap gap : gaps2List) {
            for (long i = gap.start(); i < gap.end(); i++) {
                gap.onProgress(i + 1);
            }
            gap.onCompletion();
        }
        assertTrue(future2.isDone());
        assertFalse(future1.isDone()); // A and D not yet filled

        // Fill A=[0, 20): A-side bothFiredRef fires but D has not reached 80 yet
        final SparseFileTracker.Gap gapA = gaps1List.get(0);
        for (long i = gapA.start(); i < gapA.end(); i++) {
            gapA.onProgress(i + 1);
        }
        gapA.onCompletion();
        assertFalse(future1.isDone()); // D=[60,100) has not yet reached 80 (subRange end)

        // Fill D=[60, 100): future1 fires when D reaches 80, before D even completes
        final SparseFileTracker.Gap gapD = gaps1List.get(1);
        for (long i = gapD.start(); i < gapD.end(); i++) {
            if (randomBoolean()) {
                gapD.onProgress(i + 1);
                if (i >= 80) {
                    assertTrue(future1.isDone());
                }
            }
        }
        gapD.onCompletion();
        assertTrue(future1.isDone());
    }

    /**
     * Multiple callers that invoke waitForRange before any of them call claim all receive Optional.of(Gaps), but
     * claim() is exclusive: the union of gaps returned across all callers covers each absent byte exactly once.
     */
    public void testWaitForRangeClaimExclusivity() {
        final long fileLength = between(2, 100);
        final SparseFileTracker tracker = new SparseFileTracker("test", fileLength);

        // Each caller requests a random sub-range so gaps are spread across different parts of the file
        final int callerCount = between(2, 5);
        final List<ByteRange> ranges = new ArrayList<>();
        final List<SparseFileTracker.Gaps> pendingGaps = new ArrayList<>();
        final List<PlainActionFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < callerCount; i++) {
            final long start = randomLongBetween(0, fileLength - 1);
            final long end = randomLongBetween(start + 1, fileLength);
            final ByteRange range = ByteRange.of(start, end);
            ranges.add(range);
            final PlainActionFuture<Void> future = new PlainActionFuture<>();
            futures.add(future);
            // No claim() has run yet, so every caller must see unclaimed gaps
            final var gaps = tracker.waitForRange(range, range, future);
            assertTrue(gaps.isPresent());
            pendingGaps.add(gaps.get());
        }

        // Claim in shuffled order: the winner of each byte is whoever calls claim() first, not who called waitForRange first
        Collections.shuffle(pendingGaps, random());
        final List<SparseFileTracker.Gap> allClaimed = new ArrayList<>();
        for (SparseFileTracker.Gaps gaps : pendingGaps) {
            allClaimed.addAll(gaps.claim());
        }

        // Total claimed bytes must equal the size of the union of all requested ranges
        assertThat(allClaimed.stream().mapToLong(g -> g.end() - g.start()).sum(), equalTo(unionSize(fileLength, ranges)));

        futures.forEach(f -> assertThat(f.isDone(), is(false)));

        // Fill all claimed gaps; all listeners must fire once their range is complete
        for (SparseFileTracker.Gap gap : allClaimed) {
            for (long i = gap.start(); i < gap.end(); i++) {
                if (between(0, 100) < 10) {
                    gap.onProgress(i + 1);
                }
            }
            gap.onCompletion();
        }
        futures.forEach(f -> assertThat(f.isDone(), is(true)));
    }

    private static long unionSize(long fileLength, List<ByteRange> ranges) {
        final BitSet covered = new BitSet((int) fileLength);
        for (ByteRange range : ranges) {
            covered.set((int) range.start(), (int) range.end());
        }
        return covered.cardinality();
    }

    public void testSplitDoesNotForwardProgressPastAFailedLowerHalf() {
        final int length = between(10, 1024);
        final byte[] bytes = new byte[length];
        final SparseFileTracker tracker = new SparseFileTracker("test", length);

        // Register a listener on [0, length] but only waiting for the sub-range [0, firstReadPosition], which will be completed by
        // the listeners from the following split read
        final int firstReadPosition = between(1, length / 2);
        final var firstReadListener = new PlainActionFuture<Void>();
        tracker.waitForRange(ByteRange.of(0, length), ByteRange.of(0, firstReadPosition), firstReadListener);

        // Trigger split at a position that is different from the firstReadPosition so that the new split
        // listeners forward to firstReadListener
        final int firstSplitPosition = randomValueOtherThan(firstReadPosition, () -> between(1, length - 1));
        final var lowerOptGaps = tracker.waitForRange(
            ByteRange.of(0, firstSplitPosition),
            ByteRange.of(0, firstSplitPosition),
            ActionTestUtils.assertNoSuccessListener(e -> {})
        );
        assertTrue(lowerOptGaps.isPresent());

        // Lower half [0, firstSplitPosition) fails, no bytes are ever written for it.
        final var lowerGaps = lowerOptGaps.get().claim();
        assertThat(lowerGaps, hasSize(1));
        lowerGaps.getFirst().onFailure(new ElasticsearchException("simulated"));

        // Read the upper half and fill the range
        final var upperGapListener = new PlainActionFuture<Void>();
        final var upperOptGaps = tracker.waitForRange(
            ByteRange.of(firstSplitPosition, length),
            ByteRange.of(firstSplitPosition, length),
            ActionTestUtils.assertNoFailureListener(upperGapListener::onResponse)
        );

        assertTrue(upperOptGaps.isPresent());
        final var upperGaps = upperOptGaps.get().claim();
        assertThat(upperGaps, hasSize(1));
        // Randomly register another (smaller) read for the upper half. It must not lead to incorrect forward either.
        if (randomBoolean()) {
            tracker.waitForRange(
                ByteRange.of(firstSplitPosition, length - 1),
                ByteRange.of(firstSplitPosition, length - 1),
                ActionTestUtils.assertNoFailureListener(r -> {})
            );
        }
        // Upper half [firstSplitPosition, length) fills successfully; its progress must not be forwarded to the initial firstReadListener
        fillGap(bytes, upperGaps.getFirst());

        assertTrue(upperGapListener.isDone());
        assertTrue(firstReadListener.isDone());
        expectThrows(ElasticsearchException.class, firstReadListener::actionGet);

        // tracker must not update the complete pointer since the lower half failed
        assertThat(tracker.getComplete(), equalTo(0L));

        // The filled upper range is available to future read
        {
            final var startOfAnotherUpperRange = between(firstSplitPosition, length - 1);
            final var anotherUpperRange = ByteRange.of(startOfAnotherUpperRange, between(startOfAnotherUpperRange, length));
            final var future = new PlainActionFuture<Void>();
            assertTrue(
                tracker.waitForRange(anotherUpperRange, anotherUpperRange, ActionTestUtils.assertNoFailureListener(future::onResponse))
                    .isEmpty()
            );
            assertTrue(future.isDone());
        }

        // Read the lower range again and it should see the gap again and able to be re-filled
        {
            final var anotherLowerRange = ByteRange.of(0, between(firstSplitPosition, length));
            final var future = new PlainActionFuture<Void>();
            final var anotherOptGapsForLower = tracker.waitForRange(
                anotherLowerRange,
                anotherLowerRange,
                ActionTestUtils.assertNoFailureListener(future::onResponse)
            );
            assertTrue(anotherOptGapsForLower.isPresent());
            final var anotherGapsForLower = anotherOptGapsForLower.get().claim();
            assertThat(anotherGapsForLower, hasSize(1));
            // Fill the lower range
            fillGap(bytes, anotherGapsForLower.getFirst());
            assertTrue(future.isDone());
        }

        // Tracker's complete pointer should be updated since both lower and upper ranges are now filled
        assertThat(tracker.getComplete(), equalTo((long) length));
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
                safeAwait(startLatch);
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
            assertThat(
                sparseFileTracker.waitForRange(
                    completedRange,
                    completedRange,
                    ActionTestUtils.assertNoFailureListener(ignored -> listenerCalled.set(true))
                ).isEmpty(),
                is(true)
            );
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

    public void testCompletePointerUpdatesOnProgress() {
        // min length of 2 to have at least one progress update before reaching the end
        byte[] bytes = new byte[between(2, 1024)];
        var tracker = new SparseFileTracker(getTestName(), bytes.length);

        long position = 0L;
        for (int i = 0; i < 25 && position < tracker.getLength() - 1L; i++) {
            var progress = randomLongBetween(position + 1L, tracker.getLength() - 1L);

            var listener = new PlainActionFuture<Void>();
            var gaps = tracker.waitForRange(
                ByteRange.of(position, progress),
                ByteRange.of(position, progress),
                ActionListener.runBefore(listener, () -> assertThat(tracker.getComplete(), equalTo(progress)))
            );
            assertThat(listener.isDone(), equalTo(false));
            assertThat(gaps.isPresent(), is(true));
            var gapsList = gaps.get().claim();
            assertThat(gapsList, hasSize(1));

            gapsList.forEach(gap -> {
                long latestUpdatedCompletePointer = gap.start();

                for (long j = gap.start(); j < gap.end(); j++) {
                    final PlainActionFuture<Void> awaitingListener;
                    if (randomBoolean()) {
                        awaitingListener = new PlainActionFuture<>();
                        var moreGaps = tracker.waitForRange(
                            ByteRange.of(gap.start(), j + 1L),
                            ByteRange.of(gap.start(), j + 1L),
                            awaitingListener
                        );
                        assertThat(moreGaps.isEmpty(), equalTo(true));
                    } else {
                        awaitingListener = null;
                    }

                    assertThat(bytes[toIntBytes(j)], equalTo(UNAVAILABLE));
                    bytes[toIntBytes(j)] = AVAILABLE;
                    gap.onProgress(j + 1L);

                    if (awaitingListener != null && j < gap.end() - 1L) {
                        assertThat(
                            "Complete pointer should have been updated when a listener is waiting for the gap to be completed",
                            tracker.getComplete(),
                            equalTo(j + 1L)
                        );
                        assertThat(awaitingListener.isDone(), equalTo(true));
                        latestUpdatedCompletePointer = tracker.getComplete();
                    } else {
                        assertThat(
                            "Complete pointer is not updated if no listeners are waiting for the gap to be completed",
                            tracker.getComplete(),
                            equalTo(latestUpdatedCompletePointer)
                        );
                    }
                }
                gap.onCompletion();
                assertThat(tracker.getComplete(), equalTo(gap.end()));
            });
            position = progress;
        }
    }

    public void testGetAbsentBytesWithin() {
        final int length = between(100, 2048);
        var tracker = new SparseFileTracker(getTestName(), length);
        long begin = randomLongBetween(0, length);
        long end = randomLongBetween(begin, length);
        long amount = end - begin;

        for (int i = 0; i < 10; ++i) {
            assertThat(tracker.getAbsentBytesWithin(ByteRange.of(begin, end)), equalTo(amount));
            long start = randomLongBetween(0, length);
            ByteRange range = ByteRange.of(start, randomLongBetween(start, length));
            List<SparseFileTracker.Gap> gaps = tracker.waitForRange(range, range, ActionListener.noop())
                .map(SparseFileTracker.Gaps::claim)
                .orElse(List.of());
            assertThat(tracker.getAbsentBytesWithin(ByteRange.of(begin, end)), equalTo(amount));
            long sum = gaps.stream().mapToLong(g -> Math.max(Math.min(g.end(), end) - Math.max(g.start(), begin), 0)).sum();
            gaps.forEach(g -> g.onCompletion());
            amount -= sum;
            assertThat(tracker.getAbsentBytesWithin(ByteRange.of(begin, end)), equalTo(amount));
        }
    }

    private static void checkRandomAbsentRange(byte[] fileContents, SparseFileTracker sparseFileTracker, boolean expectExact) {
        final long checkStart = randomLongBetween(0, fileContents.length - 1);
        final long checkEnd = randomLongBetween(checkStart, fileContents.length);

        ByteRange checkRange = ByteRange.of(checkStart, checkEnd);
        final ByteRange freeRange = sparseFileTracker.getAbsentRangeWithin(checkRange);
        final long bytes = sparseFileTracker.getAbsentBytesWithin(checkRange);
        if (freeRange == null) {
            for (long i = checkStart; i < checkEnd; i++) {
                assertThat(fileContents[toIntBytes(i)], equalTo(AVAILABLE));
            }
            assertThat(bytes, equalTo(0L));
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
                assertThat(bytes, greaterThanOrEqualTo(1L));
            }
            assertThat(bytes, lessThanOrEqualTo(freeRange.end() - freeRange.start()));
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
            ).map(SparseFileTracker.Gaps::claim).orElse(List.of());

            long gapSum = gaps.stream().mapToLong(g -> Math.min(g.end(), rangeEnd) - Math.max(g.start(), rangeStart)).sum();
            assertThat(sparseFileTracker.getAbsentBytesWithin(ByteRange.of(rangeStart, rangeEnd)), greaterThanOrEqualTo(gapSum));
            gaps.forEach(g -> {
                assertThat(sparseFileTracker.getAbsentBytesWithin(ByteRange.of(g.start(), g.end())), equalTo(g.end() - g.start()));
                assertThat(sparseFileTracker.getAbsentBytesWithin(ByteRange.of(g.start() + 1, g.end())), equalTo(g.end() - g.start() - 1));
                assertThat(sparseFileTracker.getAbsentBytesWithin(ByteRange.of(g.start(), g.end() - 1)), equalTo(g.end() - g.start() - 1));
            });

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

    private static void fillGap(byte[] fileContents, SparseFileTracker.Gap gap) {
        for (long i = gap.start(); i < gap.end(); i++) {
            assertThat(fileContents[toIntBytes(i)], equalTo(UNAVAILABLE));
        }
        for (long i = gap.start(); i < gap.end(); i++) {
            fileContents[toIntBytes(i)] = AVAILABLE;
            gap.onProgress(i + 1L);
        }
        gap.onCompletion();
    }

    public void testSplitRangeCompletesListenersOnBothHalves() {
        // Register a listener on [0, 100]. Leave unclaimed so a second narrower request triggers a split.
        // Then fill A [0, 50] and B [50, 100] separately and verify the original listener fires.
        final SparseFileTracker tracker = new SparseFileTracker("test", 100);

        final AtomicBoolean fullListenerCalled = new AtomicBoolean();
        final var fullGaps = tracker.waitForRange(
            ByteRange.of(0, 100),
            ByteRange.of(0, 100),
            ActionTestUtils.assertNoFailureListener(ignored -> fullListenerCalled.set(true))
        );
        assertTrue(fullGaps.isPresent());
        assertFalse(fullListenerCalled.get());

        // Requesting [0, 50] while [0, 100] is unclaimed splits it into A=[0,50] and B=[50,100].
        final AtomicBoolean midListenerCalled = new AtomicBoolean();
        final var midGaps = tracker.waitForRange(
            ByteRange.of(0, 50),
            ByteRange.of(0, 50),
            ActionTestUtils.assertNoFailureListener(ignored -> midListenerCalled.set(true))
        );
        assertTrue(midGaps.isPresent());

        // Fill A [0, 50].
        final var aGaps = midGaps.get().claim();
        assertThat(aGaps, hasSize(1));
        final var aGap = aGaps.get(0);
        assertThat(aGap.start(), equalTo(0L));
        assertThat(aGap.end(), equalTo(50L));
        for (long i = 0; i < 50; i++) {
            aGap.onProgress(i + 1L);
        }
        aGap.onCompletion();
        assertTrue("Mid-range listener must fire when A completes", midListenerCalled.get());
        assertFalse("Full listener must not fire until B also completes", fullListenerCalled.get());

        // Fill B [50, 100] via the original Gaps object (which covers [0, 100]).
        final var bGaps = fullGaps.get().claim();
        assertThat(bGaps, hasSize(1));
        final var bGap = bGaps.get(0);
        assertThat(bGap.start(), equalTo(50L));
        assertThat(bGap.end(), equalTo(100L));
        for (long i = 50; i < 100; i++) {
            bGap.onProgress(i + 1L);
        }
        bGap.onCompletion();
        assertTrue("Full listener must fire when B also completes", fullListenerCalled.get());
    }

    public void testSplitRangeForwardsIntermediateProgressToExistingListeners() {
        // Register a listener on [0, 100] but only waiting for the sub-range [0, 30].
        // Split at 50. Verify the listener fires exactly when A reaches byte 30, not before.
        final SparseFileTracker tracker = new SparseFileTracker("test", 100);

        final AtomicBoolean listener30Called = new AtomicBoolean();
        final var fullGaps = tracker.waitForRange(
            ByteRange.of(0, 100),
            ByteRange.of(0, 30),
            ActionTestUtils.assertNoFailureListener(ignored -> listener30Called.set(true))
        );
        assertTrue(fullGaps.isPresent());

        // Trigger split of [0, 100] at 50.
        final var midGaps = tracker.waitForRange(ByteRange.of(0, 50), ByteRange.of(0, 50), ActionListener.noop());
        assertTrue(midGaps.isPresent());

        // Claim and fill A [0, 50] byte-by-byte, checking the listener fires at byte 30.
        final var aGaps = midGaps.get().claim();
        assertThat(aGaps, hasSize(1));
        final var aGap = aGaps.get(0);
        for (long i = 0; i < 50; i++) {
            if (i < 30) {
                assertFalse("Listener for [0,30] must not fire before byte 30 is reached", listener30Called.get());
            }
            aGap.onProgress(i + 1L);
        }
        aGap.onCompletion();
        assertTrue("Listener for [0,30] must fire once A has filled through byte 30", listener30Called.get());
    }

    /**
     * When a pending range is split and a new listener is subscribed directly to the lower or upper sub-future,
     * the {@code complete} pointer must advance when that listener fires.
     * This requires the sub-futures created in {@link ProgressListenableActionFuture#split} to carry
     * the outer future's {@code progressConsumer} (i.e. {@code updateCompletePointer}).
     */
    public void testSplitProgressConsumerAdvancesCompletePointer() {
        final SparseFileTracker tracker = new SparseFileTracker("test", 100);

        // Wait for [0, 100) — creates unclaimed gap [0, 100) with progressConsumer = updateCompletePointer
        // because rangeStart=0 equals complete=0 at creation time.
        final var fullGaps = tracker.waitForRange(ByteRange.of(0, 100), ByteRange.of(0, 100), ActionListener.noop());
        assertTrue(fullGaps.isPresent());

        // Waiting for [0, 50) while [0, 100) is unclaimed triggers a split at 50.
        // The lower sub-future [0, 50) must inherit the outer future's progressConsumer.
        final var midGaps = tracker.waitForRange(ByteRange.of(0, 50), ByteRange.of(0, 50), ActionListener.noop());
        assertTrue(midGaps.isPresent());

        // Claim the lower half so it can receive progress updates.
        final var lowerGaps = midGaps.get().claim();
        assertThat(lowerGaps, hasSize(1));
        final var lowerGap = lowerGaps.get(0);
        assertThat(lowerGap.start(), equalTo(0L));
        assertThat(lowerGap.end(), equalTo(50L));

        // Subscribe a listener at threshold 30 directly on the lower split sub-future.
        // waitForRangeIfPending adds it to lower's completionListener at threshold min(50, 30) = 30.
        final AtomicBoolean listener30Fired = new AtomicBoolean();
        assertTrue(
            tracker.waitForRangeIfPending(
                ByteRange.of(0, 30),
                ActionTestUtils.assertNoFailureListener(ignored -> listener30Fired.set(true))
            )
        );
        assertFalse("listener at threshold 30 must not have fired", listener30Fired.get());

        // Advance lower's progress to byte 30: the listener fires, and progressConsumer must call
        // updateCompletePointer(30) so that complete advances.
        lowerGap.onProgress(30L);
        assertTrue("listener at threshold 30 must have fired", listener30Fired.get());
        assertThat(
            "complete must advance to 30 when a listener on the lower split sub-future fires at that threshold",
            tracker.getComplete(),
            equalTo(30L)
        );

        // Upper sub-future: the progressConsumer is gated on lower.isDone(). Subscribe a listener to upper
        // at threshold 80 while lower is still pending, advance upper to 80, and confirm complete does NOT
        // advance (lower is not done yet, so bytes [0, 50) are not guaranteed available).
        final var upperGaps = fullGaps.get().claim();
        assertThat(upperGaps, hasSize(1));
        final var upperGap = upperGaps.get(0);
        assertThat(upperGap.start(), equalTo(50L));
        assertThat(upperGap.end(), equalTo(100L));

        final AtomicBoolean listener80Fired = new AtomicBoolean();
        assertTrue(
            tracker.waitForRangeIfPending(
                ByteRange.of(50, 80),
                ActionTestUtils.assertNoFailureListener(ignored -> listener80Fired.set(true))
            )
        );

        upperGap.onProgress(80L);
        assertTrue("listener at threshold 80 must have fired", listener80Fired.get());
        assertThat(
            "complete must not advance past 30 when upper's listener fires while lower is still pending",
            tracker.getComplete(),
            equalTo(30L)
        );

        // Now complete lower: onGapSuccess advances complete to 50 via maybeUpdateCompletePointer.
        for (long i = 30L; i < 50L; i++) {
            lowerGap.onProgress(i + 1L);
        }
        lowerGap.onCompletion();
        assertThat("complete must advance to 50 when the lower gap completes", tracker.getComplete(), equalTo(50L));

        // Subscribe a second listener to upper at threshold 90, now that lower is done.
        final AtomicBoolean listener90Fired = new AtomicBoolean();
        assertTrue(
            tracker.waitForRangeIfPending(
                ByteRange.of(50, 90),
                ActionTestUtils.assertNoFailureListener(ignored -> listener90Fired.set(true))
            )
        );

        upperGap.onProgress(90L);
        assertTrue("listener at threshold 90 must have fired", listener90Fired.get());
        assertThat("complete must advance to 90 when upper's listener fires after lower is done", tracker.getComplete(), equalTo(90L));
    }
}
