/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache.shared;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.LongSupplier;

/**
 * Tracks counts in fixed-duration time slots using a fixed array of buckets indexed by slot time. Slots are
 * defined by a time granularity, a past retention count, and a future retention count. E.g., 10:37 would be in
 * slot 10:00 for 1h slots.
 * <p>
 * Used at node level for cache boost quota tracking (expected totals from shard data, and actual
 * totals from cache residency). Callers pass arbitrary counts via {@link #add} and {@link #remove};
 * this type is agnostic to what is being counted (the expectation is that it is cache regions).
 * <p>
 * Fixed relative window sums can be registered via {@link #registerRelativeWindow} and read
 * through {@link TimeSlottedCounterWindow#sum()}. The class automatically updates registered
 * window sums as counts change. We expect a limited number of such windows, corresponding to the
 * configured boost levels.
 * <p>
 * The array is anchored at construction time and never rolled. It spans {@code [now - past, now + future]}
 * at startup. Bucket sums are updated atomically; {@link #windowLock} coordinates registered window
 * slides with concurrent {@link #add} and {@link #remove} calls.
 * <p>
 * Timestamps older than the tail slot are clamped to the tail bucket. Timestamps beyond the construction head
 * slot are clamped to the head bucket. If the node runs longer than the configured future retention without
 * restart, the sliding-window anchor stays at the head slot and all new counts beyond that slot accumulate
 * in the head bucket; registered relative-window sums and {@link #sum} range queries then lose time resolution
 * for post-retention data until restart (quota accuracy degrades). Nodes are expected to restart at least once
 * within {@code time_slots.future.count * time_slots.granularity} (defaults: one year at hourly slots).
 */
public class TimeSlottedCounter {

    private static final Logger logger = LogManager.getLogger(TimeSlottedCounter.class);

    private final long granularityMillis;
    private final int pastBuckets;
    private final int futureBuckets;
    private final LongSupplier timeProvider;
    private final ReadWriteLock windowLock = new ReentrantReadWriteLock();
    private final List<TimeSlottedCounterWindow> registeredWindows = new ArrayList<>();

    private final AtomicLongArray counts;

    /** {@code slotStart} of the oldest retained (tail) bucket; fixed at construction. */
    private final long tailSlotStart;

    /** {@code slotStart} of the newest retained (head) bucket; fixed at construction. */
    private final long headSlotStart;

    /**
     * Creates a fixed array of {@code pastBuckets + futureBuckets} atomic count buckets, one per time slot of
     * {@code granularity}, anchored at the current wall-clock time.
     * <p>
     * Example ({@code pastBuckets=4}, {@code futureBuckets=2}, 1h granularity, clock at 12:00):
     * <pre>
     *   tail slot 09:00, anchor 12:00, head slot 14:00
     *   +--------------+--------------+--------------+--------------+--------------+--------------+
     *   | index 0      | index 1      | index 2      | index 3      | index 4      | index 5      |
     *   | tail         |              |              | anchor       |              | head         |
     *   +--------------+--------------+--------------+--------------+--------------+--------------+
     *   | 09:00-09:59  | 10:00-10:59  | 11:00-11:59  | 12:00-12:59  | 13:00-13:59  | 14:00-14:59  |
     *   +--------------+--------------+--------------+--------------+--------------+--------------+
     * </pre>
     *
     * @param granularity duration of each time slot
     * @param pastBuckets number of past slots retained, including the anchor slot
     * @param futureBuckets number of future slots retained beyond the anchor slot
     * @param timeProvider source of wall-clock time in milliseconds
     */
    public TimeSlottedCounter(TimeValue granularity, int pastBuckets, int futureBuckets, LongSupplier timeProvider) {
        if (granularity.millis() <= 0) {
            throw new IllegalArgumentException("granularity must be positive");
        }
        if (pastBuckets < 1) {
            throw new IllegalArgumentException("pastBuckets must be >= 1");
        }
        if (futureBuckets < 0) {
            throw new IllegalArgumentException("futureBuckets must be >= 0");
        }
        this.granularityMillis = granularity.millis();
        this.pastBuckets = pastBuckets;
        this.futureBuckets = futureBuckets;
        this.timeProvider = timeProvider;
        this.counts = new AtomicLongArray(pastBuckets + futureBuckets);
        long anchorSlot = slotStartForTimestamp(currentTimeMillis());
        this.tailSlotStart = anchorSlot - (long) (pastBuckets - 1) * granularityMillis;
        this.headSlotStart = anchorSlot + (long) futureBuckets * granularityMillis;
    }

    public static TimeSlottedCounter createFromSettings(Settings settings, LongSupplier timeProvider) {
        return new TimeSlottedCounter(
            SharedBlobCacheService.SHARED_CACHE_TIME_SLOTS_GRANULARITY_SETTING.get(settings),
            SharedBlobCacheService.SHARED_CACHE_TIME_SLOTS_PAST_COUNT_SETTING.get(settings),
            SharedBlobCacheService.SHARED_CACHE_TIME_SLOTS_FUTURE_COUNT_SETTING.get(settings),
            timeProvider
        );
    }

    public TimeValue granularity() {
        return TimeValue.timeValueMillis(granularityMillis);
    }

    long granularityMillis() {
        return granularityMillis;
    }

    public int pastBuckets() {
        return pastBuckets;
    }

    public int futureBuckets() {
        return futureBuckets;
    }

    public int maxBuckets() {
        return counts.length();
    }

    /**
     * Registers a fixed relative window whose sum is maintained incrementally by this counter.
     * <p>
     * {@code startOffsetMillis} and {@code endOffsetMillis} are clamped to whole slots at registration time.
     * The resulting window spans {@code [now - endOffset, now - startOffset)} in slot-aligned boundaries.
     * <p>
     * Example at {@code now=10d} with 1h slots, {@code registerRelativeWindow(1d, 3d)}:
     * <pre>
     *   exclusiveEndSlotsBackFromAnchor=24, inclusiveStartSlotsBackFromAnchor=72 — slots overlapping [7d, 9d)
     *
     *   |---- retained buckets ----|.... now ....|
     *                    ^windowStart    ^windowEnd
     * </pre>
     * The returned {@link TimeSlottedCounterWindow} is updated on every {@link #add}/{@link #remove} and window slide.
     */
    public TimeSlottedCounterWindow registerRelativeWindow(long startOffsetMillis, long endOffsetMillis) {
        if (startOffsetMillis < 0 || endOffsetMillis < 0) {
            throw new IllegalArgumentException("offsets must be non-negative");
        }
        if (endOffsetMillis <= startOffsetMillis) {
            throw new IllegalArgumentException("endOffsetMillis must be greater than startOffsetMillis");
        }
        long now = currentTimeMillis();
        int anchorOffset = anchorOffsetForNow(now);
        int nearSlotsBackFromAnchor = anchorOffset - offsetFromTailForSlotStart(slotStartForTimestamp(now - startOffsetMillis));
        int farSlotsBackFromAnchor = anchorOffset - offsetFromTailForSlotStart(slotStartForTimestamp(Math.max(0, now - endOffsetMillis)));
        int exclusiveEndSlotsBackFromAnchor = Math.min(nearSlotsBackFromAnchor, anchorOffset);
        int inclusiveStartSlotsBackFromAnchor = Math.min(farSlotsBackFromAnchor, anchorOffset);
        if (inclusiveStartSlotsBackFromAnchor <= exclusiveEndSlotsBackFromAnchor) {
            throw new IllegalArgumentException(
                "inclusiveStartSlotsBackFromAnchor must be greater than exclusiveEndSlotsBackFromAnchor after slot clamping"
            );
        }
        return registerRelativeWindow(exclusiveEndSlotsBackFromAnchor, inclusiveStartSlotsBackFromAnchor);
    }

    private TimeSlottedCounterWindow registerRelativeWindow(int exclusiveEndSlotsBackFromAnchor, int inclusiveStartSlotsBackFromAnchor) {
        windowLock.writeLock().lock();
        try {
            long now = currentTimeMillis();
            for (TimeSlottedCounterWindow registeredWindow : registeredWindows) {
                registeredWindow.advanceUnderWriteLock(now);
            }
            TimeSlottedCounterWindow window = new TimeSlottedCounterWindow(
                this,
                exclusiveEndSlotsBackFromAnchor,
                inclusiveStartSlotsBackFromAnchor
            );
            window.advanceUnderWriteLock(now);
            registeredWindows.add(window);
            return window;
        } finally {
            windowLock.writeLock().unlock();
        }
    }

    /**
     * Adds {@code count} to the slot containing {@code timestampMillis}.
     */
    public void add(long timestampMillis, long count) {
        if (count <= 0) {
            return;
        }
        mutateSlot(timestampMillis, count);
    }

    /**
     * Removes {@code count} from the slot containing {@code timestampMillis}.
     * <p>
     * The bucket is never driven below zero (excess remove is clamped).
     */
    public void remove(long timestampMillis, long count) {
        if (count <= 0) {
            return;
        }
        mutateSlot(timestampMillis, -count);
    }

    /**
     * Returns the incrementally maintained sum for {@code window} after advancing time if needed.
     * The sum is read under the same read lock used for slot mutations.
     */
    long readRegisteredWindowSum(TimeSlottedCounterWindow window) {
        windowLock.readLock().lock();
        try {
            ensureRegisteredWindowsAdvancedWhileHoldingReadLock();
            return window.cachedSum();
        } finally {
            windowLock.readLock().unlock();
        }
    }

    /**
     * Slides registered windows when the anchor slot has moved. Caller must hold {@link #windowLock} read lock;
     * may temporarily release it to acquire the write lock.
     */
    private void ensureRegisteredWindowsAdvancedWhileHoldingReadLock() {
        if (registeredWindows.isEmpty()) {
            return;
        }
        long now = currentTimeMillis();
        if (anyRegisteredWindowNeedsUpdate(now) == false) {
            return;
        }
        windowLock.readLock().unlock();
        windowLock.writeLock().lock();
        try {
            long advancedNow = currentTimeMillis();
            if (anyRegisteredWindowNeedsUpdate(advancedNow)) {
                for (TimeSlottedCounterWindow registeredWindow : registeredWindows) {
                    registeredWindow.advanceUnderWriteLock(advancedNow);
                }
            }
        } finally {
            windowLock.writeLock().unlock();
        }
        windowLock.readLock().lock();
        if (registeredWindows.isEmpty() == false && anyRegisteredWindowNeedsUpdate(currentTimeMillis())) {
            ensureRegisteredWindowsAdvancedWhileHoldingReadLock();
        }
    }

    private boolean anyRegisteredWindowNeedsUpdate(long now) {
        for (TimeSlottedCounterWindow registeredWindow : registeredWindows) {
            if (registeredWindow.needsUpdate(now)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the sum of counts in slots overlapping {@code [windowStartMillis, windowEndMillis)}.
     * The query range is clamped to {@code [tailSlotStart, headSlotStart + granularity)} before summing.
     * <p>
     * Example (1h slots, range {@code [10:00, 13:00)}):
     * <pre>
     *   includes slots 10:00, 11:00, 12:00 (each [slotStart, slotStart+granularity) intersects the range)
     *   excludes slot 13:00 (starts at range end)
     * </pre>
     */
    public long sum(long windowStartMillis, long windowEndMillis) {
        if (windowEndMillis <= windowStartMillis) {
            return 0;
        }
        windowStartMillis = Math.max(Math.max(0, windowStartMillis), tailSlotStart);
        long retainedEndExclusive = headSlotStart + granularityMillis;
        windowEndMillis = Math.min(windowEndMillis, retainedEndExclusive);
        if (windowEndMillis <= windowStartMillis) {
            return 0;
        }
        windowLock.readLock().lock();
        try {
            int firstOverlapping = offsetFromTailForSlotStart(slotStartForTimestamp(windowStartMillis));
            int firstNonOverlapping = offsetFromTailForSlotStart(slotStartForTimestamp(windowEndMillis - 1)) + 1;
            return sumBucketsInOffsetRange(firstOverlapping, firstNonOverlapping);
        } finally {
            windowLock.readLock().unlock();
        }
    }

    long currentTimeMillis() {
        return Math.max(0, timeProvider.getAsLong());
    }

    /**
     * Sum over {@code [startOffsetFromTail, endOffsetFromTail)}. Caller must hold {@link #windowLock} read or write lock.
     */
    long sumBucketsInOffsetRange(int startOffsetFromTail, int endOffsetFromTail) {
        int start = Math.max(0, startOffsetFromTail);
        int end = Math.min(counts.length(), endOffsetFromTail);
        if (end <= start) {
            return 0;
        }
        long total = 0;
        for (int offsetFromTail = start; offsetFromTail < end; offsetFromTail++) {
            total += counts.get(offsetFromTail);
        }
        return total;
    }

    int offsetFromTailForSlotStart(long slotStart) {
        return (int) Math.floorDiv(slotStart - tailSlotStart, granularityMillis);
    }

    /**
     * Slot start used as the sliding-window anchor for {@code now}. When wall clock has moved beyond
     * the construction head slot, the anchor stays at the head so relative windows remain within the
     * fixed bucket array.
     */
    long effectiveAnchorSlotStart(long now) {
        long wallSlot = slotStartForTimestamp(now);
        return wallSlot > headSlotStart ? headSlotStart : wallSlot;
    }

    int anchorOffsetForNow(long now) {
        return offsetFromTailForSlotStart(effectiveAnchorSlotStart(now));
    }

    private void mutateSlot(long timestampMillis, long delta) {
        windowLock.readLock().lock();
        try {
            ensureRegisteredWindowsAdvancedWhileHoldingReadLock();
            long slotStart = resolveSlotStart(timestampMillis);
            int index = offsetFromTailForSlotStart(slotStart);
            long appliedDelta = applyDelta(index, slotStart, delta);
            if (appliedDelta != 0) {
                for (TimeSlottedCounterWindow window : registeredWindows) {
                    if (window.containsSlot(slotStart)) {
                        window.onSlotDelta(appliedDelta);
                    }
                }
            }
        } finally {
            windowLock.readLock().unlock();
        }
    }

    private long applyDelta(int index, long slotStart, long delta) {
        assert index >= 0 && index < counts.length() : "index [" + index + "] out of range [0," + counts.length() + "]";
        if (delta > 0) {
            counts.addAndGet(index, delta);
            return delta;
        }
        long removal = -delta;
        long previous = counts.getAndAccumulate(index, removal, (current, amount) -> current < amount ? 0 : current - amount);
        if (previous < removal) {
            assert false
                : "remove clamped: slot start [" + slotStart + "] count [" + previous + "] less than remove count [" + removal + "]";
            logger.warn("remove clamped: slot start [{}] count [{}] less than remove count [{}]", slotStart, previous, removal);
        }
        return -Math.min(removal, previous);
    }

    /**
     * Maps a timestamp to the {@code slotStart} used for bucket lookup, clamped to the fixed array range.
     */
    private long resolveSlotStart(long timestampMillis) {
        return Math.clamp(slotStartForTimestamp(Math.max(0, timestampMillis)), tailSlotStart, headSlotStart);
    }

    /**
     * Truncates {@code timestampMillis} down to the start of its granularity-aligned slot.
     * <p>
     * Example: granularity 1h, {@code timestamp=10:37} -> {@code 10:00}.
     */
    long slotStartForTimestamp(long timestampMillis) {
        return Math.floorDiv(timestampMillis, granularityMillis) * granularityMillis;
    }
}
