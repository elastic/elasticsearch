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
 * Tracks counts in fixed-duration time slots using a ring buffer of buckets indexed by slot time. Slots are
 * defined by a time granularity and a max number of slots. E.g., 10:37 would be in slot 10:00 for 1h slots.
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
 * Bucket sums are updated atomically; ring structure ({@code tailSlotStart}, {@code tailBucketIndex},
 * rollover/eviction) is guarded by {@link #structureLock}. Concurrent {@link #add} and
 * {@link #remove} on existing slots use the structure read lock plus atomic bucket updates.
 * <p>
 * Buckets roll forward lazily on {@link #add}, {@link #remove}, and {@link #sum}. When time advances,
 * the oldest bucket is merged into its neighbor, then {@code tailBucketIndex} rotates and {@code tailSlotStart}
 * advances so the new head slot is empty without copying the array. Overflow mass older than retention
 * remains in the tail slot.
 */
public class TimeSlottedCounter {

    private static final Logger logger = LogManager.getLogger(TimeSlottedCounter.class);

    private final long granularityMillis;
    private final LongSupplier timeProvider;
    private final ReadWriteLock structureLock = new ReentrantReadWriteLock();
    private final List<TimeSlottedCounterWindow> registeredWindows = new ArrayList<>();

    private final AtomicLongArray counts;

    /** {@code slotStart} of the oldest retained (tail) bucket. */
    private long tailSlotStart;

    /** Physical index of the tail bucket in {@link #counts}. */
    private int tailBucketIndex;

    /**
     * Creates a ring of {@code maxBuckets} atomic count buckets, one per time slot of {@code granularity}.
     * <p>
     * Example ({@code maxBuckets=4}, 1h granularity, clock at 12:00):
     * <pre>
     *   tail slot 09:00-09:59, tailBucketIndex=0; head slot 12:00-12:59 at offset 3
     *   +--------------+--------------+--------------+--------------+
     *   | physical 0   | physical 1   | physical 2   | physical 3   |
     *   | tail         |              |              | head         |
     *   +--------------+--------------+--------------+--------------+
     *   | offset 0     | offset 1     | offset 2     | offset 3     |
     *   +--------------+--------------+--------------+--------------+
     *   | 09:00-09:59  | 10:00-10:59  | 11:00-11:59  | 12:00-12:59  |
     *   +--------------+--------------+--------------+--------------+
     *   | count [0]    | count [0]    | count [0]    | count [0]    |
     *   +--------------+--------------+--------------+--------------+
     * </pre>
     *
     * @param granularity duration of each time slot
     * @param maxBuckets maximum number of slots retained; oldest slot absorbs overflow when evicted
     * @param timeProvider source of wall-clock time in milliseconds
     */
    public TimeSlottedCounter(TimeValue granularity, int maxBuckets, LongSupplier timeProvider) {
        if (granularity.millis() <= 0) {
            throw new IllegalArgumentException("granularity must be positive");
        }
        if (maxBuckets <= 0) {
            throw new IllegalArgumentException("maxBuckets must be positive");
        }
        this.granularityMillis = granularity.millis();
        this.timeProvider = timeProvider;
        this.counts = new AtomicLongArray(maxBuckets);
        long headSlot = slotStartForTimestamp(currentTimeMillis());
        this.tailSlotStart = headSlot - (long) (maxBuckets - 1) * granularityMillis;
        this.tailBucketIndex = 0;
        assert maxBuckets() >= 2 : "max buckets must be at least 2 but was " + maxBuckets();
    }

    public static TimeSlottedCounter createFromSettings(Settings settings, LongSupplier timeProvider) {
        return new TimeSlottedCounter(
            SharedBlobCacheService.SHARED_CACHE_TIME_SLOTS_GRANULARITY_SETTING.get(settings),
            SharedBlobCacheService.SHARED_CACHE_TIME_SLOTS_COUNT_SETTING.get(settings),
            timeProvider
        );
    }

    public TimeValue granularity() {
        return TimeValue.timeValueMillis(granularityMillis);
    }

    long granularityMillis() {
        return granularityMillis;
    }

    public int maxBuckets() {
        return counts.length();
    }

    /**
     * Registers a fixed relative window {@code [now - endOffsetMillis, now - startOffsetMillis)} whose sum
     * is maintained incrementally by this counter.
     * <p>
     * Example at {@code now=10d}, {@code registerRelativeWindow(1d, 3d)}:
     * <pre>
     *   window = [now-3d, now-1d)  — counts in slots overlapping that interval
     *
     *   |---- retained buckets ----|.... now ....|
     *                    ^windowStart    ^windowEnd
     * </pre>
     * The returned {@link TimeSlottedCounterWindow} is updated on every {@link #add}/{@link #remove} and ring roll.
     */
    public TimeSlottedCounterWindow registerRelativeWindow(long startOffsetMillis, long endOffsetMillis) {
        TimeSlottedCounterWindow window = new TimeSlottedCounterWindow(this, startOffsetMillis, endOffsetMillis);
        structureLock.writeLock().lock();
        try {
            long now = currentTimeMillis();
            advanceToNowUnderWriteLock(now);
            window.initializeUnderWriteLock(now);
            registeredWindows.add(window);
        } finally {
            structureLock.writeLock().unlock();
        }
        return window;
    }

    /**
     * Adds {@code count} to the slot containing {@code timestampMillis}.
     * <p>
     * Delegates to {@link #mutateSlot}; may roll the ring forward first if wall clock has moved past the head slot.
     * Timestamps older than the retained tail are clamped to the tail slot (see {@link #resolveSlotStart}).
     */
    public void add(long timestampMillis, long count) {
        if (count <= 0) {
            return;
        }
        mutateSlot(timestampMillis, (index, slotStart) -> {
            counts.addAndGet(index, count);
            return count;
        });
    }

    /**
     * Removes {@code count} from the slot containing {@code timestampMillis}.
     * <p>
     * Same slot resolution as {@link #add}; the bucket is never driven below zero (excess remove is clamped).
     */
    public void remove(long timestampMillis, long count) {
        if (count <= 0) {
            return;
        }
        mutateSlot(timestampMillis, (index, slotStart) -> {
            long previous = counts.getAndAccumulate(index, count, (current, removal) -> current < removal ? 0 : current - removal);
            long next = previous < count ? 0 : previous - count;
            if (previous < count) {
                logger.debug("remove clamped: slot start [{}] count [{}] less than remove count [{}]", slotStart, previous, count);
            }
            return next - previous;
        });
    }

    /**
     * Rolls buckets forward to the current time. Exposed for tests.
     * <p>
     * Equivalent to {@link #advanceIfNeededForWindow} with no window: repeatedly calls
     * {@link #rollForwardOneSlot} until the head slot matches the current time slot.
     */
    public void advanceToNow() {
        advanceIfNeededForWindow(null);
    }

    /**
     * Advances ring structure and the given window when required.
     * <p>
     * Fast path (structure read lock only): return immediately when the head slot is already current
     * <em>and</em> the window's {@code [start, end)} boundaries have not crossed a slot boundary since the last slide.
     * <p>
     * Slow path: take the write lock, re-read {@code now}, re-check, then {@link #advanceToNowUnderWriteLock(long)}
     * (ring roll + window slide) using that fresh timestamp.
     * <pre>
     *   read lock: needsAdvance? / window.needsTimeSlide?
     *        | no                          | yes
     *        v                             v
     *     return                    write lock + roll/slide
     * </pre>
     */
    void advanceIfNeededForWindow(TimeSlottedCounterWindow window) {
        long now = currentTimeMillis();
        structureLock.readLock().lock();
        try {
            if (needsAdvanceToNow(now) == false && (window == null || window.needsTimeSlide(now) == false)) {
                return;
            }
        } finally {
            structureLock.readLock().unlock();
        }
        structureLock.writeLock().lock();
        try {
            now = currentTimeMillis();
            if (needsAdvanceToNow(now) == false && (window == null || window.needsTimeSlide(now) == false)) {
                if (window != null) {
                    window.touchMetadataIfNewer(now);
                }
                return;
            }
            advanceToNowUnderWriteLock(now);
        } finally {
            structureLock.writeLock().unlock();
        }
    }

    /**
     * Returns the sum of counts in slots overlapping {@code [windowStartMillis, windowEndMillis)}.
     * <p>
     * Example (1h slots, range {@code [10:00, 13:00)}):
     * <pre>
     *   includes slots 10:00, 11:00, 12:00 (each [slotStart, slotStart+granularity) intersects the range)
     *   excludes slot 13:00 (starts at range end)
     * </pre>
     * Advances the ring to now before summing.
     */
    public long sum(long windowStartMillis, long windowEndMillis) {
        if (windowEndMillis <= windowStartMillis) {
            return 0;
        }
        return sumInRange(windowStartMillis, windowEndMillis);
    }

    long currentTimeMillis() {
        long now = timeProvider.getAsLong();
        return now < 0 ? 0 : now;
    }

    /**
     * Sum over {@code [windowStartMillis, windowEndMillis)} after advancing to the current time.
     * <p>
     * Takes the write lock to roll forward, then sums under the read lock via {@link #sumBucketsInRange}.
     */
    long sumInRange(long windowStartMillis, long windowEndMillis) {
        structureLock.writeLock().lock();
        try {
            advanceToNowUnderWriteLock(currentTimeMillis());
        } finally {
            structureLock.writeLock().unlock();
        }
        structureLock.readLock().lock();
        try {
            return sumBucketsInRange(windowStartMillis, windowEndMillis);
        } finally {
            structureLock.readLock().unlock();
        }
    }

    /**
     * Sum over {@code [windowStartMillis, windowEndMillis)}. Caller must hold {@link #structureLock} read or write lock.
     * <p>
     * Iterates only bucket offsets whose slot interval overlaps the range.
     */
    long sumBucketsInRange(long windowStartMillis, long windowEndMillis) {
        if (windowEndMillis <= windowStartMillis
            || windowEndMillis <= tailSlotStart
            || windowStartMillis >= tailSlotStart + (long) counts.length() * granularityMillis) {
            return 0;
        }
        long firstOffset = Math.max(0L, Math.floorDiv(windowStartMillis - tailSlotStart - granularityMillis, granularityMillis) + 1);
        long lastOffset = Math.min(counts.length() - 1L, Math.floorDiv(windowEndMillis - 1 - tailSlotStart, granularityMillis));
        if (firstOffset > lastOffset) {
            return 0;
        }
        long total = 0;
        for (int offsetFromTail = (int) firstOffset; offsetFromTail <= lastOffset; offsetFromTail++) {
            total += counts.get(bucketIndexForOffset(offsetFromTail));
        }
        return total;
    }

    @FunctionalInterface
    private interface SlotMutation {
        /**
         * @return the count delta applied to the bucket (positive for add, negative for remove)
         */
        long apply(int index, long slotStart);
    }

    /**
     * Resolves {@code timestampMillis} to a bucket and applies {@code mutation}.
     * <p>
     * Example ({@code maxBuckets=4}, 1h granularity, tail 09:00-09:59, {@code tailBucketIndex=3}):
     * <pre>
     *   +--------------+--------------+--------------+--------------+
     *   | physical 0   | physical 1   | physical 2   | physical 3   |
     *   |              |              | head         | tail         |
     *   +--------------+--------------+--------------+--------------+
     *   | offset 1     | offset 2     | offset 3     | offset 0     |
     *   +--------------+--------------+--------------+--------------+
     *   | 10:00-10:59  | 11:00-11:59  | 12:00-12:59  | 09:00-09:59  |
     *   +--------------+--------------+--------------+--------------+
     *   | count [0]    | count [7]    | count [3]    | count [5]    |
     *   +--------------+--------------+--------------+--------------+
     *
     *   add(timestamp=10:30, count=4):
     *     1. resolveSlotStart(10:30) -> 10:00 (floor to hour)
     *     2. offsetFromTail = (10:00 - 09:00) / 1h = 1
     *     3. index = bucketIndexForOffset(1) = floorMod(3+1, 4) = 0
     *     4. counts.addAndGet(0, 4) -> counts[0] becomes 4
     *     5. notify windows overlapping slot 10:00-10:59
     * </pre>
     * If the ring is stale ({@link #needsAdvanceToNow(long)}), loops: release read lock, take write lock and
     * {@link #advanceToNowUnderWriteLock}, then retry. {@link #resolveSlotStart} clamps timestamps to the retained
     * range, so the resolved slot always maps to a bucket.
     * <p>
     * Wall clock is read once per attempt under the read lock and passed to {@link #needsAdvanceToNow(long)} and
     * {@link #resolveSlotStart(long, long)} so a slot-boundary crossing cannot leave a stale head while attributing
     * counts to an older bucket.
     */
    private void mutateSlot(long timestampMillis, SlotMutation mutation) {
        while (true) {
            structureLock.readLock().lock();
            try {
                long now = currentTimeMillis();
                if (needsAdvanceToNow(now) == false) {
                    long slotStart = resolveSlotStart(timestampMillis, now);
                    int index = findSlotIndex(slotStart);
                    long delta = mutation.apply(index, slotStart);
                    if (delta != 0) {
                        notifyRegisteredWindowsSlotDelta(slotStart, delta, now);
                    }
                    return;
                }
            } finally {
                structureLock.readLock().unlock();
            }
            structureLock.writeLock().lock();
            try {
                advanceToNowUnderWriteLock(currentTimeMillis());
            } finally {
                structureLock.writeLock().unlock();
            }
        }
    }

    /**
     * Propagates a bucket change to registered windows whose interval overlaps {@code slotStart}.
     */
    private void notifyRegisteredWindowsSlotDelta(long slotStart, long delta, long now) {
        if (registeredWindows.isEmpty()) {
            return;
        }
        long slotEnd = slotStart + granularityMillis;
        for (TimeSlottedCounterWindow window : registeredWindows) {
            if (window.mayOverlapSlot(slotStart, slotEnd, now)) {
                window.onSlotDelta(slotStart, delta, now);
            }
        }
    }

    /**
     * Whether {@link #advanceToNowUnderWriteLock(long)} must roll the ring forward. Caller must hold structure lock.
     * <p>
     * True when the current time slot is newer than the head slot (wall clock moved into a future bucket).
     */
    private boolean needsAdvanceToNow(long now) {
        return headSlotStart() < slotStartForTimestamp(now);
    }

    /**
     * Rolls the ring until the head slot equals the current time slot, then updates registered windows.
     * Caller must hold {@link #structureLock} write lock and pass a {@code now} read while holding it.
     * <p>
     * The expectation is that this will be (indirectly) called frequently enough that the ring is rolled
     * by one slot (or a small amount of slots). We do not expect a situation that a lot of time has passed
     * where this function will need to block for a long time to roll the ring by a high number of slots.
     */
    private void advanceToNowUnderWriteLock(long now) {
        long targetHeadSlot = slotStartForTimestamp(now);
        while (headSlotStart() < targetHeadSlot) {
            rollForwardOneSlot(now);
        }
        for (TimeSlottedCounterWindow window : registeredWindows) {
            if (window.needsTimeSlide(now)) {
                window.advanceTimeSlideUnderWriteLock(now);
            } else {
                window.touchMetadataIfNewer(now);
            }
        }
    }

    /**
     * Maps a timestamp to the {@code slotStart} used for bucket lookup, clamped to retained range.
     * <p>
     * <pre>
     *   +---------------------------+----------------------------------------+
     *   | condition                 | resolved slotStart                     |
     *   +---------------------------+----------------------------------------+
     *   | timestamp too old         | tail slot (overflow mass lives here)   |
     *   | timestamp in future       | current wall-clock slot ({@code now})  |
     *   | otherwise                 | floor(timestamp) to granularity        |
     *   +---------------------------+----------------------------------------+
     * </pre>
     * Caller must hold structure lock and pass a {@code now} read while holding it (same value as
     * {@link #needsAdvanceToNow(long)} for that attempt).
     */
    private long resolveSlotStart(long timestampMillis, long now) {
        long slot = slotStartForTimestamp(Math.max(0, timestampMillis));
        if (slot < tailSlotStart) {
            return tailSlotStart;
        }
        long currentSlot = slotStartForTimestamp(now);
        if (slot > currentSlot) {
            return currentSlot;
        }
        return slot;
    }

    /**
     * Truncates {@code timestampMillis} down to the start of its granularity-aligned slot.
     * <p>
     * Example: granularity 1h, {@code timestamp=10:37} -> {@code 10:00}.
     */
    long slotStartForTimestamp(long timestampMillis) {
        return Math.floorDiv(timestampMillis, granularityMillis) * granularityMillis;
    }

    /**
     * Whether the half-open slot {@code [slotStart, slotStart + granularity)} intersects
     * {@code [rangeStart, rangeEnd)}.
     * <p>
     * Example: slot {@code [10:00, 11:00)}, range {@code [10:30, 12:00)} -> true (shared 10:30–11:00).
     */
    boolean slotOverlaps(long slotStart, long rangeStart, long rangeEnd) {
        if (rangeEnd <= rangeStart) {
            return false;
        }
        return slotStart < rangeEnd && slotStart + granularityMillis > rangeStart;
    }

    /**
     * Advances the ring by one time slot: evict tail into its neighbor, rotate {@code tailBucketIndex},
     * advance {@code tailSlotStart}, clear new head.
     * <p>
     * Example ({@code maxBuckets=3}, before roll; new head slot will be 13:00-13:59):
     * <pre>
     *   Before:
     *   +--------------+--------------+--------------+
     *   | physical 0   | physical 1   | physical 2   |
     *   |              | head         | tail         |
     *   +--------------+--------------+--------------+
     *   | offset 1     | offset 2     | offset 0     |
     *   +--------------+--------------+--------------+
     *   | 11:00-11:59  | 12:00-12:59  | 10:00-10:59  |
     *   +--------------+--------------+--------------+
     *   | count [5]    | count [10]   | count [99]   |
     *   +--------------+--------------+--------------+
     *   tail [99] at offset 0 (physical 2) holds overflow from evicted slots
     *
     *   Step 1 mergeTailIntoNeighbor: counts[0] += 99, counts[2] = 0
     *   Step 2 tailBucketIndex = (2+1)%3 = 0, tailSlotStart += 1h (10:00 -> 11:00)
     *   Step 3 zero physical cell for new head (offset 2), head slot 13:00-13:59
     *
     *   After:
     *   +--------------+--------------+--------------+
     *   | physical 0   | physical 1   | physical 2   |
     *   |              |              | head         |
     *   +--------------+--------------+--------------+
     *   | offset 0     | offset 1     | offset 2     |
     *   +--------------+--------------+--------------+
     *   | 11:00-11:59  | 12:00-12:59  | 13:00-13:59  |
     *   +--------------+--------------+--------------+
     *   | count [104]  | count [10]   | count [0]    |
     *   +--------------+--------------+--------------+
     *   former tail mass [99] merged into physical 0 (neighbor of tail)
     * </pre>
     */
    private void rollForwardOneSlot(long now) {
        if (counts.length() >= 2) {
            mergeTailIntoNeighbor(bucketIndexForOffset(0), bucketIndexForOffset(1), now);
        }
        tailBucketIndex = (tailBucketIndex + 1) % counts.length();
        tailSlotStart += granularityMillis;
        counts.set(bucketIndexForOffset(counts.length() - 1), 0);
    }

    /**
     * Moves all count from the tail bucket into the next-older neighbor before the tail slot is reused.
     * <p>
     * Overflow from slots evicted off the left of the ring accumulates in the tail; merging preserves that mass
     * in the oldest retained bucket. Notifies registered windows via
     * {@link TimeSlottedCounterWindow#onTailMergeUnderWriteLock} when tail/neighbor overlap with the window differs.
     */
    private void mergeTailIntoNeighbor(int tailIndex, int neighborIndex, long now) {
        long tailCount = counts.get(tailIndex);
        if (tailCount != 0 && registeredWindows.isEmpty() == false) {
            long neighborSlotStart = slotStartForOffset(1);
            long tailSlotEnd = tailSlotStart + granularityMillis;
            long neighborSlotEnd = neighborSlotStart + granularityMillis;
            for (TimeSlottedCounterWindow window : registeredWindows) {
                if (window.mayOverlapSlot(tailSlotStart, tailSlotEnd, now)
                    || window.mayOverlapSlot(neighborSlotStart, neighborSlotEnd, now)) {
                    window.onTailMergeUnderWriteLock(tailSlotStart, neighborSlotStart, tailCount, now);
                }
            }
        }
        counts.addAndGet(neighborIndex, counts.getAndSet(tailIndex, 0));
    }

    /**
     * Returns the physical {@link #counts} index for {@code slotStart}, or {@code -1} if the slot is not retained.
     * <p>
     * Uses {@code offsetFromTail = floorDiv(slotStart - tailSlotStart, granularityMillis)} then
     * {@link #bucketIndexForOffset(int)}. See {@link #mutateSlot}.
     */
    private int findSlotIndex(long slotStart) {
        long offsetFromTail = Math.floorDiv(slotStart - tailSlotStart, granularityMillis);
        if (offsetFromTail < 0 || offsetFromTail >= counts.length()) {
            return -1;
        }
        return bucketIndexForOffset((int) offsetFromTail);
    }

    /**
     * {@code slotStart} of the newest retained bucket ({@code offsetFromTail=maxBuckets-1}).
     */
    private long headSlotStart() {
        return tailSlotStart + (long) (counts.length() - 1) * granularityMillis;
    }

    /**
     * Wall-clock start of the bucket at {@code offsetFromTail} (0 = tail, {@code maxBuckets-1} = head).
     */
    private long slotStartForOffset(int offsetFromTail) {
        return tailSlotStart + (long) offsetFromTail * granularityMillis;
    }

    /**
     * Maps a logical offset from tail to a physical index in {@link #counts}.
     * <p>
     * Example ({@code maxBuckets=4}, {@code tailBucketIndex=3}, head slot 12:00-12:59):
     * <pre>
     *   +--------------+--------------+--------------+--------------+
     *   | physical 0   | physical 1   | physical 2   | physical 3   |
     *   |              |              | head         | tail         |
     *   +--------------+--------------+--------------+--------------+
     *   | offset 1     | offset 2     | offset 3     | offset 0     |
     *   +--------------+--------------+--------------+--------------+
     *   | 10:00-10:59  | 11:00-11:59  | 12:00-12:59  | 09:00-09:59  |
     *   +--------------+--------------+--------------+--------------+
     *   floorMod(tailBucketIndex + offsetFromTail, 4)
     * </pre>
     */
    private int bucketIndexForOffset(int offsetFromTail) {
        return Math.floorMod(tailBucketIndex + offsetFromTail, counts.length());
    }
}
