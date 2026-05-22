/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache.shared;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A fixed relative-to-now window registered with a {@link TimeSlottedCounter}. The counter
 * incrementally maintains the sum on slot mutations and structural updates; {@link #sum()}
 * only advances time and returns the cached total.
 */
public class TimeSlottedCounterWindow {

    private final TimeSlottedCounter counter;
    private final long startOffsetMillis;
    private final long endOffsetMillis;
    private final AtomicLong sum = new AtomicLong();

    private long lastNow = -1;
    private long lastWindowStartMillis;
    private long lastWindowEndMillis;

    /**
     * Package-private constructor; use {@link TimeSlottedCounter#registerRelativeWindow}.
     * <p>
     * Defines a sliding interval {@code [now - endOffsetMillis, now - startOffsetMillis)}. For example,
     * {@code startOffset=1d, endOffset=3d} at {@code now=10d} tracks {@code [7d, 9d)}.
     */
    TimeSlottedCounterWindow(TimeSlottedCounter counter, long startOffsetMillis, long endOffsetMillis) {
        if (startOffsetMillis < 0 || endOffsetMillis < 0) {
            throw new IllegalArgumentException("offsets must be non-negative");
        }
        if (endOffsetMillis <= startOffsetMillis) {
            throw new IllegalArgumentException("endOffsetMillis must be greater than startOffsetMillis");
        }
        this.counter = counter;
        this.startOffsetMillis = startOffsetMillis;
        this.endOffsetMillis = endOffsetMillis;
    }

    /**
     * Initializes the cached sum from a full bucket scan. Caller must hold {@link TimeSlottedCounter}'s write lock.
     * <p>
     * Example at registration time {@code now=10d}, window {@code [now-3d, now-1d)}:
     * <pre>
     *   sum = counter.sumBucketsInRange(7d, 9d)   // one-time full scan
     *   lastWindowStart/End stored for later incremental slides
     * </pre>
     */
    void initializeUnderWriteLock(long now) {
        long windowStart = windowStart(now);
        long windowEnd = windowEnd(now);
        sum.set(counter.sumBucketsInRange(windowStart, windowEnd));
        updateMetadata(now, windowStart, windowEnd);
    }

    /**
     * Returns the incrementally maintained sum after advancing the counter to the current time.
     * <p>
     * May trigger a ring roll and/or {@link #advanceTimeSlideUnderWriteLock} when time or slot boundaries moved;
     * otherwise returns the cached {@link #sum} with no bucket scan.
     */
    public long sum() {
        counter.advanceIfNeededForWindow(this);
        return sum.get();
    }

    /**
     * Whether {@link #advanceTimeSlideUnderWriteLock(long)} must run for {@code now}. Caller must hold
     * {@link TimeSlottedCounter}'s structure read lock.
     * <p>
     * True when {@code now} advanced and either window boundary crossed into a new time slot:
     * <pre>
     *   last window [7d, 9d) at now=10d
     *   clock -> 10d + 2h still same slot boundaries -> false (fast path in advanceIfNeededForWindow)
     *   clock -> 10d + 2h such that windowEnd enters next hour slot -> true
     * </pre>
     */
    boolean needsTimeSlide(long now) {
        return lastNow < 0 || (now > lastNow && windowBoundariesSlotChanged(now));
    }

    /**
     * Full scan for tests and validation. Recomputes {@code sumBucketsInRange} for the current window;
     * does not read the cached {@link #sum}.
     */
    long sumUncached() {
        long now = counter.currentTimeMillis();
        return counter.sumInRange(windowStart(now), windowEnd(now));
    }

    /**
     * Fast reject before applying a slot delta. Caller must pass the same {@code now} used for the mutation.
     */
    boolean mayOverlapSlot(long slotStart, long slotEnd, long now) {
        long windowStart = windowStart(now);
        long windowEnd = windowEnd(now);
        return slotEnd > windowStart && slotStart < windowEnd;
    }

    /**
     * Adjusts the cached sum when a bucket overlapping the current window changes.
     * Called only after {@link #mayOverlapSlot(long, long, long)} returns true.
     */
    void onSlotDelta(long slotStart, long delta, long now) {
        if (delta != 0) {
            sum.addAndGet(delta);
        }
    }

    /**
     * Adjusts the cached sum when tail mass is merged into the neighbor slot. Caller must hold the counter write lock.
     * <p>
     * During ring roll, overflow in the tail bucket moves to the neighbor. If exactly one of the two slots overlaps
     * the window, the cached sum must change by {@code ±tailCount}:
     * <pre>
     *   tail in window, neighbor not  -> sum -= tailCount  (mass left the window's slot representation)
     *   neighbor in window, tail not -> sum += tailCount
     *   both in or both out          -> no change (mass stayed inside or outside the window)
     * </pre>
     */
    void onTailMergeUnderWriteLock(long tailSlotStart, long neighborSlotStart, long tailCount, long now) {
        if (tailCount == 0) {
            return;
        }
        long windowStart = windowStart(now);
        long windowEnd = windowEnd(now);
        boolean tailInWindow = counter.slotOverlaps(tailSlotStart, windowStart, windowEnd);
        boolean neighborInWindow = counter.slotOverlaps(neighborSlotStart, windowStart, windowEnd);
        if (tailInWindow != neighborInWindow) {
            sum.addAndGet(tailInWindow ? -tailCount : tailCount);
        }
    }

    /**
     * Slides the cached sum when time advances across slot boundaries. Caller must hold the counter write lock.
     * <p>
     * The relative window moves with {@code now}; when a boundary enters a new bucket, drop or add whole slots:
     * <pre>
     *   was [7d, 9d), now [7d+2h, 9d+2h) — start boundary crossed a slot:
     *     sum -= sumBucketsInRange(oldStart, newStart)   // slots that fell out of the left edge
     *   end boundary crossed a slot:
     *     sum += sumBucketsInRange(oldEnd, newEnd)       // slots that entered on the right
     * </pre>
     * If neither boundary changed slot alignment, only metadata is updated.
     */
    void advanceTimeSlideUnderWriteLock(long now) {
        if (now <= lastNow) {
            return;
        }
        long windowStart = windowStart(now);
        long windowEnd = windowEnd(now);
        if (lastNow >= 0 && windowBoundariesSlotChanged(now)) {
            if (windowStart > lastWindowStartMillis) {
                sum.addAndGet(-counter.sumBucketsInRange(lastWindowStartMillis, windowStart));
            }
            if (windowEnd > lastWindowEndMillis) {
                sum.addAndGet(counter.sumBucketsInRange(lastWindowEndMillis, windowEnd));
            }
        }
        updateMetadata(now, windowStart, windowEnd);
    }

    /**
     * Updates cached window boundaries when time moved but slot-aligned boundaries did not. Caller must hold write lock.
     */
    void touchMetadataIfNewer(long now) {
        if (now > lastNow) {
            updateMetadata(now, windowStart(now), windowEnd(now));
        }
    }

    private boolean windowBoundariesSlotChanged(long now) {
        return counter.slotStartForTimestamp(windowStart(now)) != counter.slotStartForTimestamp(lastWindowStartMillis)
            || counter.slotStartForTimestamp(windowEnd(now)) != counter.slotStartForTimestamp(lastWindowEndMillis);
    }

    private long windowStart(long now) {
        return Math.max(0, now - endOffsetMillis);
    }

    /**
     * Exclusive end of the half-open window at {@code now}.
     */
    private long windowEnd(long now) {
        return now - startOffsetMillis;
    }

    /**
     * Records {@code now} and the last window boundaries used for {@link #needsTimeSlide} and incremental slides.
     */
    private void updateMetadata(long now, long windowStart, long windowEnd) {
        lastNow = now;
        lastWindowStartMillis = windowStart;
        lastWindowEndMillis = windowEnd;
    }
}
