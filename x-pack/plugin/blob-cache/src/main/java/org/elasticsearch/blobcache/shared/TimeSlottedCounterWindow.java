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
 * incrementally maintains the sum on slot mutations and window time slides; {@link #sum()}
 * advances time if needed and returns the cached total under the counter read lock.
 */
public class TimeSlottedCounterWindow {

    private final TimeSlottedCounter counter;
    /**
     * Exclusive window end: slots back from the sliding anchor at registration time. Smaller values are closer to
     * {@code now}.
     */
    private final int exclusiveEndSlotsBackFromAnchor;
    /**
     * Inclusive window start: slots back from the sliding anchor at registration time. Larger values reach farther
     * into the past.
     */
    private final int inclusiveStartSlotsBackFromAnchor;
    private final AtomicLong sum = new AtomicLong();

    private long lastNow = -1;
    /** Inclusive window start as an offset from the tail slot. */
    private int lastWindowStartOffsetFromTail = -1;
    /** Exclusive window end as an offset from the tail slot. */
    private int lastWindowEndOffsetFromTail = -1;

    /**
     * Package-private constructor; use {@link TimeSlottedCounter#registerRelativeWindow}.
     * <p>
     * Defines a sliding interval over {@code inclusiveStartSlotsBackFromAnchor - exclusiveEndSlotsBackFromAnchor}
     * whole slots ending {@code exclusiveEndSlotsBackFromAnchor} slots before the current anchor. For example,
     * {@code exclusiveEndSlotsBackFromAnchor=24, inclusiveStartSlotsBackFromAnchor=72} with 1h slots at
     * {@code now=10d} tracks {@code [7d, 9d)}.
     */
    TimeSlottedCounterWindow(TimeSlottedCounter counter, int exclusiveEndSlotsBackFromAnchor, int inclusiveStartSlotsBackFromAnchor) {
        this.counter = counter;
        this.exclusiveEndSlotsBackFromAnchor = exclusiveEndSlotsBackFromAnchor;
        this.inclusiveStartSlotsBackFromAnchor = inclusiveStartSlotsBackFromAnchor;
    }

    /**
     * Returns the incrementally maintained sum after advancing the counter to the current time.
     */
    public long sum() {
        return counter.readRegisteredWindowSum(this);
    }

    long cachedSum() {
        return sum.get();
    }

    /**
     * Whether this window needs a metadata touch or time slide for {@code now}. Caller must hold
     * {@link TimeSlottedCounter}'s read lock.
     */
    boolean needsUpdate(long now) {
        if (lastNow < 0) {
            return true;
        }
        return counter.effectiveAnchorSlotStart(now) != counter.effectiveAnchorSlotStart(lastNow);
    }

    /**
     * Full scan for tests and validation. Recomputes the bucket sum for the current window;
     * does not read the cached {@link #sum}.
     */
    long sumUncached() {
        long now = counter.currentTimeMillis();
        long anchorSlot = counter.effectiveAnchorSlotStart(now);
        return counter.sum(
            anchorSlot - (long) inclusiveStartSlotsBackFromAnchor * counter.granularityMillis(),
            anchorSlot - (long) exclusiveEndSlotsBackFromAnchor * counter.granularityMillis()
        );
    }

    /**
     * Fast reject before applying a slot delta.
     */
    boolean containsSlot(long slotStart) {
        int slotOffset = counter.offsetFromTailForSlotStart(slotStart);
        return slotOffset >= lastWindowStartOffsetFromTail && slotOffset < lastWindowEndOffsetFromTail;
    }

    /**
     * Adjusts the cached sum when a bucket overlapping the current window changes.
     */
    void onSlotDelta(long delta) {
        sum.addAndGet(delta);
    }

    /**
     * Initializes or slides the cached sum when time advances. Caller must hold the counter write lock.
     */
    void advanceUnderWriteLock(long now) {
        if (now <= lastNow) {
            return;
        }
        int anchorOffset = counter.anchorOffsetForNow(now);
        int windowStart = anchorOffset - inclusiveStartSlotsBackFromAnchor;
        int windowEnd = anchorOffset - exclusiveEndSlotsBackFromAnchor;
        if (lastWindowStartOffsetFromTail < 0) {
            sum.set(counter.sumBucketsInOffsetRange(windowStart, windowEnd));
        } else {
            if (windowStart > lastWindowStartOffsetFromTail) {
                sum.addAndGet(-counter.sumBucketsInOffsetRange(lastWindowStartOffsetFromTail, windowStart));
            }
            if (windowEnd > lastWindowEndOffsetFromTail) {
                sum.addAndGet(counter.sumBucketsInOffsetRange(lastWindowEndOffsetFromTail, windowEnd));
            }
        }
        updateMetadata(now, windowStart, windowEnd);
    }

    private void updateMetadata(long now, int windowStart, int windowEnd) {
        lastNow = now;
        lastWindowStartOffsetFromTail = windowStart;
        lastWindowEndOffsetFromTail = windowEnd;
    }
}
