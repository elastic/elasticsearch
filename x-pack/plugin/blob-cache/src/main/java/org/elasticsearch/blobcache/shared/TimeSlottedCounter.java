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

import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.LongSupplier;

/**
 * Tracks counts in fixed-duration time slots using a ring buffer of buckets indexed by slot time. Slots are
 * defined by a time granularity and a max number of slots. E.g., 10:37 would be in slot 10:00 for 1h slots.
 * <p>
 * Used at node level for cache boost quota tracking (expected totals from shard data, and actual
 * totals from cache residency). Callers pass arbitrary counts via {@link #add} and {@link #remove};
 * this type is agnostic to what is being counted (the expectation is that it is cache regions).
 * <p>
 * The ring is fixed at construction: {@code tailSlotStart} does not move. Timestamps after the head slot are
 * clamped to the head bucket; timestamps before the tail slot are clamped to the tail bucket.
 * <p>
 * Bucket sums are updated atomically via {@link AtomicLongArray}.
 */
public class TimeSlottedCounter {

    private static final Logger logger = LogManager.getLogger(TimeSlottedCounter.class);

    private final long granularityMillis;

    /** {@code slotStart} of the oldest retained (tail) bucket. */
    private final long tailSlotStart;

    private final AtomicLongArray counts;

    /**
     * Creates a ring of {@code maxBuckets} atomic count buckets, one per time slot of {@code granularity}.
     * <p>
     * Example ({@code maxBuckets=4}, 1h granularity, clock at 12:00):
     * <pre>
     *   tail slot 09:00-09:59 at offset 0; head slot 12:00-12:59 at offset 3
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
     * @param maxBuckets maximum number of slots retained
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
        this.counts = new AtomicLongArray(maxBuckets);
        long now = timeProvider.getAsLong();
        long headSlot = slotStartForTimestamp(now < 0 ? 0 : now);
        this.tailSlotStart = headSlot - (long) (maxBuckets - 1) * granularityMillis;
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

    public int maxBuckets() {
        return counts.length();
    }

    /**
     * Adds {@code count} to the slot containing {@code timestampMillis}.
     * <p>
     * Timestamps older than the retained tail are clamped to the tail slot; timestamps after the head
     * slot are clamped to the head slot.
     */
    public void add(long timestampMillis, long count) {
        if (count <= 0) {
            return;
        }
        counts.addAndGet(bucketIndex(timestampMillis), count);
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
        int index = bucketIndex(timestampMillis);
        long slotStart = tailSlotStart + (long) index * granularityMillis;
        counts.accumulateAndGet(index, count, (current, removal) -> {
            if (current < removal) {
                logger.debug("remove clamped: slot start [{}] count [{}] less than remove count [{}]", slotStart, current, removal);
                return 0;
            }
            return current - removal;
        });
    }

    /**
     * Returns the sum of counts in slots overlapping {@code [windowStartMillis, windowEndMillis)}.
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
        long retentionEnd = tailSlotStart + (long) counts.length() * granularityMillis;
        if (windowEndMillis <= tailSlotStart || windowStartMillis >= retentionEnd) {
            return 0;
        }
        long firstOffset = Math.max(0L, Math.floorDiv(windowStartMillis - tailSlotStart - granularityMillis, granularityMillis) + 1);
        long lastOffset = Math.min(counts.length() - 1L, Math.floorDiv(windowEndMillis - 1 - tailSlotStart, granularityMillis));
        if (firstOffset > lastOffset) {
            return 0;
        }
        long total = 0;
        for (int offset = (int) firstOffset; offset <= lastOffset; offset++) {
            total += counts.get(offset);
        }
        return total;
    }

    /**
     * Resolves {@code timestampMillis} to a physical bucket index.
     * <p>
     * Example ({@code maxBuckets=4}, 1h granularity, tail 09:00-09:59):
     * <pre>
     *   +--------------+--------------+--------------+--------------+
     *   | physical 0   | physical 1   | physical 2   | physical 3   |
     *   | tail         |              |              | head         |
     *   +--------------+--------------+--------------+--------------+
     *   | offset 0     | offset 1     | offset 2     | offset 3     |
     *   +--------------+--------------+--------------+--------------+
     *   | 09:00-09:59  | 10:00-10:59  | 11:00-11:59  | 12:00-12:59  |
     *   +--------------+--------------+--------------+--------------+
     *
     *   add(timestamp=10:30, count=4):
     *     1. resolve slot start -> 10:00 (floor to hour)
     *     2. offsetFromTail = (10:00 - 09:00) / 1h = 1
     *     3. counts.addAndGet(1, 4)
     * </pre>
     * Slot start clamping:
     * <pre>
     *   +---------------------------+----------------------------------------+
     *   | condition                 | resolved slotStart                     |
     *   +---------------------------+----------------------------------------+
     *   | timestamp too old         | tail slot                              |
     *   | timestamp in future       | head slot (newest retained)            |
     *   | otherwise                 | floor(timestamp) to granularity        |
     *   +---------------------------+----------------------------------------+
     * </pre>
     */
    private int bucketIndex(long timestampMillis) {
        long slotStart = slotStartForTimestamp(Math.max(0, timestampMillis));
        if (slotStart < tailSlotStart) {
            slotStart = tailSlotStart;
        } else {
            long headSlot = tailSlotStart + (long) (counts.length() - 1) * granularityMillis;
            if (slotStart > headSlot) {
                slotStart = headSlot;
            }
        }
        return Math.toIntExact((slotStart - tailSlotStart) / granularityMillis);
    }

    /**
     * Truncates {@code timestampMillis} down to the start of its granularity-aligned slot.
     * <p>
     * Example: granularity 1h, {@code timestamp=10:37} -> {@code 10:00}.
     */
    private long slotStartForTimestamp(long timestampMillis) {
        return Math.floorDiv(timestampMillis, granularityMillis) * granularityMillis;
    }
}
