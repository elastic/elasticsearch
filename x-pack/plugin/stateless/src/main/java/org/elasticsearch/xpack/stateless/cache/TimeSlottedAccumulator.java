/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;

import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.LongSupplier;

/**
 * A {@link TimestampAccumulator} implementation backed by a fixed array of time slots.
 * The array is anchored at construction time: {@code pastSlots} slots ending at the anchor (inclusive),
 * plus {@code futureSlots} slots after the anchor ({@code pastSlots + futureSlots} total).
 * Counts are updated atomically per slot.
 * <p>
 * A <em>slot</em> is the granularity-aligned start time (e.g., in epoch millis) of a time range. Array index {@code 0}
 * is the tail (the oldest retained slot); higher indices are progressively newer slots up to the head.
 */
public final class TimeSlottedAccumulator implements TimestampAccumulator {

    public static final Setting<TimeValue> TIME_SLOTS_GRANULARITY_SETTING = Setting.timeSetting(
        "stateless.cache_boost_preference.time_slots.granularity",
        TimeValue.timeValueHours(1),
        TimeValue.timeValueMinutes(1),
        Setting.Property.NodeScope
    );

    public static final Setting<Long> TIME_SLOTS_PAST_COUNT_SETTING = Setting.longSetting(
        "stateless.cache_boost_preference.time_slots.past.count",
        87600L, // default 10y past retention (for 1h slots)
        1L,
        Setting.Property.NodeScope
    );

    public static final Setting<Long> TIME_SLOTS_FUTURE_COUNT_SETTING = Setting.longSetting(
        "stateless.cache_boost_preference.time_slots.future.count",
        8760L, // default 1y future retention (for 1h slots); uptime beyond this w/o restart clamps new counts to the head slot
        0L,
        Setting.Property.NodeScope
    );

    private final long granularityMillis;

    private final AtomicLongArray counts;

    /** Oldest retained (tail) slot's granularity-aligned time; fixed at construction. */
    private final long tailSlotMillis;

    /** Newest retained (head) slot's granularity-aligned time; fixed at construction. */
    private final long headSlotMillis;

    /** Exclusive end of the retained window: {@code headSlotMillis + granularityMillis}. */
    private final long retainedEndExclusiveMillis;

    /**
     * Creates a fixed array of {@code pastSlots + futureSlots} atomic slot counts, one per time slot of
     * {@code granularity}, anchored at the current time.
     * <p>
     * Example ({@code pastSlots=4}, {@code futureSlots=2}, 1h granularity, clock at 12:00):
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
     * @param pastSlots number of past slots retained, including the anchor slot
     * @param futureSlots number of future slots retained beyond the anchor slot
     * @param timeProvider source of time in milliseconds
     */
    public TimeSlottedAccumulator(TimeValue granularity, long pastSlots, long futureSlots, LongSupplier timeProvider) {
        if (granularity.millis() <= 0) {
            throw new IllegalArgumentException("granularity must be positive");
        }
        if (pastSlots < 1) {
            throw new IllegalArgumentException("pastSlots must be >= 1");
        }
        if (futureSlots < 0) {
            throw new IllegalArgumentException("futureSlots must be >= 0");
        }
        final long totalSlots;
        try {
            totalSlots = Math.addExact(pastSlots, futureSlots);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException(
                "pastSlots + futureSlots overflowed but was [pastSlots=" + pastSlots + ", futureSlots=" + futureSlots + "]",
                e
            );
        }
        if (totalSlots > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                "pastSlots + futureSlots must not exceed " + Integer.MAX_VALUE + " but was [" + totalSlots + "]"
            );
        }
        this.granularityMillis = granularity.millis();
        this.counts = new AtomicLongArray((int) totalSlots);
        long anchorSlotMillis = alignTimestampToSlotMillis(Math.max(0, timeProvider.getAsLong()));
        try {
            this.tailSlotMillis = Math.subtractExact(anchorSlotMillis, Math.multiplyExact(pastSlots - 1, granularityMillis));
            this.headSlotMillis = Math.addExact(anchorSlotMillis, Math.multiplyExact(futureSlots, granularityMillis));
            this.retainedEndExclusiveMillis = Math.addExact(headSlotMillis, granularityMillis);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("slot configuration overflows", e);
        }
    }

    public static TimestampAccumulator createFromSettings(Settings settings, LongSupplier timeProvider) {
        return new TimeSlottedAccumulator(
            TIME_SLOTS_GRANULARITY_SETTING.get(settings),
            TIME_SLOTS_PAST_COUNT_SETTING.get(settings),
            TIME_SLOTS_FUTURE_COUNT_SETTING.get(settings),
            timeProvider
        );
    }

    // package-private for testing
    TimeValue granularity() {
        return TimeValue.timeValueMillis(granularityMillis);
    }

    // package-private for testing
    int slots() {
        return counts.length();
    }

    /**
     * Adds {@code delta} to the slot containing {@code timestampMillis}. A negative {@code delta} subtracts from the slot.
     */
    @Override
    public void accumulate(long timestampMillis, long delta) {
        if (delta == 0) {
            return;
        }
        int slot = slotForTimestamp(timestampMillis);
        counts.addAndGet(slot, delta);
    }

    /**
     * Returns the sum of counts in slots overlapping {@code [startMillis, endMillis)}.
     * The query range is clamped to {@code [tailSlot, headSlot + granularity)} before summing.
     * Counts are stored per granularity slot: any overlap with a slot includes that slot's full count,
     * not a fraction proportional to how much of the slot lies inside the query range.
     * If the sum overflows or underflows {@code long}, the result saturates to {@link Long#MAX_VALUE}
     * or {@link Long#MIN_VALUE}.
     * <p>
     * Example (1h slots, range {@code [10:37, 13:00)}):
     * <pre>
     *   includes slots 10:00, 11:00, 12:00 (each [slot, slot+granularity) intersects the range)
     *   excludes slot 13:00 (starts at the range end)
     * </pre>
     */
    @Override
    public long sum(long startMillis, long endMillis) {
        if (endMillis <= startMillis) {
            return 0;
        }
        startMillis = Math.max(tailSlotMillis, Math.max(0, startMillis));
        endMillis = Math.min(endMillis, retainedEndExclusiveMillis);
        if (endMillis <= startMillis) {
            return 0;
        }
        int start = Math.max(0, slotForTimestamp(startMillis));
        int end = Math.min(counts.length(), slotForTimestamp(endMillis - 1) + 1);
        long total = 0;
        for (int slot = start; slot < end; slot++) {
            total = addSaturating(total, counts.get(slot));
        }
        return total;
    }

    private static long addSaturating(long left, long right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException e) {
            return left > 0 ? Long.MAX_VALUE : Long.MIN_VALUE;
        }
    }

    /**
     * Maps a timestamp to its array slot index: truncates to a granularity-aligned slot timestamp, clamps to
     * {@code [tailSlotMillis, headSlotMillis]}, then returns the offset from the tail (whose index is 0).
     */
    private int slotForTimestamp(long timestampMillis) {
        long slot = Math.clamp(alignTimestampToSlotMillis(Math.max(0, timestampMillis)), tailSlotMillis, headSlotMillis);
        return (int) Math.floorDiv(slot - tailSlotMillis, granularityMillis);
    }

    /**
     * Truncates {@code timestampMillis} down to its granularity-aligned slot timestamp.
     * <p>
     * Example: granularity 1h, {@code timestamp=10:37} -> slot {@code 10:00}.
     */
    private long alignTimestampToSlotMillis(long timestampMillis) {
        return Math.floorDiv(timestampMillis, granularityMillis) * granularityMillis;
    }

}
