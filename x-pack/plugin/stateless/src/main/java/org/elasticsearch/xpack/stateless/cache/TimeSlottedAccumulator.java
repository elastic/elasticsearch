/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.TimeProvider;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.concurrent.atomic.AtomicIntegerArray;

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

    private static final Logger logger = LogManager.getLogger(TimeSlottedAccumulator.class);

    /** Maximum allowed memory for the per-slot counts array. */
    static final long MAX_COUNTS_ARRAY_BYTES = ByteSizeValue.ofGb(4).getBytes();

    /** Maximum length of the per-slot counts array. */
    static final int MAX_TOTAL_SLOTS = (int) (MAX_COUNTS_ARRAY_BYTES / Integer.BYTES);

    public static final Setting<TimeValue> TIME_SLOTS_GRANULARITY_SETTING = Setting.timeSetting(
        "stateless.cache_boost_preference.time_slots.granularity",
        TimeValue.timeValueHours(1),
        TimeValue.timeValueMinutes(1),
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> TIME_SLOTS_PAST_COUNT_SETTING = Setting.intSetting(
        "stateless.cache_boost_preference.time_slots.past.count",
        87600, // default 10y past retention (for 1h slots)
        1,
        MAX_TOTAL_SLOTS,
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> TIME_SLOTS_FUTURE_COUNT_SETTING = Setting.intSetting(
        "stateless.cache_boost_preference.time_slots.future.count",
        8760, // default 1y future retention (for 1h slots); uptime beyond this w/o restart clamps new counts to the head slot
        0,
        MAX_TOTAL_SLOTS,
        Setting.Property.NodeScope
    );

    private final long granularityMillis;

    private final AtomicIntegerArray counts;

    /** Start of the oldest retained slot (i.e., the tail at index 0), in epoch milliseconds. Fixed at construction,
     *  computed from granularity and pastSlots.
     */
    private final long tailSlotStartMillis;

    /** Start of the newest retained slot (i.e., the head at index {@code pastSlots + futureSlots - 1}), in epoch milliseconds.
     *  Fixed at construction, computed from granularity and pastSlots.
     */
    private final long headSlotStartMillis;

    /** Exclusive end of the head slot: {@code headSlotStartMillis + granularityMillis}. */
    private final long headSlotEndExclusiveMillis;

    /**
     * Creates a fixed array of {@code pastSlots + futureSlots} atomic slot counts, one per time slot of
     * {@code granularity}, anchored at the current time. The anchor slot counts towards {@code pastSlots}.
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
     * @param timeProvider source of time in milliseconds; {@code absoluteTimeInMillis()} must be large enough that
     *                     {@code (pastSlots - 1) * granularity} past slots fit after epoch
     */
    public TimeSlottedAccumulator(TimeValue granularity, int pastSlots, int futureSlots, TimeProvider timeProvider) {
        if (granularity.millis() <= 0) {
            throw new IllegalArgumentException("granularity must be positive");
        }
        if (pastSlots < 1) {
            throw new IllegalArgumentException("pastSlots must be >= 1");
        }
        if (futureSlots < 0) {
            throw new IllegalArgumentException("futureSlots must be >= 0");
        }
        final int totalSlots;
        try {
            totalSlots = Math.addExact(pastSlots, futureSlots);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException(
                "pastSlots + futureSlots overflowed [pastSlots=" + pastSlots + ", futureSlots=" + futureSlots + "]",
                e
            );
        }
        if (totalSlots > MAX_TOTAL_SLOTS) {
            throw new IllegalArgumentException(
                "pastSlots + futureSlots ["
                    + totalSlots
                    + "] exceeds maximum ["
                    + MAX_TOTAL_SLOTS
                    + "] ("
                    + ByteSizeValue.ofBytes(MAX_COUNTS_ARRAY_BYTES)
                    + " counts array limit)"
            );
        }
        this.granularityMillis = granularity.millis();
        this.counts = new AtomicIntegerArray(totalSlots);
        long anchorSlotStartMillis = toSlotStartMillis(timeProvider.absoluteTimeInMillis());
        try {
            long minAnchorSlotStartMillis = Math.multiplyExact(pastSlots - 1, granularityMillis);
            if (anchorSlotStartMillis < minAnchorSlotStartMillis) {
                throw new IllegalArgumentException(
                    "timeProvider.absoluteTimeInMillis() ["
                        + timeProvider.absoluteTimeInMillis()
                        + "] is too early for pastSlots ["
                        + pastSlots
                        + "] at granularity ["
                        + granularity
                        + "]; anchor slot start ["
                        + anchorSlotStartMillis
                        + "] must be >= ["
                        + minAnchorSlotStartMillis
                        + "]"
                );
            }
            this.tailSlotStartMillis = Math.subtractExact(anchorSlotStartMillis, minAnchorSlotStartMillis);
            this.headSlotStartMillis = Math.addExact(anchorSlotStartMillis, Math.multiplyExact(futureSlots, granularityMillis));
            this.headSlotEndExclusiveMillis = Math.addExact(headSlotStartMillis, granularityMillis);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("slot configuration overflows", e);
        }
        if (tailSlotStartMillis < 0 || headSlotStartMillis < 0) {
            throw new IllegalStateException(
                "retained slot bounds must be non-negative [tail=" + tailSlotStartMillis + ", head=" + headSlotStartMillis + "]"
            );
        }
    }

    public static TimestampAccumulator createFromSettings(Settings settings, TimeProvider timeProvider) {
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
     * If callers are expected to keep per-slot counts non-negative, use the return value to verify that invariant.
     *
     * @return the slot count after applying {@code delta}
     */
    @Override
    public int accumulate(long timestampMillis, int delta) {
        int slot = slotForTimestamp(timestampMillis);
        return counts.addAndGet(slot, delta);
    }

    /**
     * Returns the sum of counts in slots overlapping {@code [startMillis, endMillis)}.
     * The query range is clamped to {@code [tailSlot, headSlot + granularity)} before summing.
     * Counts are stored per granularity slot: any overlap with a slot includes that slot's full count,
     * not a fraction proportional to how much of the slot lies inside the query range.
     * If the sum overflows or underflows {@code int}, the result saturates to {@link Integer#MAX_VALUE}
     * or {@link Integer#MIN_VALUE}.
     * <p>
     * Note: {@link #accumulate} clamps out-of-range timestamps into the retained tail or head slot, but
     * {@code sum} clamps the query range to retained slots first. A query window entirely before the tail
     * therefore returns {@code 0} even though {@code accumulate} would have counted such timestamps in the
     * tail slot. This asymmetry is intentional: counts beyond the retained window cannot be reconstructed.
     * <p>
     * Example (1h slots, range {@code [10:37, 13:00)}):
     * <pre>
     *   includes slots 10:00, 11:00, 12:00 (each [slot, slot+granularity) intersects the range)
     *   excludes slot 13:00 (starts at the range end)
     * </pre>
     */
    @Override
    public int sum(long startMillis, long endMillis) {
        if (endMillis <= startMillis) {
            return 0;
        }
        startMillis = Math.max(tailSlotStartMillis, startMillis);
        endMillis = Math.min(endMillis, headSlotEndExclusiveMillis);
        if (endMillis <= startMillis) {
            return 0;
        }
        int startSlot = Math.max(0, slotForTimestamp(startMillis));
        int endSlot = Math.min(counts.length(), slotForTimestamp(endMillis - 1) + 1);
        int total = 0;
        for (int slot = startSlot; slot < endSlot; slot++) {
            int result;
            int right = counts.get(slot);
            try {
                result = Math.addExact(total, right);
            } catch (ArithmeticException e) {
                logger.warn("sum overflowed while adding slot count [{}] to total [{}]", right, total);
                return right > 0 ? Integer.MAX_VALUE : Integer.MIN_VALUE;
            }
            total = result;
        }
        return total;
    }

    /**
     * Maps a timestamp to its array slot index: truncates to a granularity-aligned slot timestamp, clamps to
     * {@code [tailSlotMillis, headSlotMillis]}, then returns the offset from the tail (whose index is 0).
     */
    private int slotForTimestamp(long timestampMillis) {
        long normalizedTimestampMillis = Math.clamp(toSlotStartMillis(timestampMillis), tailSlotStartMillis, headSlotStartMillis);
        return (int) Math.floorDiv(normalizedTimestampMillis - tailSlotStartMillis, granularityMillis);
    }

    /**
     * Truncates {@code timestampMillis} down to its granularity-aligned slot timestamp.
     * <p>
     * Example: granularity 1h, {@code timestamp=10:37} -> {@code 10:00}.
     */
    private long toSlotStartMillis(long timestampMillis) {
        return Math.floorDiv(timestampMillis, granularityMillis) * granularityMillis;
    }

}
