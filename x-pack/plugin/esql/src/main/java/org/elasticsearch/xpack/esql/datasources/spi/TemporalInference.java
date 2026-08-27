/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.common.time.DateUtils;

import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;

/**
 * The one place that decides, for every text rail, when a parsed timestamp string means
 * {@code date_nanos} rather than {@code datetime}.
 *
 * <h2>The decision</h2>
 * A timestamp forces {@code date_nanos} when it carries a <b>non-zero sub-millisecond component</b>:
 * such a value has no lossless {@code datetime} reading, because {@code datetime} is epoch-millis and
 * the extra digits would simply be dropped. This is a property of the value, not of how it was
 * written &mdash; {@code .360000000Z} is millisecond-exact and stays {@code datetime}, while
 * {@code .360103847Z} cannot round-trip and becomes {@code date_nanos}.
 *
 * <p>Deciding on the value rather than on the text is deliberate. A fixed-width nine-digit fraction is
 * the default rendering of many exporters, and those files lose nothing today; retyping them would
 * change the column type of a large population for no gain. Confining the flip to values that
 * genuinely cannot round-trip means every flipped column contains at least one value the
 * {@code datetime} rail would have silently truncated.
 *
 * <h2>Why this is free</h2>
 * Both text inferrers already parse the string to decide whether it is a timestamp at all, and both
 * already throw the parse result away. This class reads the answer off that existing result: no
 * second parse, no regex, no character scan, no allocation on the common path. A text-based test
 * ("more than three fractional digits") could not work this way &mdash; a parsed accessor cannot
 * distinguish {@code .36} from {@code .360000000} &mdash; so it would need the character scan that
 * this one avoids.
 *
 * <h2>The range guard</h2>
 * {@code date_nanos} represents exactly {@code [1970-01-01T00:00:00Z, 2262-04-11T23:47:16.854775807Z]}
 * ({@link DateUtils#toLong} rejects both sides). A sub-millisecond value outside that window has no
 * {@code date_nanos} reading at all, and every decode rail would fail such a cell, so it must stay
 * {@code datetime} &mdash; where the value at least still has a millisecond representation. The guard
 * is checked off the same already-parsed accessor, via a year pre-filter that keeps the common case
 * allocation-free.
 *
 * <h2>What this class does not decide</h2>
 * It is only consulted on the default ISO rail. When a file declares its own {@code datetime_format},
 * the user has expressed intent and the declared-schema route is the way to ask for nanoseconds; each
 * inferrer applies that gate itself before calling here.
 *
 * <p>It also says nothing about whether a <i>column</i> ends up {@code date_nanos} &mdash; that is the
 * inferrer's type-resolution job. This answers one question about one value.
 *
 * <p>Nor does it check that the string would survive the {@code date_nanos} <i>decode</i> rail. Two
 * dialects the CSV rail's datetime parser accepts are rejected by {@code strict_date_optional_time_nanos}:
 * the whitespace-separated form ({@code 2023-10-23 12:15:03}) and times without seconds. The second can
 * never be a forcing value &mdash; a fraction requires seconds in both parsers &mdash; but the first can,
 * so the CSV inferrer screens for it before consulting this class.
 */
public final class TemporalInference {

    private static final long MAX_EPOCH_SECOND = DateUtils.MAX_NANOSECOND_INSTANT.getEpochSecond();
    private static final int MAX_NANO_OF_SECOND = DateUtils.MAX_NANOSECOND_INSTANT.getNano();

    private static final int NANOS_PER_MILLI = 1_000_000;

    // The window's edges sit in 1970 and 2262, and a zone offset can move an instant by at most 18h,
    // so a local year of 1971..2261 is inside the window whatever the offset, and 1968-or-earlier /
    // 2263-or-later is outside it. Only the three straddling years need the exact check.
    private static final int FIRST_UNAMBIGUOUSLY_IN_YEAR = 1971;
    private static final int LAST_UNAMBIGUOUSLY_IN_YEAR = 2261;
    private static final int LAST_UNAMBIGUOUSLY_OUT_YEAR_BELOW = 1968;
    private static final int FIRST_UNAMBIGUOUSLY_OUT_YEAR_ABOVE = 2263;

    private TemporalInference() {}

    /**
     * Whether an already-parsed timestamp must be read as {@code date_nanos} to be read losslessly:
     * it has a non-zero sub-millisecond component and falls inside the {@code date_nanos} window.
     *
     * <p>Callers pass the accessor their existing parse produced. {@code false} means "leave this value
     * at {@code datetime}", which covers two different situations: the value is millisecond-exact and
     * loses nothing there, or it carries sub-millisecond digits but falls outside the window, where
     * {@code datetime} is merely the best available reading &mdash; it still drops those digits, and
     * {@code date_nanos} could not have held the value at all.
     *
     * @param parsed the result of the caller's own successful datetime parse
     */
    public static boolean forcesDateNanos(TemporalAccessor parsed) {
        if (parsed.isSupported(ChronoField.NANO_OF_SECOND) == false) {
            return false; // date-only, or otherwise no time-of-day: no fraction to lose
        }
        long nanoOfSecond = parsed.getLong(ChronoField.NANO_OF_SECOND);
        if (nanoOfSecond % NANOS_PER_MILLI == 0) {
            return false; // millisecond-exact: datetime reads it losslessly
        }

        // In range? The fraction is offset-independent (offsets are whole seconds), so only the
        // instant needs the window check, and only near the window's edges is it not decidable
        // from the year alone.
        int year = (int) parsed.getLong(ChronoField.YEAR);
        if (year >= FIRST_UNAMBIGUOUSLY_IN_YEAR && year <= LAST_UNAMBIGUOUSLY_IN_YEAR) {
            return true;
        }
        if (year <= LAST_UNAMBIGUOUSLY_OUT_YEAR_BELOW || year >= FIRST_UNAMBIGUOUSLY_OUT_YEAR_ABOVE) {
            return false;
        }
        return inWindowExactly(parsed, nanoOfSecond);
    }

    /**
     * The exact window check, for the three years that straddle a window edge. This is the only arm
     * that can allocate, and only for a value that is already known to carry sub-millisecond digits.
     */
    private static boolean inWindowExactly(TemporalAccessor parsed, long nanoOfSecond) {
        if (parsed.isSupported(ChronoField.INSTANT_SECONDS) == false) {
            return false; // not enough fields to place it on the timeline; datetime keeps the value
        }
        if (parsed.isSupported(ChronoField.OFFSET_SECONDS) == false && parsed.query(TemporalQueries.zoneId()) != null) {
            // A named zone with no resolved offset: the epoch second cannot be computed here without
            // applying that zone's rules, and reading it as UTC would place the instant up to a day
            // off — enough to cross the epoch floor. Leave it at datetime rather than guess.
            return false;
        }
        long epochSecond = parsed.getLong(ChronoField.INSTANT_SECONDS);
        if (epochSecond < 0) {
            return false; // date_nanos has no pre-epoch representation at all
        }
        return epochSecond < MAX_EPOCH_SECOND || (epochSecond == MAX_EPOCH_SECOND && nanoOfSecond <= MAX_NANO_OF_SECOND);
    }
}
