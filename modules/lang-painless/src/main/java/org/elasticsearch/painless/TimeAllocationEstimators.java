/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

import org.elasticsearch.core.Nullable;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalAdjuster;
import java.time.temporal.TemporalAmount;
import java.time.temporal.TemporalField;
import java.time.temporal.TemporalQuery;
import java.time.temporal.TemporalUnit;
import java.util.Locale;

/**
 * {@code @allocates} estimators for the {@code java.time} allowlists. They live in their own class because there are a lot
 * of them and they all use a few fixed sizes.
 *
 * <p>Same rules as {@link AllocationEstimators}: {@code public static long}, same parameters as the annotated method with
 * the receiver first, run before the real call, never throw.
 *
 * <p>Three sizes cover almost everything. A flat value is one object with only primitive fields. A composite is an object
 * that points at other new values, so the whole chain is charged. Text members are sized from the text. A call that
 * returns a cached object, like {@code Instant.EPOCH}, is still charged in full, because we cannot know that before the call.
 */
public final class TimeAllocationEstimators {

    private TimeAllocationEstimators() {}

    /** Size of an object that holds only {@code references} references. */
    private static long shellBytes(int references) {
        return AllocSizes.pad8(AllocSizes.OBJECT_HEADER + (long) references * AllocSizes.REFERENCE_SIZE);
    }

    /**
     * A flat value: one object with only primitive fields. The biggest is a {@code long} plus an {@code int}, which pads to
     * 24 bytes. Covers {@link Instant}, {@link java.time.Duration}, {@link LocalDate}, {@link LocalTime},
     * {@link java.time.Period} and {@link java.time.MonthDay}.
     */
    public static final long FLAT_VALUE_BYTES = AllocSizes.pad8(AllocSizes.OBJECT_HEADER + Long.BYTES + Integer.BYTES);

    /** A {@link LocalDateTime}: its own object with two references, plus a new {@code LocalDate} and {@code LocalTime}. 80 bytes. */
    public static final long LOCAL_DATE_TIME_BYTES = shellBytes(2) + 2L * FLAT_VALUE_BYTES;

    /**
     * An {@link java.time.OffsetDateTime}: its own object with two references, plus the local date-time chain. Offsets are
     * cached. 112 bytes.
     */
    public static final long OFFSET_DATE_TIME_BYTES = shellBytes(2) + LOCAL_DATE_TIME_BYTES;

    /**
     * A {@link ZonedDateTime}: its own object with three references, plus the local date-time chain. Offset and zone are
     * cached. 120 bytes.
     */
    public static final long ZONED_DATE_TIME_BYTES = shellBytes(3) + LOCAL_DATE_TIME_BYTES;

    /**
     * Characters allowed for one {@code DateTimeFormatter.format} result. A formatter does not expose its pattern length, so
     * this is a fixed allowance. 128 is several times the longest pattern the JDK ships.
     */
    private static final long FORMAT_CHARACTERS = 128;

    /** Fixed allowance for the parse state a formatter builds: a field map, a chronology, a zone and the resolver. */
    private static final long PARSE_CONTEXT_BYTES = 512;

    /** Bytes charged per character of parsed text, in case the parser copies its input. */
    private static final long PARSE_BYTES_PER_CHARACTER = 2;

    /** Base cost of building a formatter from a pattern: the builder, its element list, and the formatter. */
    private static final long PATTERN_BASE_BYTES = 256;

    /** Bytes charged per pattern character. Each letter can add a printer-parser object and a list slot. */
    private static final long PATTERN_BYTES_PER_CHARACTER = 64;

    /** A new {@link String} of {@code chars} characters: the object plus its array. */
    private static long newStringBytes(long chars) {
        return AllocSizes.addSat(AllocSizes.STRING_CONCAT_RESULT_OVERHEAD, AllocSizes.mulSat(2L, Math.max(0L, chars)));
    }

    /** Length of {@code text}. A {@code null} counts as empty; the real call rejects it. */
    private static long textLength(@Nullable CharSequence text) {
        return text == null ? 0 : text.length();
    }

    // ---- Flat values, 24 bytes. ----

    /** {@code Instant.from(TemporalAccessor)}. */
    public static long flatValueBytes(TemporalAccessor temporal) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code Instant.ofEpochSecond(long)} and {@code Instant.ofEpochMilli(long)}. */
    public static long flatValueBytes(long value) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code Instant.ofEpochSecond(long, long)}. */
    public static long flatValueBytes(long value, long adjustment) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code Instant.plus(TemporalAmount)} and {@code Instant.minus(TemporalAmount)}. */
    public static long flatValueBytes(Instant receiver, TemporalAmount amount) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code Instant.plus(long, TemporalUnit)} and {@code Instant.minus(long, TemporalUnit)}. */
    public static long flatValueBytes(Instant receiver, long amount, TemporalUnit unit) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code Instant.plusSeconds}, {@code plusMillis}, {@code plusNanos} and their {@code minus} counterparts. */
    public static long flatValueBytes(Instant receiver, long amount) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code Instant.truncatedTo(TemporalUnit)}. */
    public static long flatValueBytes(Instant receiver, TemporalUnit unit) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code Instant.with(TemporalAdjuster)}. */
    public static long flatValueBytes(Instant receiver, TemporalAdjuster adjuster) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code Instant.with(TemporalField, long)}. */
    public static long flatValueBytes(Instant receiver, TemporalField field, long value) {
        return FLAT_VALUE_BYTES;
    }

    // ---- Composites: the returned object plus every new value under it. ----

    /** {@code Instant.atOffset(ZoneOffset)}: builds the whole offset date-time chain. */
    public static long offsetDateTimeBytes(Instant receiver, ZoneOffset offset) {
        return OFFSET_DATE_TIME_BYTES;
    }

    /** {@code ZonedDateTime.toOffsetDateTime()}. */
    public static long offsetDateTimeBytes(ZonedDateTime receiver) {
        return OFFSET_DATE_TIME_BYTES;
    }

    /** {@code ZonedDateTime.from(TemporalAccessor)}. */
    public static long zonedDateTimeBytes(TemporalAccessor temporal) {
        return ZONED_DATE_TIME_BYTES;
    }

    /** {@code Instant.atZone(ZoneId)} and {@code ZonedDateTime.ofInstant(Instant, ZoneId)}: build the whole chain. */
    public static long zonedDateTimeBytes(Instant instant, ZoneId zone) {
        return ZONED_DATE_TIME_BYTES;
    }

    /** {@code ZonedDateTime.of(LocalDate, LocalTime, ZoneId)}. */
    public static long zonedDateTimeBytes(LocalDate date, LocalTime time, ZoneId zone) {
        return ZONED_DATE_TIME_BYTES;
    }

    /** {@code ZonedDateTime.of(LocalDateTime, ZoneId)}. */
    public static long zonedDateTimeBytes(LocalDateTime dateTime, ZoneId zone) {
        return ZONED_DATE_TIME_BYTES;
    }

    /** {@code ZonedDateTime.ofInstant(LocalDateTime, ZoneOffset, ZoneId)} and {@code ofStrict(LocalDateTime, ZoneOffset, ZoneId)}. */
    public static long zonedDateTimeBytes(LocalDateTime dateTime, ZoneOffset offset, ZoneId zone) {
        return ZONED_DATE_TIME_BYTES;
    }

    /** {@code ZonedDateTime.ofLocal(LocalDateTime, ZoneId, ZoneOffset)}. */
    public static long zonedDateTimeBytes(LocalDateTime dateTime, ZoneId zone, ZoneOffset preferredOffset) {
        return ZONED_DATE_TIME_BYTES;
    }

    /** {@code ZonedDateTime.of(int, int, int, int, int, int, int, ZoneId)}. */
    public static long zonedDateTimeBytes(
        int year,
        int month,
        int dayOfMonth,
        int hour,
        int minute,
        int second,
        int nanoOfSecond,
        ZoneId zone
    ) {
        return ZONED_DATE_TIME_BYTES;
    }

    /**
     * {@code ZonedDateTime.withEarlierOffsetAtOverlap()}, {@code withLaterOffsetAtOverlap()} and {@code withFixedOffsetZone()}.
     * These often return the receiver, but the full size is charged either way.
     */
    public static long zonedDateTimeBytes(ZonedDateTime receiver) {
        return ZONED_DATE_TIME_BYTES;
    }

    /** {@code ZonedDateTime.plus(TemporalAmount)} and {@code minus(TemporalAmount)}. */
    public static long zonedDateTimeBytes(ZonedDateTime receiver, TemporalAmount amount) {
        return ZONED_DATE_TIME_BYTES;
    }

    /** {@code ZonedDateTime.plus(long, TemporalUnit)} and {@code minus(long, TemporalUnit)}. */
    public static long zonedDateTimeBytes(ZonedDateTime receiver, long amount, TemporalUnit unit) {
        return ZONED_DATE_TIME_BYTES;
    }

    /**
     * {@code ZonedDateTime.plusYears} through {@code plusNanos} and the {@code minus} forms. These always build a new chain,
     * even for zero.
     */
    public static long zonedDateTimeBytes(ZonedDateTime receiver, long amount) {
        return ZONED_DATE_TIME_BYTES;
    }

    /** {@code ZonedDateTime.withYear} through {@code withNano}. */
    public static long zonedDateTimeBytes(ZonedDateTime receiver, int value) {
        return ZONED_DATE_TIME_BYTES;
    }

    /** {@code ZonedDateTime.truncatedTo(TemporalUnit)}. */
    public static long zonedDateTimeBytes(ZonedDateTime receiver, TemporalUnit unit) {
        return ZONED_DATE_TIME_BYTES;
    }

    /** {@code ZonedDateTime.with(TemporalAdjuster)}. */
    public static long zonedDateTimeBytes(ZonedDateTime receiver, TemporalAdjuster adjuster) {
        return ZONED_DATE_TIME_BYTES;
    }

    /** {@code ZonedDateTime.with(TemporalField, long)}. */
    public static long zonedDateTimeBytes(ZonedDateTime receiver, TemporalField field, long value) {
        return ZONED_DATE_TIME_BYTES;
    }

    /** {@code ZonedDateTime.withZoneSameLocal(ZoneId)} and {@code withZoneSameInstant(ZoneId)}. */
    public static long zonedDateTimeBytes(ZonedDateTime receiver, ZoneId zone) {
        return ZONED_DATE_TIME_BYTES;
    }

    // ---- Text: parse state and new strings, sized from the text when we have it. ----

    /** {@code Instant.parse(CharSequence)}: the parse state plus the flat value. */
    public static long parseFlatValueBytes(CharSequence text) {
        return AllocSizes.addSat(parseTextBytes(text), FLAT_VALUE_BYTES);
    }

    /** {@code ZonedDateTime.parse(CharSequence)}: the parse state plus the zoned chain. */
    public static long parseZonedDateTimeBytes(CharSequence text) {
        return AllocSizes.addSat(parseTextBytes(text), ZONED_DATE_TIME_BYTES);
    }

    /** {@code ZonedDateTime.parse(CharSequence, DateTimeFormatter)}. */
    public static long parseZonedDateTimeBytes(CharSequence text, DateTimeFormatter formatter) {
        return parseZonedDateTimeBytes(text);
    }

    /** {@code DateTimeFormatter.parse(CharSequence)}: the parse state only. */
    public static long formatterParseBytes(DateTimeFormatter receiver, CharSequence text) {
        return parseTextBytes(text);
    }

    /** {@code DateTimeFormatter.parse(CharSequence, TemporalQuery)}: the parse state plus the biggest chain the query could build. */
    public static long formatterParseBytes(DateTimeFormatter receiver, CharSequence text, TemporalQuery<?> query) {
        return AllocSizes.addSat(parseTextBytes(text), ZONED_DATE_TIME_BYTES);
    }

    /** Parse state plus a per-character allowance for {@code text}. */
    private static long parseTextBytes(CharSequence text) {
        return AllocSizes.addSat(PARSE_CONTEXT_BYTES, AllocSizes.mulSat(PARSE_BYTES_PER_CHARACTER, textLength(text)));
    }

    /** {@code DateTimeFormatter.format(TemporalAccessor)}: a new string of {@link #FORMAT_CHARACTERS}. */
    public static long formatBytes(DateTimeFormatter receiver, TemporalAccessor temporal) {
        return newStringBytes(FORMAT_CHARACTERS);
    }

    /** {@code DateTimeFormatter.ofPattern(String)}: a base cost plus a cost per pattern character. */
    public static long ofPatternBytes(String pattern) {
        long patternCost = AllocSizes.mulSat(PATTERN_BYTES_PER_CHARACTER, textLength(pattern));
        return AllocSizes.addSat(PATTERN_BASE_BYTES, patternCost);
    }

    /** {@code DateTimeFormatter.ofPattern(String, Locale)}. */
    public static long ofPatternBytes(String pattern, Locale locale) {
        return ofPatternBytes(pattern);
    }
}
