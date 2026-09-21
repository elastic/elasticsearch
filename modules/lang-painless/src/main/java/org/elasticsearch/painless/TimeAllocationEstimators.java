/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

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
 * Built-in {@code @allocates} estimators for the {@code java.time} allowlists. These live apart from
 * {@link AllocationEstimators} because the date and time allowlists hold several hundred members that all fall into a
 * handful of size tiers, and keeping them here leaves the shared file readable.
 *
 * <p>The rules are the same as in the shared file. Every estimator is {@code public static long}, matches the annotated
 * member's full Java signature with the receiver first for instance methods, runs before the real call, and must not throw
 * or consume its arguments. Results are normalized by {@link AllocationGuard#sanitizeEstimate(long)}.
 *
 * <p>Three tiers cover nearly everything. A flat value holds only primitive fields. A composite holds references to other
 * value objects and allocates that whole chain. Text members build a string or a parse context, so they are sized from the
 * text. Over-charging is deliberate: a member that happens to return a cached constant, such as {@code Instant.EPOCH} for
 * zero, is still charged its full tier, because which calls hit the cache is not knowable before the call.
 */
public final class TimeAllocationEstimators {

    private TimeAllocationEstimators() {}

    /** Heap cost of an object whose only state is {@code references} references to other objects. */
    private static long shellBytes(int references) {
        return AllocSizes.pad8(AllocSizes.OBJECT_HEADER + (long) references * AllocSizes.REFERENCE_SIZE);
    }

    /**
     * Heap cost of a flat {@code java.time} value, one object holding only primitive fields. The widest such layout is an
     * object header plus a {@code long} and an {@code int}, which pads to 24 bytes. That covers {@link Instant} (long
     * seconds, int nanos), {@link java.time.Duration} (long seconds, int nanos), {@link LocalDate} (int year, byte month,
     * byte day), {@link LocalTime} (byte hour, byte minute, byte second, int nano), {@link java.time.Period} (three ints)
     * and {@link java.time.MonthDay} (two ints).
     */
    public static final long FLAT_VALUE_BYTES = AllocSizes.pad8(AllocSizes.OBJECT_HEADER + Long.BYTES + Integer.BYTES);

    /**
     * Heap cost of a {@link LocalDateTime} and everything it points at. Its own object holds a {@code LocalDate} and a
     * {@code LocalTime} reference, so a header plus two references pads to 32 bytes, and both referenced values are fresh
     * flat values at 24 bytes each. That is 80 bytes for the chain.
     */
    public static final long LOCAL_DATE_TIME_BYTES = shellBytes(2) + 2L * FLAT_VALUE_BYTES;

    /**
     * Heap cost of an {@link java.time.OffsetDateTime} chain. Its own object holds a {@code LocalDateTime} and a {@code ZoneOffset}
     * reference, so a header plus two references pads to 32 bytes, on top of the 80 byte local date-time chain. Offsets are
     * served from a static cache, so the offset itself costs nothing. That is 112 bytes.
     */
    public static final long OFFSET_DATE_TIME_BYTES = shellBytes(2) + LOCAL_DATE_TIME_BYTES;

    /**
     * Heap cost of a {@link ZonedDateTime} chain. Its own object holds a {@code LocalDateTime}, a {@code ZoneOffset} and a
     * {@code ZoneId} reference, so a header plus three references pads to 40 bytes, on top of the 80 byte local date-time
     * chain. Both the offset and the zone come from caches and cost nothing. That is 120 bytes.
     */
    public static final long ZONED_DATE_TIME_BYTES = shellBytes(3) + LOCAL_DATE_TIME_BYTES;

    /**
     * Characters allowed for one {@code DateTimeFormatter.format} result. The real driver is the formatter's pattern, but a
     * formatter does not expose its pattern length without building a string of its own, which an estimator may not do. So
     * this is a flat allowance rather than a measurement. 128 characters is several times the longest localized date-time
     * pattern the JDK ships.
     */
    private static final long FORMAT_CHARACTERS = 128;

    /**
     * Flat allowance for the parse context a formatter builds: a {@code Parsed} holding a field map, a chronology, a zone
     * and the resolver state. None of that is reachable before the call, so this is an allowance rather than a measurement,
     * set high enough to cover a parse that resolves every date and time field.
     */
    private static final long PARSE_CONTEXT_BYTES = 512;

    /** Bytes charged per character of parsed text, covering any copy the parser makes of its input. */
    private static final long PARSE_BYTES_PER_CHARACTER = 2;

    /**
     * Base cost of building a formatter from a pattern: the builder, its element list, and the finished formatter that the
     * builder is converted into.
     */
    private static final long PATTERN_BASE_BYTES = 256;

    /**
     * Bytes charged per pattern character. Each pattern letter can add a printer-parser object plus a slot in the builder's
     * element list, so the cost grows with the pattern instead of being fixed.
     */
    private static final long PATTERN_BYTES_PER_CHARACTER = 64;

    /** Heap cost of a new {@link String} of {@code chars} UTF-16 characters: the object plus its backing array. */
    private static long newStringBytes(long chars) {
        return AllocSizes.STRING_CONCAT_RESULT_OVERHEAD + AllocSizes.mulSat(2L, Math.max(0L, chars));
    }

    /** Length of {@code text}, treating a {@code null} the real call would reject as empty. */
    private static long textLength(CharSequence text) {
        return text == null ? 0 : text.length();
    }

    // ---- Flat value tier: one object of primitive fields, 24 bytes. ----

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

    // ---- Composite tiers: the returned object plus the whole value chain under it. ----

    /** {@code Instant.atOffset(ZoneOffset)}, which builds the local date, the local time and the offset date-time. */
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

    /** {@code Instant.atZone(ZoneId)} and {@code ZonedDateTime.ofInstant(Instant, ZoneId)}, which build the whole chain. */
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
     * {@code ZonedDateTime.withEarlierOffsetAtOverlap()}, {@code withLaterOffsetAtOverlap()} and
     * {@code withFixedOffsetZone()}. These return the receiver unless the zone rules say otherwise, and the full tier is
     * charged either way.
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
     * {@code ZonedDateTime.plusYears} through {@code plusNanos} and their {@code minus} counterparts. Unlike the flat
     * values, these never short-circuit on a zero amount, so a new chain is always built.
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

    // ---- Text tier: parse context and produced strings, sized from the text where the text is an argument. ----

    /** {@code Instant.parse(CharSequence)}: the parse context plus the flat value it resolves to. */
    public static long parseFlatValueBytes(CharSequence text) {
        return AllocSizes.addSat(parseTextBytes(text), FLAT_VALUE_BYTES);
    }

    /** {@code ZonedDateTime.parse(CharSequence)}: the parse context plus the whole zoned chain. */
    public static long parseZonedDateTimeBytes(CharSequence text) {
        return AllocSizes.addSat(parseTextBytes(text), ZONED_DATE_TIME_BYTES);
    }

    /** {@code ZonedDateTime.parse(CharSequence, DateTimeFormatter)}. */
    public static long parseZonedDateTimeBytes(CharSequence text, DateTimeFormatter formatter) {
        return parseZonedDateTimeBytes(text);
    }

    /** {@code DateTimeFormatter.parse(CharSequence)}: the parse context, with no resolved value handed back. */
    public static long formatterParseBytes(DateTimeFormatter receiver, CharSequence text) {
        return parseTextBytes(text);
    }

    /**
     * {@code DateTimeFormatter.parse(CharSequence, TemporalQuery)}: the parse context plus whatever the query builds from
     * it. The query is opaque here, so the widest chain is charged for the result.
     */
    public static long formatterParseBytes(DateTimeFormatter receiver, CharSequence text, TemporalQuery<?> query) {
        return AllocSizes.addSat(parseTextBytes(text), ZONED_DATE_TIME_BYTES);
    }

    /** Parse context plus a per-character allowance for any copy the parser makes of {@code text}. */
    private static long parseTextBytes(CharSequence text) {
        return AllocSizes.addSat(PARSE_CONTEXT_BYTES, AllocSizes.mulSat(PARSE_BYTES_PER_CHARACTER, textLength(text)));
    }

    /** {@code DateTimeFormatter.format(TemporalAccessor)}: a new string, bounded by {@link #FORMAT_CHARACTERS}. */
    public static long formatBytes(DateTimeFormatter receiver, TemporalAccessor temporal) {
        return newStringBytes(FORMAT_CHARACTERS);
    }

    /** {@code DateTimeFormatter.ofPattern(String)}: a base cost plus an allowance for every character of the pattern. */
    public static long ofPatternBytes(String pattern) {
        long patternCost = AllocSizes.mulSat(PATTERN_BYTES_PER_CHARACTER, textLength(pattern));
        return AllocSizes.addSat(PATTERN_BASE_BYTES, patternCost);
    }

    /** {@code DateTimeFormatter.ofPattern(String, Locale)}. */
    public static long ofPatternBytes(String pattern, Locale locale) {
        return ofPatternBytes(pattern);
    }
}
