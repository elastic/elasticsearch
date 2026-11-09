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

import java.text.ParsePosition;
import java.time.DayOfWeek;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.MonthDay;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.chrono.ChronoLocalDate;
import java.time.chrono.Chronology;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DecimalStyle;
import java.time.format.FormatStyle;
import java.time.format.ResolverStyle;
import java.time.format.SignStyle;
import java.time.format.TextStyle;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalAdjuster;
import java.time.temporal.TemporalAmount;
import java.time.temporal.TemporalField;
import java.time.temporal.TemporalQuery;
import java.time.temporal.TemporalUnit;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

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

    /** An {@link OffsetTime}: its own object with two references, plus a new {@code LocalTime}. Offsets are cached. 48 bytes. */
    public static final long OFFSET_TIME_BYTES = shellBytes(2) + FLAT_VALUE_BYTES;

    /** A {@link java.time.temporal.ValueRange}: four longs. 48 bytes. */
    public static final long VALUE_RANGE_BYTES = AllocSizes.pad8(AllocSizes.OBJECT_HEADER + 4L * Long.BYTES);

    /** A {@link DateTimeFormatter} copy from a {@code with} method: eight references. 80 bytes. */
    public static final long FORMATTER_BYTES = shellBytes(8);

    /** A new {@link java.time.format.DateTimeFormatterBuilder}: the builder and its element list. */
    private static final long BUILDER_BYTES = 128;

    /** Characters allowed for a display name or a localized pattern string. */
    private static final long DISPLAY_NAME_CHARACTERS = 32;
    private static final long PATTERN_TEXT_CHARACTERS = 64;

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

    // ---- Flat values, 24 bytes. ----

    /** {@code Clock.fixed}. */
    public static long flatValueBytes(Instant instant, ZoneId zoneId) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code Duration.abs}, {@code Duration.negated}. */
    public static long flatValueBytes(Duration receiver) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code Duration.between}. */
    public static long flatValueBytes(Temporal temporal, Temporal temporal2) {
        return FLAT_VALUE_BYTES;
    }

    /**
     * {@code Duration.dividedBy}, {@code Duration.minusDays}, {@code Duration.minusHours}, {@code Duration.minusMinutes}, {@code
     * Duration.minusSeconds}, {@code Duration.minusMillis} and 9 more.
     */
    public static long flatValueBytes(Duration receiver, long value) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code Duration.from}, {@code Period.from}. */
    public static long flatValueBytes(TemporalAmount temporalAmount) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code Duration.minus}, {@code Duration.plus}. */
    public static long flatValueBytes(Duration receiver, Duration duration) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code Duration.minus}, {@code Duration.plus}. */
    public static long flatValueBytes(Duration receiver, long value, TemporalUnit temporalUnit) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code Duration.of}. */
    public static long flatValueBytes(long value, TemporalUnit temporalUnit) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code Duration.withNanos}. */
    public static long flatValueBytes(Duration receiver, int value) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code LocalDate.minus}, {@code LocalDate.plus}. */
    public static long flatValueBytes(LocalDate receiver, TemporalAmount temporalAmount) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code LocalDate.minus}, {@code LocalDate.plus}. */
    public static long flatValueBytes(LocalDate receiver, long value, TemporalUnit temporalUnit) {
        return FLAT_VALUE_BYTES;
    }

    /**
     * {@code LocalDate.minusYears}, {@code LocalDate.minusMonths}, {@code LocalDate.minusWeeks}, {@code LocalDate.minusDays}, {@code
     * LocalDate.plusYears}, {@code LocalDate.plusMonths} and 2 more.
     */
    public static long flatValueBytes(LocalDate receiver, long value) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code LocalDate.of}, {@code LocalTime.of}, {@code Period.of}, {@code ZoneOffset.ofHoursMinutesSeconds}. */
    public static long flatValueBytes(int value, int value2, int value3) {
        return FLAT_VALUE_BYTES;
    }

    /**
     * {@code LocalDate.ofYearDay}, {@code LocalTime.of}, {@code MonthDay.of}, {@code YearMonth.of}, {@code
     * ZoneOffset.ofHoursMinutes}.
     */
    public static long flatValueBytes(int value, int value2) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code LocalDate.until}. */
    public static long flatValueBytes(LocalDate receiver, ChronoLocalDate chronoLocalDate) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code LocalDate.with}. */
    public static long flatValueBytes(LocalDate receiver, TemporalAdjuster temporalAdjuster) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code LocalDate.with}. */
    public static long flatValueBytes(LocalDate receiver, TemporalField temporalField, long value) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code LocalDate.withDayOfMonth}, {@code LocalDate.withDayOfYear}, {@code LocalDate.withMonth}, {@code LocalDate.withYear}. */
    public static long flatValueBytes(LocalDate receiver, int value) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code LocalTime.minus}, {@code LocalTime.plus}. */
    public static long flatValueBytes(LocalTime receiver, TemporalAmount temporalAmount) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code LocalTime.minus}, {@code LocalTime.plus}. */
    public static long flatValueBytes(LocalTime receiver, long value, TemporalUnit temporalUnit) {
        return FLAT_VALUE_BYTES;
    }

    /**
     * {@code LocalTime.minusHours}, {@code LocalTime.minusMinutes}, {@code LocalTime.minusNanos}, {@code LocalTime.minusSeconds},
     * {@code LocalTime.plusHours}, {@code LocalTime.plusMinutes} and 2 more.
     */
    public static long flatValueBytes(LocalTime receiver, long value) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code LocalTime.of}. */
    public static long flatValueBytes(int value, int value2, int value3, int value4) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code LocalTime.truncatedTo}. */
    public static long flatValueBytes(LocalTime receiver, TemporalUnit temporalUnit) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code LocalTime.with}. */
    public static long flatValueBytes(LocalTime receiver, TemporalAdjuster temporalAdjuster) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code LocalTime.with}. */
    public static long flatValueBytes(LocalTime receiver, TemporalField temporalField, long value) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code LocalTime.withHour}, {@code LocalTime.withMinute}, {@code LocalTime.withNano}, {@code LocalTime.withSecond}. */
    public static long flatValueBytes(LocalTime receiver, int value) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code MonthDay.atYear}, {@code MonthDay.withDayOfMonth}, {@code MonthDay.withMonth}. */
    public static long flatValueBytes(MonthDay receiver, int value) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code MonthDay.with}. */
    public static long flatValueBytes(MonthDay receiver, Month month) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code OffsetDateTime.toInstant}. */
    public static long flatValueBytes(OffsetDateTime receiver) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code Period.between}. */
    public static long flatValueBytes(LocalDate localDate, LocalDate localDate2) {
        return FLAT_VALUE_BYTES;
    }

    /**
     * {@code Period.ofYears}, {@code Period.ofMonths}, {@code Period.ofWeeks}, {@code Period.ofDays}, {@code Year.of}, {@code
     * ZoneOffset.ofHours} and 1 more.
     */
    public static long flatValueBytes(int value) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code Period.plus}, {@code Period.minus}. */
    public static long flatValueBytes(Period receiver, TemporalAmount temporalAmount) {
        return FLAT_VALUE_BYTES;
    }

    /**
     * {@code Period.plusYears}, {@code Period.plusMonths}, {@code Period.plusDays}, {@code Period.minusYears}, {@code
     * Period.minusMonths}, {@code Period.minusDays}.
     */
    public static long flatValueBytes(Period receiver, long value) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code Period.multipliedBy}, {@code Period.withDays}, {@code Period.withMonths}, {@code Period.withYears}. */
    public static long flatValueBytes(Period receiver, int value) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code Period.negated}, {@code Period.normalized}. */
    public static long flatValueBytes(Period receiver) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code Year.atDay}, {@code Year.atMonth}. */
    public static long flatValueBytes(Year receiver, int value) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code Year.atMonthDay}. */
    public static long flatValueBytes(Year receiver, MonthDay monthDay) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code Year.minus}, {@code Year.plus}. */
    public static long flatValueBytes(Year receiver, TemporalAmount temporalAmount) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code Year.minus}, {@code Year.plus}. */
    public static long flatValueBytes(Year receiver, long value, TemporalUnit temporalUnit) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code Year.minusYears}, {@code Year.plusYears}. */
    public static long flatValueBytes(Year receiver, long value) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code Year.with}. */
    public static long flatValueBytes(Year receiver, TemporalAdjuster temporalAdjuster) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code Year.with}. */
    public static long flatValueBytes(Year receiver, TemporalField temporalField, long value) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code YearMonth.atDay}, {@code YearMonth.withYear}, {@code YearMonth.withMonth}. */
    public static long flatValueBytes(YearMonth receiver, int value) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code YearMonth.atEndOfMonth}. */
    public static long flatValueBytes(YearMonth receiver) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code YearMonth.minus}, {@code YearMonth.plus}. */
    public static long flatValueBytes(YearMonth receiver, TemporalAmount temporalAmount) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code YearMonth.minus}, {@code YearMonth.plus}. */
    public static long flatValueBytes(YearMonth receiver, long value, TemporalUnit temporalUnit) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code YearMonth.minusYears}, {@code YearMonth.minusMonths}, {@code YearMonth.plusYears}, {@code YearMonth.plusMonths}. */
    public static long flatValueBytes(YearMonth receiver, long value) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code YearMonth.with}. */
    public static long flatValueBytes(YearMonth receiver, TemporalAdjuster temporalAdjuster) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code YearMonth.with}. */
    public static long flatValueBytes(YearMonth receiver, TemporalField temporalField, long value) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code ZoneId.of}, {@code ZoneOffset.of}. */
    public static long flatValueBytes(String text) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code ZoneId.of}. */
    public static long flatValueBytes(String text, Map<?, ?> aliases) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code ZoneId.ofOffset}. */
    public static long flatValueBytes(String text, ZoneOffset zoneOffset) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code ZoneId.normalized}. */
    public static long flatValueBytes(ZoneId receiver) {
        return FLAT_VALUE_BYTES;
    }

    /**
     * {@code DecimalStyle.withDecimalSeparator}, {@code DecimalStyle.withNegativeSign}, {@code DecimalStyle.withPositiveSign},
     * {@code DecimalStyle.withZeroDigit}.
     */
    public static long flatValueBytes(DecimalStyle receiver, char character) {
        return FLAT_VALUE_BYTES;
    }

    // ---- LocalDateTime chains, 80 bytes. ----

    /** {@code LocalDate.atStartOfDay}. */
    public static long localDateTimeBytes(LocalDate receiver) {
        return LOCAL_DATE_TIME_BYTES;
    }

    /** {@code LocalDate.atTime}, {@code LocalDateTime.of}. */
    public static long localDateTimeBytes(LocalDate receiver, LocalTime localTime) {
        return LOCAL_DATE_TIME_BYTES;
    }

    /** {@code LocalDate.atTime}. */
    public static long localDateTimeBytes(LocalDate receiver, int value, int value2) {
        return LOCAL_DATE_TIME_BYTES;
    }

    /** {@code LocalDate.atTime}. */
    public static long localDateTimeBytes(LocalDate receiver, int value, int value2, int value3) {
        return LOCAL_DATE_TIME_BYTES;
    }

    /** {@code LocalDate.atTime}. */
    public static long localDateTimeBytes(LocalDate receiver, int value, int value2, int value3, int value4) {
        return LOCAL_DATE_TIME_BYTES;
    }

    /** {@code LocalDateTime.from}. */
    public static long localDateTimeBytes(TemporalAccessor temporalAccessor) {
        return LOCAL_DATE_TIME_BYTES;
    }

    /** {@code LocalDateTime.minus}, {@code LocalDateTime.plus}. */
    public static long localDateTimeBytes(LocalDateTime receiver, TemporalAmount temporalAmount) {
        return LOCAL_DATE_TIME_BYTES;
    }

    /** {@code LocalDateTime.minus}, {@code LocalDateTime.plus}. */
    public static long localDateTimeBytes(LocalDateTime receiver, long value, TemporalUnit temporalUnit) {
        return LOCAL_DATE_TIME_BYTES;
    }

    /**
     * {@code LocalDateTime.minusDays}, {@code LocalDateTime.minusHours}, {@code LocalDateTime.minusMinutes}, {@code
     * LocalDateTime.minusMonths}, {@code LocalDateTime.minusNanos}, {@code LocalDateTime.minusSeconds} and 10 more.
     */
    public static long localDateTimeBytes(LocalDateTime receiver, long value) {
        return LOCAL_DATE_TIME_BYTES;
    }

    /** {@code LocalDateTime.of}. */
    public static long localDateTimeBytes(int value, int value2, int value3, int value4, int value5) {
        return LOCAL_DATE_TIME_BYTES;
    }

    /** {@code LocalDateTime.of}. */
    public static long localDateTimeBytes(int value, int value2, int value3, int value4, int value5, int value6) {
        return LOCAL_DATE_TIME_BYTES;
    }

    /** {@code LocalDateTime.of}. */
    public static long localDateTimeBytes(int value, int value2, int value3, int value4, int value5, int value6, int value7) {
        return LOCAL_DATE_TIME_BYTES;
    }

    /** {@code LocalDateTime.ofInstant}. */
    public static long localDateTimeBytes(Instant instant, ZoneId zoneId) {
        return LOCAL_DATE_TIME_BYTES;
    }

    /** {@code LocalDateTime.ofEpochSecond}. */
    public static long localDateTimeBytes(long value, int value2, ZoneOffset zoneOffset) {
        return LOCAL_DATE_TIME_BYTES;
    }

    /** {@code LocalDateTime.truncatedTo}. */
    public static long localDateTimeBytes(LocalDateTime receiver, TemporalUnit temporalUnit) {
        return LOCAL_DATE_TIME_BYTES;
    }

    /** {@code LocalDateTime.with}. */
    public static long localDateTimeBytes(LocalDateTime receiver, TemporalAdjuster temporalAdjuster) {
        return LOCAL_DATE_TIME_BYTES;
    }

    /** {@code LocalDateTime.with}. */
    public static long localDateTimeBytes(LocalDateTime receiver, TemporalField temporalField, long value) {
        return LOCAL_DATE_TIME_BYTES;
    }

    /**
     * {@code LocalDateTime.withDayOfMonth}, {@code LocalDateTime.withDayOfYear}, {@code LocalDateTime.withHour}, {@code
     * LocalDateTime.withMinute}, {@code LocalDateTime.withMonth}, {@code LocalDateTime.withSecond} and 1 more.
     */
    public static long localDateTimeBytes(LocalDateTime receiver, int value) {
        return LOCAL_DATE_TIME_BYTES;
    }

    /** {@code LocalTime.atDate}. */
    public static long localDateTimeBytes(LocalTime receiver, LocalDate localDate) {
        return LOCAL_DATE_TIME_BYTES;
    }

    // ---- OffsetDateTime chains, 112 bytes. ----

    /** {@code LocalDateTime.atOffset}, {@code OffsetDateTime.of}. */
    public static long offsetDateTimeBytes(LocalDateTime receiver, ZoneOffset zoneOffset) {
        return OFFSET_DATE_TIME_BYTES;
    }

    /** {@code OffsetDateTime.from}. */
    public static long offsetDateTimeBytes(TemporalAccessor temporalAccessor) {
        return OFFSET_DATE_TIME_BYTES;
    }

    /** {@code OffsetDateTime.minus}, {@code OffsetDateTime.plus}. */
    public static long offsetDateTimeBytes(OffsetDateTime receiver, TemporalAmount temporalAmount) {
        return OFFSET_DATE_TIME_BYTES;
    }

    /** {@code OffsetDateTime.minus}, {@code OffsetDateTime.plus}. */
    public static long offsetDateTimeBytes(OffsetDateTime receiver, long value, TemporalUnit temporalUnit) {
        return OFFSET_DATE_TIME_BYTES;
    }

    /**
     * {@code OffsetDateTime.minusYears}, {@code OffsetDateTime.minusMonths}, {@code OffsetDateTime.minusWeeks}, {@code
     * OffsetDateTime.minusDays}, {@code OffsetDateTime.minusHours}, {@code OffsetDateTime.minusMinutes} and 10 more.
     */
    public static long offsetDateTimeBytes(OffsetDateTime receiver, long value) {
        return OFFSET_DATE_TIME_BYTES;
    }

    /** {@code OffsetDateTime.of}. */
    public static long offsetDateTimeBytes(LocalDate localDate, LocalTime localTime, ZoneOffset zoneOffset) {
        return OFFSET_DATE_TIME_BYTES;
    }

    /** {@code OffsetDateTime.of}. */
    public static long offsetDateTimeBytes(
        int value,
        int value2,
        int value3,
        int value4,
        int value5,
        int value6,
        int value7,
        ZoneOffset zoneOffset
    ) {
        return OFFSET_DATE_TIME_BYTES;
    }

    /** {@code OffsetDateTime.ofInstant}. */
    public static long offsetDateTimeBytes(Instant instant, ZoneId zoneId) {
        return OFFSET_DATE_TIME_BYTES;
    }

    /** {@code OffsetDateTime.truncatedTo}. */
    public static long offsetDateTimeBytes(OffsetDateTime receiver, TemporalUnit temporalUnit) {
        return OFFSET_DATE_TIME_BYTES;
    }

    /** {@code OffsetDateTime.with}. */
    public static long offsetDateTimeBytes(OffsetDateTime receiver, TemporalAdjuster temporalAdjuster) {
        return OFFSET_DATE_TIME_BYTES;
    }

    /** {@code OffsetDateTime.with}. */
    public static long offsetDateTimeBytes(OffsetDateTime receiver, TemporalField temporalField, long value) {
        return OFFSET_DATE_TIME_BYTES;
    }

    /**
     * {@code OffsetDateTime.withDayOfMonth}, {@code OffsetDateTime.withDayOfYear}, {@code OffsetDateTime.withHour}, {@code
     * OffsetDateTime.withMinute}, {@code OffsetDateTime.withMonth}, {@code OffsetDateTime.withNano} and 2 more.
     */
    public static long offsetDateTimeBytes(OffsetDateTime receiver, int value) {
        return OFFSET_DATE_TIME_BYTES;
    }

    /** {@code OffsetDateTime.withOffsetSameLocal}, {@code OffsetDateTime.withOffsetSameInstant}. */
    public static long offsetDateTimeBytes(OffsetDateTime receiver, ZoneOffset zoneOffset) {
        return OFFSET_DATE_TIME_BYTES;
    }

    // ---- OffsetTime chains, 48 bytes. ----

    /** {@code LocalTime.atOffset}, {@code OffsetTime.of}. */
    public static long offsetTimeBytes(LocalTime receiver, ZoneOffset zoneOffset) {
        return OFFSET_TIME_BYTES;
    }

    /** {@code OffsetDateTime.toOffsetTime}. */
    public static long offsetTimeBytes(OffsetDateTime receiver) {
        return OFFSET_TIME_BYTES;
    }

    /** {@code OffsetTime.from}. */
    public static long offsetTimeBytes(TemporalAccessor temporalAccessor) {
        return OFFSET_TIME_BYTES;
    }

    /** {@code OffsetTime.of}. */
    public static long offsetTimeBytes(int value, int value2, int value3, int value4, ZoneOffset zoneOffset) {
        return OFFSET_TIME_BYTES;
    }

    /** {@code OffsetTime.ofInstant}. */
    public static long offsetTimeBytes(Instant instant, ZoneId zoneId) {
        return OFFSET_TIME_BYTES;
    }

    /** {@code OffsetTime.plus}, {@code OffsetTime.minus}. */
    public static long offsetTimeBytes(OffsetTime receiver, TemporalAmount temporalAmount) {
        return OFFSET_TIME_BYTES;
    }

    /** {@code OffsetTime.plus}, {@code OffsetTime.minus}. */
    public static long offsetTimeBytes(OffsetTime receiver, long value, TemporalUnit temporalUnit) {
        return OFFSET_TIME_BYTES;
    }

    /**
     * {@code OffsetTime.plusHours}, {@code OffsetTime.plusMinutes}, {@code OffsetTime.plusSeconds}, {@code OffsetTime.plusNanos},
     * {@code OffsetTime.minusHours}, {@code OffsetTime.minusMinutes} and 2 more.
     */
    public static long offsetTimeBytes(OffsetTime receiver, long value) {
        return OFFSET_TIME_BYTES;
    }

    /** {@code OffsetTime.truncatedTo}. */
    public static long offsetTimeBytes(OffsetTime receiver, TemporalUnit temporalUnit) {
        return OFFSET_TIME_BYTES;
    }

    /** {@code OffsetTime.with}. */
    public static long offsetTimeBytes(OffsetTime receiver, TemporalAdjuster temporalAdjuster) {
        return OFFSET_TIME_BYTES;
    }

    /** {@code OffsetTime.with}. */
    public static long offsetTimeBytes(OffsetTime receiver, TemporalField temporalField, long value) {
        return OFFSET_TIME_BYTES;
    }

    /** {@code OffsetTime.withHour}, {@code OffsetTime.withMinute}, {@code OffsetTime.withNano}, {@code OffsetTime.withSecond}. */
    public static long offsetTimeBytes(OffsetTime receiver, int value) {
        return OFFSET_TIME_BYTES;
    }

    /** {@code OffsetTime.withOffsetSameLocal}, {@code OffsetTime.withOffsetSameInstant}. */
    public static long offsetTimeBytes(OffsetTime receiver, ZoneOffset zoneOffset) {
        return OFFSET_TIME_BYTES;
    }

    // ---- ZonedDateTime chains, 120 bytes. ----

    /** {@code LocalDate.atStartOfDay}. */
    public static long zonedDateTimeBytes(LocalDate receiver, ZoneId zoneId) {
        return ZONED_DATE_TIME_BYTES;
    }

    /** {@code OffsetDateTime.atZoneSameInstant}, {@code OffsetDateTime.atZoneSimilarLocal}. */
    public static long zonedDateTimeBytes(OffsetDateTime receiver, ZoneId zoneId) {
        return ZONED_DATE_TIME_BYTES;
    }

    /** {@code OffsetDateTime.toZonedDateTime}. */
    public static long zonedDateTimeBytes(OffsetDateTime receiver) {
        return ZONED_DATE_TIME_BYTES;
    }

    // ---- Interface calls: the result type is unknown, so the biggest chain is charged. ----

    /** {@code Temporal.minus}, {@code Temporal.plus}. */
    public static long temporalBytes(Temporal receiver, long value, TemporalUnit temporalUnit) {
        return ZONED_DATE_TIME_BYTES;
    }

    /** {@code Temporal.minus}, {@code Temporal.plus}. */
    public static long temporalBytes(Temporal receiver, TemporalAmount temporalAmount) {
        return ZONED_DATE_TIME_BYTES;
    }

    /** {@code Temporal.with}. */
    public static long temporalBytes(Temporal receiver, TemporalAdjuster temporalAdjuster) {
        return ZONED_DATE_TIME_BYTES;
    }

    /** {@code Temporal.with}. */
    public static long temporalBytes(Temporal receiver, TemporalField temporalField, long value) {
        return ZONED_DATE_TIME_BYTES;
    }

    /** {@code TemporalAdjuster.adjustInto}. */
    public static long temporalBytes(TemporalAdjuster receiver, Temporal temporal) {
        return ZONED_DATE_TIME_BYTES;
    }

    /** {@code TemporalAmount.addTo}, {@code TemporalAmount.subtractFrom}. */
    public static long temporalBytes(TemporalAmount receiver, Temporal temporal) {
        return ZONED_DATE_TIME_BYTES;
    }

    /** {@code TemporalField.adjustInto}. */
    public static long temporalBytes(TemporalField receiver, Temporal temporal, long value) {
        return ZONED_DATE_TIME_BYTES;
    }

    /** {@code TemporalUnit.addTo}. */
    public static long temporalBytes(TemporalUnit receiver, Temporal temporal, long value) {
        return ZONED_DATE_TIME_BYTES;
    }

    // ---- ValueRange, 48 bytes. ----

    /** {@code ValueRange.of}. */
    public static long valueRangeBytes(long value, long value2) {
        return VALUE_RANGE_BYTES;
    }

    /** {@code ValueRange.of}. */
    public static long valueRangeBytes(long value, long value2, long value3) {
        return VALUE_RANGE_BYTES;
    }

    /** {@code ValueRange.of}. */
    public static long valueRangeBytes(long value, long value2, long value3, long value4) {
        return VALUE_RANGE_BYTES;
    }

    // ---- Parse: the parse state plus the result. ----

    /** {@code LocalDate.parse}, {@code LocalTime.parse}, {@code MonthDay.parse}, {@code Year.parse}, {@code YearMonth.parse}. */
    public static long parseFlatValueBytes(CharSequence text, DateTimeFormatter dateTimeFormatter) {
        return AllocSizes.addSat(parseTextBytes(text), FLAT_VALUE_BYTES);
    }

    /** {@code LocalDateTime.parse}. */
    public static long parseLocalDateTimeBytes(CharSequence text) {
        return AllocSizes.addSat(parseTextBytes(text), LOCAL_DATE_TIME_BYTES);
    }

    /** {@code LocalDateTime.parse}. */
    public static long parseLocalDateTimeBytes(CharSequence text, DateTimeFormatter dateTimeFormatter) {
        return AllocSizes.addSat(parseTextBytes(text), LOCAL_DATE_TIME_BYTES);
    }

    /** {@code OffsetDateTime.parse}. */
    public static long parseOffsetDateTimeBytes(CharSequence text) {
        return AllocSizes.addSat(parseTextBytes(text), OFFSET_DATE_TIME_BYTES);
    }

    /** {@code OffsetDateTime.parse}. */
    public static long parseOffsetDateTimeBytes(CharSequence text, DateTimeFormatter dateTimeFormatter) {
        return AllocSizes.addSat(parseTextBytes(text), OFFSET_DATE_TIME_BYTES);
    }

    /** {@code OffsetTime.parse}. */
    public static long parseOffsetTimeBytes(CharSequence text) {
        return AllocSizes.addSat(parseTextBytes(text), OFFSET_TIME_BYTES);
    }

    /** {@code OffsetTime.parse}. */
    public static long parseOffsetTimeBytes(CharSequence text, DateTimeFormatter dateTimeFormatter) {
        return AllocSizes.addSat(parseTextBytes(text), OFFSET_TIME_BYTES);
    }

    // ---- Format: a bounded new string. ----

    /** {@code LocalTime.format}. */
    public static long formatBytes(LocalTime receiver, DateTimeFormatter dateTimeFormatter) {
        return newStringBytes(FORMAT_CHARACTERS);
    }

    /** {@code MonthDay.format}. */
    public static long formatBytes(MonthDay receiver, DateTimeFormatter dateTimeFormatter) {
        return newStringBytes(FORMAT_CHARACTERS);
    }

    /** {@code OffsetDateTime.format}. */
    public static long formatBytes(OffsetDateTime receiver, DateTimeFormatter dateTimeFormatter) {
        return newStringBytes(FORMAT_CHARACTERS);
    }

    /** {@code OffsetTime.format}. */
    public static long formatBytes(OffsetTime receiver, DateTimeFormatter dateTimeFormatter) {
        return newStringBytes(FORMAT_CHARACTERS);
    }

    /** {@code Year.format}. */
    public static long formatBytes(Year receiver, DateTimeFormatter dateTimeFormatter) {
        return newStringBytes(FORMAT_CHARACTERS);
    }

    /** {@code YearMonth.format}. */
    public static long formatBytes(YearMonth receiver, DateTimeFormatter dateTimeFormatter) {
        return newStringBytes(FORMAT_CHARACTERS);
    }

    /** {@code DateTimeFormatter.formatTo}. */
    public static long formatBytes(DateTimeFormatter receiver, TemporalAccessor temporalAccessor, Appendable appendable) {
        return newStringBytes(FORMAT_CHARACTERS);
    }

    // ---- Formatter parse state. ----

    /** {@code DateTimeFormatter.parseBest}. */
    public static long formatterParseBytes(DateTimeFormatter receiver, CharSequence text, TemporalQuery<?>[] temporalQuery) {
        return parseTextBytes(text);
    }

    /** {@code DateTimeFormatter.parseUnresolved}. */
    public static long formatterParseBytes(DateTimeFormatter receiver, CharSequence text, ParsePosition position) {
        return parseTextBytes(text);
    }

    // ---- Formatter copies. ----

    /** {@code DateTimeFormatter.withChronology}. */
    public static long formatterCopyBytes(DateTimeFormatter receiver, Chronology chronology) {
        return FORMATTER_BYTES;
    }

    /** {@code DateTimeFormatter.withDecimalStyle}. */
    public static long formatterCopyBytes(DateTimeFormatter receiver, DecimalStyle decimalStyle) {
        return FORMATTER_BYTES;
    }

    /** {@code DateTimeFormatter.withLocale}. */
    public static long formatterCopyBytes(DateTimeFormatter receiver, Locale locale) {
        return FORMATTER_BYTES;
    }

    /** {@code DateTimeFormatter.withResolverFields}. */
    public static long formatterCopyBytes(DateTimeFormatter receiver, Set<?> set) {
        return FORMATTER_BYTES;
    }

    /** {@code DateTimeFormatter.withResolverStyle}. */
    public static long formatterCopyBytes(DateTimeFormatter receiver, ResolverStyle resolverStyle) {
        return FORMATTER_BYTES;
    }

    /** {@code DateTimeFormatter.withZone}. */
    public static long formatterCopyBytes(DateTimeFormatter receiver, ZoneId zoneId) {
        return FORMATTER_BYTES;
    }

    // ---- Localized formatters are built from a pattern each call. ----

    /**
     * {@code DateTimeFormatter.ofLocalizedDate}, {@code DateTimeFormatter.ofLocalizedDateTime}, {@code
     * DateTimeFormatter.ofLocalizedTime}.
     */
    public static long localizedFormatterBytes(FormatStyle formatStyle) {
        return PATTERN_BASE_BYTES;
    }

    /** {@code DateTimeFormatter.ofLocalizedDateTime}. */
    public static long localizedFormatterBytes(FormatStyle formatStyle, FormatStyle formatStyle2) {
        return PATTERN_BASE_BYTES;
    }

    // ---- Format adapters. ----

    /** {@code DateTimeFormatter.toFormat}. */
    public static long toFormatBytes(DateTimeFormatter receiver) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code DateTimeFormatter.toFormat}. */
    public static long toFormatBytes(DateTimeFormatter receiver, TemporalQuery<?> temporalQuery) {
        return FLAT_VALUE_BYTES;
    }

    // ---- Pattern text. ----

    /** {@code DateTimeFormatterBuilder.getLocalizedDateTimePattern}. */
    public static long patternTextBytes(FormatStyle formatStyle, FormatStyle formatStyle2, Chronology chronology, Locale locale) {
        return newStringBytes(PATTERN_TEXT_CHARACTERS);
    }

    // ---- Formatter builder: the builder, one step per appended element, and the built formatter. ----

    /** {@code new DateTimeFormatterBuilder}. */
    public static long formatterBuilderBytes() {
        return BUILDER_BYTES;
    }

    /** {@code DateTimeFormatterBuilder.append}, {@code DateTimeFormatterBuilder.appendOptional}. */
    public static long builderStepBytes(DateTimeFormatterBuilder receiver, DateTimeFormatter dateTimeFormatter) {
        return PATTERN_BYTES_PER_CHARACTER;
    }

    /**
     * {@code DateTimeFormatterBuilder.appendChronologyId}, {@code DateTimeFormatterBuilder.appendInstant}, {@code
     * DateTimeFormatterBuilder.appendOffsetId}, {@code DateTimeFormatterBuilder.appendZoneId}, {@code
     * DateTimeFormatterBuilder.appendZoneOrOffsetId}, {@code DateTimeFormatterBuilder.appendZoneRegionId} and 6 more.
     */
    public static long builderStepBytes(DateTimeFormatterBuilder receiver) {
        return PATTERN_BYTES_PER_CHARACTER;
    }

    /**
     * {@code DateTimeFormatterBuilder.appendChronologyText}, {@code DateTimeFormatterBuilder.appendLocalizedOffset}, {@code
     * DateTimeFormatterBuilder.appendZoneText}.
     */
    public static long builderStepBytes(DateTimeFormatterBuilder receiver, TextStyle textStyle) {
        return PATTERN_BYTES_PER_CHARACTER;
    }

    /** {@code DateTimeFormatterBuilder.appendFraction}. */
    public static long builderStepBytes(
        DateTimeFormatterBuilder receiver,
        TemporalField temporalField,
        int value,
        int value2,
        boolean flag
    ) {
        return PATTERN_BYTES_PER_CHARACTER;
    }

    /** {@code DateTimeFormatterBuilder.appendInstant}, {@code DateTimeFormatterBuilder.padNext}. */
    public static long builderStepBytes(DateTimeFormatterBuilder receiver, int value) {
        return PATTERN_BYTES_PER_CHARACTER;
    }

    /** {@code DateTimeFormatterBuilder.appendLiteral}. */
    public static long builderStepBytes(DateTimeFormatterBuilder receiver, String text) {
        return PATTERN_BYTES_PER_CHARACTER;
    }

    /** {@code DateTimeFormatterBuilder.appendLocalized}. */
    public static long builderStepBytes(DateTimeFormatterBuilder receiver, FormatStyle formatStyle, FormatStyle formatStyle2) {
        return PATTERN_BYTES_PER_CHARACTER;
    }

    /** {@code DateTimeFormatterBuilder.appendOffset}. */
    public static long builderStepBytes(DateTimeFormatterBuilder receiver, String text, String text2) {
        return PATTERN_BYTES_PER_CHARACTER;
    }

    /** {@code DateTimeFormatterBuilder.appendText}, {@code DateTimeFormatterBuilder.appendValue}. */
    public static long builderStepBytes(DateTimeFormatterBuilder receiver, TemporalField temporalField) {
        return PATTERN_BYTES_PER_CHARACTER;
    }

    /** {@code DateTimeFormatterBuilder.appendText}. */
    public static long builderStepBytes(DateTimeFormatterBuilder receiver, TemporalField temporalField, TextStyle textStyle) {
        return PATTERN_BYTES_PER_CHARACTER;
    }

    /** {@code DateTimeFormatterBuilder.appendValue}. */
    public static long builderStepBytes(DateTimeFormatterBuilder receiver, TemporalField temporalField, int value) {
        return PATTERN_BYTES_PER_CHARACTER;
    }

    /** {@code DateTimeFormatterBuilder.appendValue}. */
    public static long builderStepBytes(
        DateTimeFormatterBuilder receiver,
        TemporalField temporalField,
        int value,
        int value2,
        SignStyle signStyle
    ) {
        return PATTERN_BYTES_PER_CHARACTER;
    }

    /** {@code DateTimeFormatterBuilder.appendValueReduced}. */
    public static long builderStepBytes(DateTimeFormatterBuilder receiver, TemporalField temporalField, int value, int value2, int value3) {
        return PATTERN_BYTES_PER_CHARACTER;
    }

    /** {@code DateTimeFormatterBuilder.appendZoneText}. */
    public static long builderStepBytes(DateTimeFormatterBuilder receiver, TextStyle textStyle, Set<?> set) {
        return PATTERN_BYTES_PER_CHARACTER;
    }

    /** {@code DateTimeFormatterBuilder.padNext}. */
    public static long builderStepBytes(DateTimeFormatterBuilder receiver, int value, char character) {
        return PATTERN_BYTES_PER_CHARACTER;
    }

    /** {@code DateTimeFormatterBuilder.parseDefaulting}. */
    public static long builderStepBytes(DateTimeFormatterBuilder receiver, TemporalField temporalField, long value) {
        return PATTERN_BYTES_PER_CHARACTER;
    }

    /** {@code DateTimeFormatterBuilder.appendPattern}. */
    public static long patternStepBytes(DateTimeFormatterBuilder receiver, String pattern) {
        return AllocSizes.addSat(PATTERN_BYTES_PER_CHARACTER, AllocSizes.mulSat(PATTERN_BYTES_PER_CHARACTER, textLength(pattern)));
    }

    /** {@code DateTimeFormatterBuilder.toFormatter}. */
    public static long toFormatterBytes(DateTimeFormatterBuilder receiver) {
        return PATTERN_BASE_BYTES;
    }

    /** {@code DateTimeFormatterBuilder.toFormatter}. */
    public static long toFormatterBytes(DateTimeFormatterBuilder receiver, Locale locale) {
        return PATTERN_BASE_BYTES;
    }

    // ---- Display names: a short new string. ----

    /** {@code ZoneId.getDisplayName}. */
    public static long displayNameBytes(ZoneId receiver, TextStyle textStyle, Locale locale) {
        return newStringBytes(DISPLAY_NAME_CHARACTERS);
    }

    /** {@code DayOfWeek.getDisplayName}. */
    public static long displayNameBytes(DayOfWeek receiver, TextStyle textStyle, Locale locale) {
        return newStringBytes(DISPLAY_NAME_CHARACTERS);
    }

    /** {@code Month.getDisplayName}. */
    public static long displayNameBytes(Month receiver, TextStyle textStyle, Locale locale) {
        return newStringBytes(DISPLAY_NAME_CHARACTERS);
    }
}
