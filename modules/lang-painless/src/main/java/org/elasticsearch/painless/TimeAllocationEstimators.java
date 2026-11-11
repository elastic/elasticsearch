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
import java.time.Clock;
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
import java.time.chrono.ChronoLocalDateTime;
import java.time.chrono.ChronoPeriod;
import java.time.chrono.ChronoZonedDateTime;
import java.time.chrono.Chronology;
import java.time.chrono.Era;
import java.time.chrono.HijrahChronology;
import java.time.chrono.HijrahDate;
import java.time.chrono.IsoChronology;
import java.time.chrono.JapaneseChronology;
import java.time.chrono.JapaneseDate;
import java.time.chrono.MinguoChronology;
import java.time.chrono.MinguoDate;
import java.time.chrono.ThaiBuddhistChronology;
import java.time.chrono.ThaiBuddhistDate;
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
 * <p>A few fixed sizes cover almost everything. A flat value is one object with only primitive fields. A composite is an
 * object that points at other new values, so the whole chain is charged. Text members are sized from the text. A call that
 * returns a cached object, like {@code Instant.EPOCH}, is still charged in full, because we cannot know that before the call.
 *
 * <p>The receiver parameter is the class that declares the Java method, which for an inherited method is the supertype, not
 * the allowlisted class.
 */
public final class TimeAllocationEstimators {

    private TimeAllocationEstimators() {}

    /** Size of an object that holds only {@code references} references. */
    private static long shellBytes(int references) {
        return AllocSizes.pad8(AllocSizes.OBJECT_HEADER + (long) references * AllocSizes.REFERENCE_SIZE);
    }

    /**
     * A flat value: one object with only primitive fields. The biggest is a {@code long} plus an {@code int}, which pads to
     * 24 bytes. Covers {@link Instant}, {@link java.time.Duration}, {@link LocalDate}, {@link LocalTime}, {@link Period},
     * {@link Year}, {@link YearMonth}, {@link MonthDay} and {@link DecimalStyle}.
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

    /** An {@link OffsetTime}: its own object with two references, plus a new {@code LocalTime}. Offsets are cached. 56 bytes. */
    public static final long OFFSET_TIME_BYTES = shellBytes(2) + FLAT_VALUE_BYTES;

    /**
     * An object holding two references and nothing new behind them. 32 bytes. Covers a {@link ZoneId} region, a fixed
     * {@link Clock}, a {@code Format} adapter and a {@code ChronoPeriod}.
     */
    public static final long TWO_REFERENCE_BYTES = shellBytes(2);

    /**
     * A date from a non-ISO chronology: its own object with up to two references, plus the ISO date most of them wrap.
     * 56 bytes.
     */
    public static final long CHRONO_DATE_BYTES = shellBytes(2) + FLAT_VALUE_BYTES;

    /** A date-time from a non-ISO chronology: its own object, the chrono date and a new {@code LocalTime}. 112 bytes. */
    public static final long CHRONO_LOCAL_DATE_TIME_BYTES = shellBytes(2) + CHRONO_DATE_BYTES + FLAT_VALUE_BYTES;

    /** A zoned date-time from a non-ISO chronology: its own object with three references plus the chrono date-time. 152 bytes. */
    public static final long CHRONO_ZONED_DATE_TIME_BYTES = shellBytes(3) + CHRONO_LOCAL_DATE_TIME_BYTES;

    /** A {@link java.time.temporal.ValueRange}: four longs. 48 bytes. */
    public static final long VALUE_RANGE_BYTES = AllocSizes.pad8(AllocSizes.OBJECT_HEADER + 4L * Long.BYTES);

    /** A {@link DateTimeFormatter} copy from a {@code with} method: eight references. 80 bytes. */
    public static final long FORMATTER_BYTES = shellBytes(8);

    /** A new {@link java.time.format.DateTimeFormatterBuilder}: the builder and its element list. */
    private static final long BUILDER_BYTES = 128;

    /** Characters allowed for a display name. The longest zone names are about 40. */
    private static final long DISPLAY_NAME_CHARACTERS = 64;

    /** {@code ZoneId.getAvailableZoneIds()} copies about 600 ids into a new set: a hash entry and a table slot each. */
    private static final long AVAILABLE_ZONE_IDS_BYTES = 640L * 64;

    /** {@code Chronology.getAvailableChronologies()}: a new set of the few chronologies plus a service loader pass. */
    private static final long AVAILABLE_CHRONOLOGIES_BYTES = 1024;

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

    /** A formatter built from a localized pattern on each call: a builder, one step and the formatter. */
    private static final long LOCALIZED_FORMATTER_BYTES = BUILDER_BYTES + PATTERN_BYTES_PER_CHARACTER + PATTERN_BASE_BYTES;

    /** A new {@link String} of {@code chars} characters: the object plus its array. */
    private static long newStringBytes(long chars) {
        return AllocSizes.addSat(AllocSizes.STRING_CONCAT_RESULT_OVERHEAD, AllocSizes.mulSat(2L, Math.max(0L, chars)));
    }

    /** Length of {@code text}. A {@code null} counts as empty; the real call rejects it. */
    private static long textLength(@Nullable CharSequence text) {
        return text == null ? 0 : text.length();
    }

    // ---- Flat values, 24 bytes. ----

    /**
     * {@code Instant.from}, {@code LocalDate.from}, {@code LocalTime.from}, {@code MonthDay.from}, {@code Year.from},
     * {@code YearMonth.from}.
     */
    public static long flatValueBytes(TemporalAccessor temporal) {
        return FLAT_VALUE_BYTES;
    }

    /**
     * {@code Duration.ofDays}, {@code Duration.ofHours}, {@code Duration.ofMillis}, {@code Duration.ofMinutes}, {@code
     * Duration.ofNanos}, {@code Duration.ofSeconds}, {@code Instant.ofEpochSecond}, {@code Instant.ofEpochMilli},
     * {@code LocalDate.ofEpochDay}, {@code LocalTime.ofNanoOfDay}, {@code LocalTime.ofSecondOfDay}.
     */
    public static long flatValueBytes(long value) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code Duration.ofSeconds}, {@code Instant.ofEpochSecond}. */
    public static long flatValueBytes(long epochSecond, long nanoAdjustment) {
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
    public static long flatValueBytes(Instant receiver, TemporalField field, long newValue) {
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

    /** {@code LocalDateTime.atZone}, {@code ZonedDateTime.of}. */
    public static long zonedDateTimeBytes(LocalDateTime dateTime, ZoneId zone) {
        return ZONED_DATE_TIME_BYTES;
    }

    /** {@code ZonedDateTime.ofInstant}, {@code ZonedDateTime.ofStrict}. */
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

    /** {@code ZonedDateTime.minus}, {@code ZonedDateTime.plus}. */
    public static long zonedDateTimeBytes(ZonedDateTime receiver, TemporalAmount amount) {
        return ZONED_DATE_TIME_BYTES;
    }

    /** {@code ZonedDateTime.minus}, {@code ZonedDateTime.plus}. */
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
    public static long zonedDateTimeBytes(ZonedDateTime receiver, TemporalField field, long newValue) {
        return ZONED_DATE_TIME_BYTES;
    }

    /** {@code ZonedDateTime.withZoneSameLocal}, {@code ZonedDateTime.withZoneSameInstant}. */
    public static long zonedDateTimeBytes(ZonedDateTime receiver, ZoneId zone) {
        return ZONED_DATE_TIME_BYTES;
    }

    // ---- Text: parse state and new strings, sized from the text when we have it. ----

    /**
     * {@code Instant.parse(CharSequence)}, and the same form on {@code Duration}, {@code LocalDate}, {@code LocalTime},
     * {@code MonthDay}, {@code Period}, {@code Year} and {@code YearMonth}: the parse state plus the flat value.
     */
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

    // ---- Flat values, 24 bytes: the rest of java.time. ----

    /** {@code Clock.fixed}: a clock holding an instant and a zone. */
    public static long clockBytes(Instant instant, ZoneId zone) {
        return TWO_REFERENCE_BYTES;
    }

    /** {@code Clock.instant}. */
    public static long flatValueBytes(Clock receiver) {
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
     * {@code Duration.dividedBy}, {@code Duration.minusDays}, {@code Duration.minusHours}, {@code
     * Duration.minusMinutes}, {@code Duration.minusSeconds}, {@code Duration.minusMillis}, {@code
     * Duration.minusNanos}, {@code Duration.multipliedBy}, {@code Duration.plusDays}, {@code Duration.plusHours},
     * {@code Duration.plusMinutes}, {@code Duration.plusSeconds}, {@code Duration.plusMillis}, {@code
     * Duration.plusNanos}, {@code Duration.withSeconds}.
     */
    public static long flatValueBytes(Duration receiver, long amount) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code Duration.from}, {@code Period.from}. */
    public static long flatValueBytes(TemporalAmount amount) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code Duration.minus}, {@code Duration.plus}. */
    public static long flatValueBytes(Duration receiver, Duration duration) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code Duration.minus}, {@code Duration.plus}. */
    public static long flatValueBytes(Duration receiver, long amount, TemporalUnit unit) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code Duration.of}. */
    public static long flatValueBytes(long amount, TemporalUnit unit) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code Duration.withNanos}. */
    public static long flatValueBytes(Duration receiver, int nanoOfSecond) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code LocalDate.minus}, {@code LocalDate.plus}. */
    public static long flatValueBytes(LocalDate receiver, TemporalAmount amount) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code LocalDate.minus}, {@code LocalDate.plus}. */
    public static long flatValueBytes(LocalDate receiver, long amount, TemporalUnit unit) {
        return FLAT_VALUE_BYTES;
    }

    /**
     * {@code LocalDate.minusYears}, {@code LocalDate.minusMonths}, {@code LocalDate.minusWeeks}, {@code
     * LocalDate.minusDays}, {@code LocalDate.plusYears}, {@code LocalDate.plusMonths}, {@code LocalDate.plusWeeks},
     * {@code LocalDate.plusDays}.
     */
    public static long flatValueBytes(LocalDate receiver, long amount) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code LocalDate.of}, {@code LocalTime.of}, {@code Period.of}, {@code ZoneOffset.ofHoursMinutesSeconds}. */
    public static long flatValueBytes(int a, int b, int c) {
        return FLAT_VALUE_BYTES;
    }

    /**
     * {@code LocalDate.ofYearDay}, {@code LocalTime.of}, {@code MonthDay.of}, {@code YearMonth.of}, {@code
     * ZoneOffset.ofHoursMinutes}.
     */
    public static long flatValueBytes(int a, int b) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code LocalDate.until}. */
    public static long flatValueBytes(LocalDate receiver, ChronoLocalDate other) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code LocalDate.with}. */
    public static long flatValueBytes(LocalDate receiver, TemporalAdjuster adjuster) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code LocalDate.with}. */
    public static long flatValueBytes(LocalDate receiver, TemporalField field, long newValue) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code LocalDate.withDayOfMonth}, {@code LocalDate.withDayOfYear}, {@code LocalDate.withMonth}, {@code LocalDate.withYear}. */
    public static long flatValueBytes(LocalDate receiver, int value) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code LocalTime.minus}, {@code LocalTime.plus}. */
    public static long flatValueBytes(LocalTime receiver, TemporalAmount amount) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code LocalTime.minus}, {@code LocalTime.plus}. */
    public static long flatValueBytes(LocalTime receiver, long amount, TemporalUnit unit) {
        return FLAT_VALUE_BYTES;
    }

    /**
     * {@code LocalTime.minusHours}, {@code LocalTime.minusMinutes}, {@code LocalTime.minusNanos}, {@code
     * LocalTime.minusSeconds}, {@code LocalTime.plusHours}, {@code LocalTime.plusMinutes}, {@code
     * LocalTime.plusNanos}, {@code LocalTime.plusSeconds}.
     */
    public static long flatValueBytes(LocalTime receiver, long amount) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code LocalTime.of}. */
    public static long flatValueBytes(int hour, int minute, int second, int nanoOfSecond) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code LocalTime.truncatedTo}. */
    public static long flatValueBytes(LocalTime receiver, TemporalUnit unit) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code LocalTime.with}. */
    public static long flatValueBytes(LocalTime receiver, TemporalAdjuster adjuster) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code LocalTime.with}. */
    public static long flatValueBytes(LocalTime receiver, TemporalField field, long newValue) {
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
    public static long flatValueBytes(LocalDate date, LocalDate date2) {
        return FLAT_VALUE_BYTES;
    }

    /**
     * {@code Period.ofYears}, {@code Period.ofMonths}, {@code Period.ofWeeks}, {@code Period.ofDays}, {@code Year.of},
     * {@code ZoneOffset.ofHours}, {@code ZoneOffset.ofTotalSeconds}.
     */
    public static long flatValueBytes(int value) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code Period.plus}, {@code Period.minus}. */
    public static long flatValueBytes(Period receiver, TemporalAmount amount) {
        return FLAT_VALUE_BYTES;
    }

    /**
     * {@code Period.plusYears}, {@code Period.plusMonths}, {@code Period.plusDays}, {@code Period.minusYears}, {@code
     * Period.minusMonths}, {@code Period.minusDays}.
     */
    public static long flatValueBytes(Period receiver, long amount) {
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
    public static long flatValueBytes(Year receiver, TemporalAmount amount) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code Year.minus}, {@code Year.plus}. */
    public static long flatValueBytes(Year receiver, long amount, TemporalUnit unit) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code Year.minusYears}, {@code Year.plusYears}. */
    public static long flatValueBytes(Year receiver, long amount) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code Year.with}. */
    public static long flatValueBytes(Year receiver, TemporalAdjuster adjuster) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code Year.with}. */
    public static long flatValueBytes(Year receiver, TemporalField field, long newValue) {
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
    public static long flatValueBytes(YearMonth receiver, TemporalAmount amount) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code YearMonth.minus}, {@code YearMonth.plus}. */
    public static long flatValueBytes(YearMonth receiver, long amount, TemporalUnit unit) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code YearMonth.minusYears}, {@code YearMonth.minusMonths}, {@code YearMonth.plusYears}, {@code YearMonth.plusMonths}. */
    public static long flatValueBytes(YearMonth receiver, long amount) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code YearMonth.with}. */
    public static long flatValueBytes(YearMonth receiver, TemporalAdjuster adjuster) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code YearMonth.with}. */
    public static long flatValueBytes(YearMonth receiver, TemporalField field, long newValue) {
        return FLAT_VALUE_BYTES;
    }

    /**
     * {@code ZoneId.of}, {@code ZoneOffset.of}. A region id is a new two-reference object over cached rules. An offset is
     * cached unless it is not a multiple of fifteen minutes.
     */
    public static long zoneIdBytes(String text) {
        return TWO_REFERENCE_BYTES;
    }

    /** {@code ZoneId.of}. */
    public static long zoneIdBytes(String text, Map<?, ?> aliases) {
        return TWO_REFERENCE_BYTES;
    }

    /** {@code ZoneId.ofOffset}. */
    public static long zoneIdBytes(String prefix, ZoneOffset offset) {
        return TWO_REFERENCE_BYTES;
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
    public static long localDateTimeBytes(LocalDate receiver, LocalTime time) {
        return LOCAL_DATE_TIME_BYTES;
    }

    /** {@code LocalDate.atTime}. */
    public static long localDateTimeBytes(LocalDate receiver, int hour, int minute) {
        return LOCAL_DATE_TIME_BYTES;
    }

    /** {@code LocalDate.atTime}. */
    public static long localDateTimeBytes(LocalDate receiver, int hour, int minute, int second) {
        return LOCAL_DATE_TIME_BYTES;
    }

    /** {@code LocalDate.atTime}. */
    public static long localDateTimeBytes(LocalDate receiver, int hour, int minute, int second, int nanoOfSecond) {
        return LOCAL_DATE_TIME_BYTES;
    }

    /** {@code LocalDateTime.from}. */
    public static long localDateTimeBytes(TemporalAccessor temporal) {
        return LOCAL_DATE_TIME_BYTES;
    }

    /** {@code LocalDateTime.minus}, {@code LocalDateTime.plus}. */
    public static long localDateTimeBytes(LocalDateTime receiver, TemporalAmount amount) {
        return LOCAL_DATE_TIME_BYTES;
    }

    /** {@code LocalDateTime.minus}, {@code LocalDateTime.plus}. */
    public static long localDateTimeBytes(LocalDateTime receiver, long amount, TemporalUnit unit) {
        return LOCAL_DATE_TIME_BYTES;
    }

    /**
     * {@code LocalDateTime.minusDays}, {@code LocalDateTime.minusHours}, {@code LocalDateTime.minusMinutes}, {@code
     * LocalDateTime.minusMonths}, {@code LocalDateTime.minusNanos}, {@code LocalDateTime.minusSeconds}, {@code
     * LocalDateTime.minusWeeks}, {@code LocalDateTime.minusYears}, {@code LocalDateTime.plusDays}, {@code
     * LocalDateTime.plusHours}, {@code LocalDateTime.plusMinutes}, {@code LocalDateTime.plusMonths}, {@code
     * LocalDateTime.plusNanos}, {@code LocalDateTime.plusSeconds}, {@code LocalDateTime.plusWeeks}, {@code
     * LocalDateTime.plusYears}.
     */
    public static long localDateTimeBytes(LocalDateTime receiver, long amount) {
        return LOCAL_DATE_TIME_BYTES;
    }

    /** {@code LocalDateTime.of}. */
    public static long localDateTimeBytes(int year, int month, int dayOfMonth, int hour, int minute) {
        return LOCAL_DATE_TIME_BYTES;
    }

    /** {@code LocalDateTime.of}. */
    public static long localDateTimeBytes(int year, int month, int dayOfMonth, int hour, int minute, int second) {
        return LOCAL_DATE_TIME_BYTES;
    }

    /** {@code LocalDateTime.of}. */
    public static long localDateTimeBytes(int year, int month, int dayOfMonth, int hour, int minute, int second, int nanoOfSecond) {
        return LOCAL_DATE_TIME_BYTES;
    }

    /** {@code LocalDateTime.ofInstant}. */
    public static long localDateTimeBytes(Instant instant, ZoneId zone) {
        return LOCAL_DATE_TIME_BYTES;
    }

    /** {@code LocalDateTime.ofEpochSecond}. */
    public static long localDateTimeBytes(long epochSecond, int nanoOfSecond, ZoneOffset offset) {
        return LOCAL_DATE_TIME_BYTES;
    }

    /** {@code LocalDateTime.truncatedTo}. */
    public static long localDateTimeBytes(LocalDateTime receiver, TemporalUnit unit) {
        return LOCAL_DATE_TIME_BYTES;
    }

    /** {@code LocalDateTime.with}. */
    public static long localDateTimeBytes(LocalDateTime receiver, TemporalAdjuster adjuster) {
        return LOCAL_DATE_TIME_BYTES;
    }

    /** {@code LocalDateTime.with}. */
    public static long localDateTimeBytes(LocalDateTime receiver, TemporalField field, long newValue) {
        return LOCAL_DATE_TIME_BYTES;
    }

    /**
     * {@code LocalDateTime.withDayOfMonth}, {@code LocalDateTime.withDayOfYear}, {@code LocalDateTime.withHour},
     * {@code LocalDateTime.withMinute}, {@code LocalDateTime.withMonth}, {@code LocalDateTime.withSecond}, {@code
     * LocalDateTime.withYear}.
     */
    public static long localDateTimeBytes(LocalDateTime receiver, int value) {
        return LOCAL_DATE_TIME_BYTES;
    }

    /** {@code LocalTime.atDate}. */
    public static long localDateTimeBytes(LocalTime receiver, LocalDate date) {
        return LOCAL_DATE_TIME_BYTES;
    }

    // ---- OffsetDateTime chains, 112 bytes. ----

    /** {@code LocalDateTime.atOffset}, {@code OffsetDateTime.of}. */
    public static long offsetDateTimeBytes(LocalDateTime receiver, ZoneOffset offset) {
        return OFFSET_DATE_TIME_BYTES;
    }

    /** {@code OffsetDateTime.from}. */
    public static long offsetDateTimeBytes(TemporalAccessor temporal) {
        return OFFSET_DATE_TIME_BYTES;
    }

    /** {@code OffsetDateTime.minus}, {@code OffsetDateTime.plus}. */
    public static long offsetDateTimeBytes(OffsetDateTime receiver, TemporalAmount amount) {
        return OFFSET_DATE_TIME_BYTES;
    }

    /** {@code OffsetDateTime.minus}, {@code OffsetDateTime.plus}. */
    public static long offsetDateTimeBytes(OffsetDateTime receiver, long amount, TemporalUnit unit) {
        return OFFSET_DATE_TIME_BYTES;
    }

    /**
     * {@code OffsetDateTime.minusYears}, {@code OffsetDateTime.minusMonths}, {@code OffsetDateTime.minusWeeks}, {@code
     * OffsetDateTime.minusDays}, {@code OffsetDateTime.minusHours}, {@code OffsetDateTime.minusMinutes}, {@code
     * OffsetDateTime.minusSeconds}, {@code OffsetDateTime.minusNanos}, {@code OffsetDateTime.plusYears}, {@code
     * OffsetDateTime.plusMonths}, {@code OffsetDateTime.plusWeeks}, {@code OffsetDateTime.plusDays}, {@code
     * OffsetDateTime.plusHours}, {@code OffsetDateTime.plusMinutes}, {@code OffsetDateTime.plusSeconds}, {@code
     * OffsetDateTime.plusNanos}.
     */
    public static long offsetDateTimeBytes(OffsetDateTime receiver, long amount) {
        return OFFSET_DATE_TIME_BYTES;
    }

    /** {@code OffsetDateTime.of}. */
    public static long offsetDateTimeBytes(LocalDate date, LocalTime time, ZoneOffset offset) {
        return OFFSET_DATE_TIME_BYTES;
    }

    /** {@code OffsetDateTime.of}. */
    public static long offsetDateTimeBytes(
        int year,
        int month,
        int dayOfMonth,
        int hour,
        int minute,
        int second,
        int nanoOfSecond,
        ZoneOffset offset
    ) {
        return OFFSET_DATE_TIME_BYTES;
    }

    /** {@code OffsetDateTime.ofInstant}. */
    public static long offsetDateTimeBytes(Instant instant, ZoneId zone) {
        return OFFSET_DATE_TIME_BYTES;
    }

    /** {@code OffsetDateTime.truncatedTo}. */
    public static long offsetDateTimeBytes(OffsetDateTime receiver, TemporalUnit unit) {
        return OFFSET_DATE_TIME_BYTES;
    }

    /** {@code OffsetDateTime.with}. */
    public static long offsetDateTimeBytes(OffsetDateTime receiver, TemporalAdjuster adjuster) {
        return OFFSET_DATE_TIME_BYTES;
    }

    /** {@code OffsetDateTime.with}. */
    public static long offsetDateTimeBytes(OffsetDateTime receiver, TemporalField field, long newValue) {
        return OFFSET_DATE_TIME_BYTES;
    }

    /**
     * {@code OffsetDateTime.withDayOfMonth}, {@code OffsetDateTime.withDayOfYear}, {@code OffsetDateTime.withHour},
     * {@code OffsetDateTime.withMinute}, {@code OffsetDateTime.withMonth}, {@code OffsetDateTime.withNano}, {@code
     * OffsetDateTime.withSecond}, {@code OffsetDateTime.withYear}.
     */
    public static long offsetDateTimeBytes(OffsetDateTime receiver, int value) {
        return OFFSET_DATE_TIME_BYTES;
    }

    /** {@code OffsetDateTime.withOffsetSameLocal}, {@code OffsetDateTime.withOffsetSameInstant}. */
    public static long offsetDateTimeBytes(OffsetDateTime receiver, ZoneOffset offset) {
        return OFFSET_DATE_TIME_BYTES;
    }

    // ---- OffsetTime chains, 56 bytes. ----

    /** {@code LocalTime.atOffset}, {@code OffsetTime.of}. */
    public static long offsetTimeBytes(LocalTime receiver, ZoneOffset offset) {
        return OFFSET_TIME_BYTES;
    }

    /** {@code OffsetDateTime.toOffsetTime}. */
    public static long offsetTimeBytes(OffsetDateTime receiver) {
        return OFFSET_TIME_BYTES;
    }

    /** {@code OffsetTime.from}. */
    public static long offsetTimeBytes(TemporalAccessor temporal) {
        return OFFSET_TIME_BYTES;
    }

    /** {@code OffsetTime.of}. */
    public static long offsetTimeBytes(int hour, int minute, int second, int nanoOfSecond, ZoneOffset offset) {
        return OFFSET_TIME_BYTES;
    }

    /** {@code OffsetTime.ofInstant}. */
    public static long offsetTimeBytes(Instant instant, ZoneId zone) {
        return OFFSET_TIME_BYTES;
    }

    /** {@code OffsetTime.plus}, {@code OffsetTime.minus}. */
    public static long offsetTimeBytes(OffsetTime receiver, TemporalAmount amount) {
        return OFFSET_TIME_BYTES;
    }

    /** {@code OffsetTime.plus}, {@code OffsetTime.minus}. */
    public static long offsetTimeBytes(OffsetTime receiver, long amount, TemporalUnit unit) {
        return OFFSET_TIME_BYTES;
    }

    /**
     * {@code OffsetTime.plusHours}, {@code OffsetTime.plusMinutes}, {@code OffsetTime.plusSeconds}, {@code
     * OffsetTime.plusNanos}, {@code OffsetTime.minusHours}, {@code OffsetTime.minusMinutes}, {@code
     * OffsetTime.minusSeconds}, {@code OffsetTime.minusNanos}.
     */
    public static long offsetTimeBytes(OffsetTime receiver, long amount) {
        return OFFSET_TIME_BYTES;
    }

    /** {@code OffsetTime.truncatedTo}. */
    public static long offsetTimeBytes(OffsetTime receiver, TemporalUnit unit) {
        return OFFSET_TIME_BYTES;
    }

    /** {@code OffsetTime.with}. */
    public static long offsetTimeBytes(OffsetTime receiver, TemporalAdjuster adjuster) {
        return OFFSET_TIME_BYTES;
    }

    /** {@code OffsetTime.with}. */
    public static long offsetTimeBytes(OffsetTime receiver, TemporalField field, long newValue) {
        return OFFSET_TIME_BYTES;
    }

    /** {@code OffsetTime.withHour}, {@code OffsetTime.withMinute}, {@code OffsetTime.withNano}, {@code OffsetTime.withSecond}. */
    public static long offsetTimeBytes(OffsetTime receiver, int value) {
        return OFFSET_TIME_BYTES;
    }

    /** {@code OffsetTime.withOffsetSameLocal}, {@code OffsetTime.withOffsetSameInstant}. */
    public static long offsetTimeBytes(OffsetTime receiver, ZoneOffset offset) {
        return OFFSET_TIME_BYTES;
    }

    // ---- ZonedDateTime chains, 120 bytes. ----

    /** {@code LocalDate.atStartOfDay(ZoneId)}. */
    public static long zonedDateTimeBytes(LocalDate receiver, ZoneId zone) {
        return ZONED_DATE_TIME_BYTES;
    }

    /** {@code OffsetDateTime.atZoneSameInstant}, {@code OffsetDateTime.atZoneSimilarLocal}. */
    public static long zonedDateTimeBytes(OffsetDateTime receiver, ZoneId zone) {
        return ZONED_DATE_TIME_BYTES;
    }

    /** {@code OffsetDateTime.toZonedDateTime}. */
    public static long zonedDateTimeBytes(OffsetDateTime receiver) {
        return ZONED_DATE_TIME_BYTES;
    }

    // ---- Interface calls: the result type is unknown, so the biggest chain is charged. ----

    /** {@code Temporal.minus}, {@code Temporal.plus}. */
    public static long temporalBytes(Temporal receiver, long amount, TemporalUnit unit) {
        return ZONED_DATE_TIME_BYTES;
    }

    /** {@code Temporal.minus}, {@code Temporal.plus}. */
    public static long temporalBytes(Temporal receiver, TemporalAmount amount) {
        return ZONED_DATE_TIME_BYTES;
    }

    /** {@code Temporal.with}. */
    public static long temporalBytes(Temporal receiver, TemporalAdjuster adjuster) {
        return ZONED_DATE_TIME_BYTES;
    }

    /** {@code Temporal.with}. */
    public static long temporalBytes(Temporal receiver, TemporalField field, long newValue) {
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
    public static long temporalBytes(TemporalField receiver, Temporal temporal, long newValue) {
        return ZONED_DATE_TIME_BYTES;
    }

    /** {@code TemporalUnit.addTo}. */
    public static long temporalBytes(TemporalUnit receiver, Temporal temporal, long amount) {
        return ZONED_DATE_TIME_BYTES;
    }

    // ---- ValueRange, 48 bytes. ----

    /** {@code ValueRange.of}. */
    public static long valueRangeBytes(long min, long max) {
        return VALUE_RANGE_BYTES;
    }

    /** {@code ValueRange.of}. */
    public static long valueRangeBytes(long min, long maxSmallest, long maxLargest) {
        return VALUE_RANGE_BYTES;
    }

    /** {@code ValueRange.of}. */
    public static long valueRangeBytes(long minSmallest, long minLargest, long maxSmallest, long maxLargest) {
        return VALUE_RANGE_BYTES;
    }

    // ---- Parse: the parse state plus the result. ----

    /** {@code LocalDate.parse}, {@code LocalTime.parse}, {@code MonthDay.parse}, {@code Year.parse}, {@code YearMonth.parse}. */
    public static long parseFlatValueBytes(CharSequence text, DateTimeFormatter formatter) {
        return AllocSizes.addSat(parseTextBytes(text), FLAT_VALUE_BYTES);
    }

    /** {@code LocalDateTime.parse}. */
    public static long parseLocalDateTimeBytes(CharSequence text) {
        return AllocSizes.addSat(parseTextBytes(text), LOCAL_DATE_TIME_BYTES);
    }

    /** {@code LocalDateTime.parse}. */
    public static long parseLocalDateTimeBytes(CharSequence text, DateTimeFormatter formatter) {
        return AllocSizes.addSat(parseTextBytes(text), LOCAL_DATE_TIME_BYTES);
    }

    /** {@code OffsetDateTime.parse}. */
    public static long parseOffsetDateTimeBytes(CharSequence text) {
        return AllocSizes.addSat(parseTextBytes(text), OFFSET_DATE_TIME_BYTES);
    }

    /** {@code OffsetDateTime.parse}. */
    public static long parseOffsetDateTimeBytes(CharSequence text, DateTimeFormatter formatter) {
        return AllocSizes.addSat(parseTextBytes(text), OFFSET_DATE_TIME_BYTES);
    }

    /** {@code OffsetTime.parse}. */
    public static long parseOffsetTimeBytes(CharSequence text) {
        return AllocSizes.addSat(parseTextBytes(text), OFFSET_TIME_BYTES);
    }

    /** {@code OffsetTime.parse}. */
    public static long parseOffsetTimeBytes(CharSequence text, DateTimeFormatter formatter) {
        return AllocSizes.addSat(parseTextBytes(text), OFFSET_TIME_BYTES);
    }

    // ---- Format: a bounded new string. ----

    /** {@code LocalTime.format}. */
    public static long formatBytes(LocalTime receiver, DateTimeFormatter formatter) {
        return newStringBytes(FORMAT_CHARACTERS);
    }

    /** {@code MonthDay.format}. */
    public static long formatBytes(MonthDay receiver, DateTimeFormatter formatter) {
        return newStringBytes(FORMAT_CHARACTERS);
    }

    /** {@code OffsetDateTime.format}. */
    public static long formatBytes(OffsetDateTime receiver, DateTimeFormatter formatter) {
        return newStringBytes(FORMAT_CHARACTERS);
    }

    /** {@code OffsetTime.format}. */
    public static long formatBytes(OffsetTime receiver, DateTimeFormatter formatter) {
        return newStringBytes(FORMAT_CHARACTERS);
    }

    /** {@code Year.format}. */
    public static long formatBytes(Year receiver, DateTimeFormatter formatter) {
        return newStringBytes(FORMAT_CHARACTERS);
    }

    /** {@code YearMonth.format}. */
    public static long formatBytes(YearMonth receiver, DateTimeFormatter formatter) {
        return newStringBytes(FORMAT_CHARACTERS);
    }

    /** {@code DateTimeFormatter.formatTo}: the string allowance stands in for the growth of the caller's appendable. */
    public static long formatBytes(DateTimeFormatter receiver, TemporalAccessor temporal, Appendable appendable) {
        return newStringBytes(FORMAT_CHARACTERS);
    }

    // ---- Formatter parse state. ----

    /** {@code DateTimeFormatter.parseBest}: the parse state plus the biggest chain one of the queries could build. */
    public static long formatterParseBytes(DateTimeFormatter receiver, CharSequence text, TemporalQuery<?>[] queries) {
        return AllocSizes.addSat(parseTextBytes(text), ZONED_DATE_TIME_BYTES);
    }

    /** {@code DateTimeFormatter.parseUnresolved}. */
    public static long formatterParseBytes(DateTimeFormatter receiver, CharSequence text, ParsePosition position) {
        return parseTextBytes(text);
    }

    // ---- Formatter copies. ----

    /** {@code DateTimeFormatter.withChronology}. */
    public static long formatterCopyBytes(DateTimeFormatter receiver, Chronology chrono) {
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

    /** {@code DateTimeFormatter.withResolverFields(Set)}: the formatter plus a copy of the set. */
    public static long formatterCopyBytes(DateTimeFormatter receiver, Set<?> set) {
        return AllocSizes.addSat(FORMATTER_BYTES, AllocationEstimators.setCopyBytes(set));
    }

    /** {@code DateTimeFormatter.withResolverStyle}. */
    public static long formatterCopyBytes(DateTimeFormatter receiver, ResolverStyle resolverStyle) {
        return FORMATTER_BYTES;
    }

    /** {@code DateTimeFormatter.withZone}. */
    public static long formatterCopyBytes(DateTimeFormatter receiver, ZoneId zone) {
        return FORMATTER_BYTES;
    }

    // ---- Localized formatters are built from a pattern each call. ----

    /**
     * {@code DateTimeFormatter.ofLocalizedDate}, {@code DateTimeFormatter.ofLocalizedDateTime}, {@code
     * DateTimeFormatter.ofLocalizedTime}.
     */
    public static long localizedFormatterBytes(FormatStyle style) {
        return LOCALIZED_FORMATTER_BYTES;
    }

    /** {@code DateTimeFormatter.ofLocalizedDateTime}. */
    public static long localizedFormatterBytes(FormatStyle dateStyle, FormatStyle timeStyle) {
        return LOCALIZED_FORMATTER_BYTES;
    }

    /** Characters allowed for a localized pattern string. */
    private static final long LOCALIZED_PATTERN_CHARACTERS = 64;

    /**
     * {@code DateTimeFormatterBuilder.getLocalizedDateTimePattern}: the date and time patterns are cached, but each call builds
     * a cache key string and, when both styles are given, joins the two patterns into a new string.
     */
    public static long localizedPatternBytes(FormatStyle dateStyle, FormatStyle timeStyle, Chronology chrono, Locale locale) {
        return 2 * newStringBytes(LOCALIZED_PATTERN_CHARACTERS);
    }

    // ---- Format adapters. ----

    /** {@code DateTimeFormatter.toFormat}: an adapter holding the formatter and a query. */
    public static long toFormatBytes(DateTimeFormatter receiver) {
        return TWO_REFERENCE_BYTES;
    }

    /** {@code DateTimeFormatter.toFormat}: an adapter holding the formatter and the query. */
    public static long toFormatBytes(DateTimeFormatter receiver, TemporalQuery<?> query) {
        return TWO_REFERENCE_BYTES;
    }

    // ---- Formatter builder: the builder, one step per appended element, and the built formatter. ----

    /** {@code new DateTimeFormatterBuilder()}. */
    public static long formatterBuilderBytes() {
        return BUILDER_BYTES;
    }

    /** {@code DateTimeFormatterBuilder.append}, {@code DateTimeFormatterBuilder.appendOptional}. */
    public static long builderStepBytes(DateTimeFormatterBuilder receiver, DateTimeFormatter formatter) {
        return PATTERN_BYTES_PER_CHARACTER;
    }

    /**
     * {@code DateTimeFormatterBuilder.appendChronologyId}, {@code DateTimeFormatterBuilder.appendInstant}, {@code
     * DateTimeFormatterBuilder.appendOffsetId}, {@code DateTimeFormatterBuilder.appendZoneId}, {@code
     * DateTimeFormatterBuilder.appendZoneOrOffsetId}, {@code DateTimeFormatterBuilder.appendZoneRegionId}, {@code
     * DateTimeFormatterBuilder.optionalEnd}, {@code DateTimeFormatterBuilder.optionalStart}, {@code
     * DateTimeFormatterBuilder.parseCaseInsensitive}, {@code DateTimeFormatterBuilder.parseCaseSensitive}, {@code
     * DateTimeFormatterBuilder.parseLenient}, {@code DateTimeFormatterBuilder.parseStrict}.
     */
    public static long builderStepBytes(DateTimeFormatterBuilder receiver) {
        return PATTERN_BYTES_PER_CHARACTER;
    }

    /**
     * {@code DateTimeFormatterBuilder.appendChronologyText}, {@code DateTimeFormatterBuilder.appendLocalizedOffset},
     * {@code DateTimeFormatterBuilder.appendZoneText}.
     */
    public static long builderStepBytes(DateTimeFormatterBuilder receiver, TextStyle style) {
        return PATTERN_BYTES_PER_CHARACTER;
    }

    /** {@code DateTimeFormatterBuilder.appendFraction}. */
    public static long builderStepBytes(
        DateTimeFormatterBuilder receiver,
        TemporalField field,
        int minWidth,
        int maxWidth,
        boolean decimalPoint
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
    public static long builderStepBytes(DateTimeFormatterBuilder receiver, FormatStyle dateStyle, FormatStyle timeStyle) {
        return PATTERN_BYTES_PER_CHARACTER;
    }

    /** {@code DateTimeFormatterBuilder.appendOffset}. */
    public static long builderStepBytes(DateTimeFormatterBuilder receiver, String text, String text2) {
        return PATTERN_BYTES_PER_CHARACTER;
    }

    /** {@code DateTimeFormatterBuilder.appendText}, {@code DateTimeFormatterBuilder.appendValue}. */
    public static long builderStepBytes(DateTimeFormatterBuilder receiver, TemporalField field) {
        return PATTERN_BYTES_PER_CHARACTER;
    }

    /** {@code DateTimeFormatterBuilder.appendText}. */
    public static long builderStepBytes(DateTimeFormatterBuilder receiver, TemporalField field, TextStyle style) {
        return PATTERN_BYTES_PER_CHARACTER;
    }

    /** {@code DateTimeFormatterBuilder.appendValue}. */
    public static long builderStepBytes(DateTimeFormatterBuilder receiver, TemporalField field, int width) {
        return PATTERN_BYTES_PER_CHARACTER;
    }

    /** {@code DateTimeFormatterBuilder.appendValue}. */
    public static long builderStepBytes(
        DateTimeFormatterBuilder receiver,
        TemporalField field,
        int minWidth,
        int maxWidth,
        SignStyle signStyle
    ) {
        return PATTERN_BYTES_PER_CHARACTER;
    }

    /** {@code DateTimeFormatterBuilder.appendValueReduced}. */
    public static long builderStepBytes(DateTimeFormatterBuilder receiver, TemporalField field, int width, int maxWidth, int baseValue) {
        return PATTERN_BYTES_PER_CHARACTER;
    }

    /** {@code DateTimeFormatterBuilder.appendZoneText(TextStyle, Set)}: the step plus a copy of the set when it has entries. */
    public static long builderStepBytes(DateTimeFormatterBuilder receiver, TextStyle style, Set<?> set) {
        long copy = set == null || set.isEmpty() ? 0 : AllocationEstimators.setCopyBytes(set);
        return AllocSizes.addSat(PATTERN_BYTES_PER_CHARACTER, copy);
    }

    /** {@code DateTimeFormatterBuilder.padNext}. */
    public static long builderStepBytes(DateTimeFormatterBuilder receiver, int padWidth, char padChar) {
        return PATTERN_BYTES_PER_CHARACTER;
    }

    /** {@code DateTimeFormatterBuilder.parseDefaulting}. */
    public static long builderStepBytes(DateTimeFormatterBuilder receiver, TemporalField field, long value) {
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

    // ---- Display names: the JDK builds a localized formatter and formats with it. ----

    /** A display name: a localized formatter, then a short new string. */
    private static long displayNameBytes() {
        return AllocSizes.addSat(LOCALIZED_FORMATTER_BYTES, newStringBytes(DISPLAY_NAME_CHARACTERS));
    }

    /** {@code ZoneId.getDisplayName}. */
    public static long displayNameBytes(ZoneId receiver, TextStyle style, Locale locale) {
        return displayNameBytes();
    }

    /** {@code DayOfWeek.getDisplayName}. */
    public static long displayNameBytes(DayOfWeek receiver, TextStyle style, Locale locale) {
        return displayNameBytes();
    }

    /** {@code Month.getDisplayName}. */
    public static long displayNameBytes(Month receiver, TextStyle style, Locale locale) {
        return displayNameBytes();
    }

    /** {@code Chronology.getDisplayName}. */
    public static long displayNameBytes(Chronology receiver, TextStyle style, Locale locale) {
        return displayNameBytes();
    }

    /** {@code Era.getDisplayName}. */
    public static long displayNameBytes(Era receiver, TextStyle style, Locale locale) {
        return displayNameBytes();
    }

    /** {@code ZoneId.getAvailableZoneIds}. */
    public static long availableZoneIdsBytes() {
        return AVAILABLE_ZONE_IDS_BYTES;
    }

    /** {@code Chronology.getAvailableChronologies}. */
    public static long availableChronologiesBytes() {
        return AVAILABLE_CHRONOLOGIES_BYTES;
    }

    /** {@code TemporalAccessor.range}: often a fresh range from the value, like the days in this month. */
    public static long valueRangeBytes(TemporalAccessor receiver, TemporalField field) {
        return VALUE_RANGE_BYTES;
    }

    /** {@code TemporalField.rangeRefinedBy}. */
    public static long valueRangeBytes(TemporalField receiver, TemporalAccessor temporal) {
        return VALUE_RANGE_BYTES;
    }

    // ---- Chrono interfaces. The ISO classes inherit these entries, so the charge lives here. ----

    /** {@code ChronoLocalDate.atTime}. */
    public static long localDateTimeBytes(ChronoLocalDate receiver, LocalTime time) {
        return LOCAL_DATE_TIME_BYTES;
    }

    /** {@code ChronoLocalDate.format}. */
    public static long formatBytes(ChronoLocalDate receiver, DateTimeFormatter formatter) {
        return newStringBytes(FORMAT_CHARACTERS);
    }

    /** {@code ChronoLocalDate.minus}, {@code ChronoLocalDate.plus}. */
    public static long flatValueBytes(ChronoLocalDate receiver, TemporalAmount amount) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code ChronoLocalDate.minus}, {@code ChronoLocalDate.plus}. */
    public static long flatValueBytes(ChronoLocalDate receiver, long amount, TemporalUnit unit) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code ChronoLocalDate.until}. */
    public static long flatValueBytes(ChronoLocalDate receiver, ChronoLocalDate other) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code ChronoLocalDate.with}. */
    public static long flatValueBytes(ChronoLocalDate receiver, TemporalAdjuster adjuster) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code ChronoLocalDate.with}. */
    public static long flatValueBytes(ChronoLocalDate receiver, TemporalField field, long newValue) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code ChronoLocalDateTime.atZone}. */
    public static long zonedDateTimeBytes(ChronoLocalDateTime<?> receiver, ZoneId zone) {
        return ZONED_DATE_TIME_BYTES;
    }

    /** {@code ChronoLocalDateTime.format}. */
    public static long formatBytes(ChronoLocalDateTime<?> receiver, DateTimeFormatter formatter) {
        return newStringBytes(FORMAT_CHARACTERS);
    }

    /** {@code ChronoLocalDateTime.minus}, {@code ChronoLocalDateTime.plus}. */
    public static long localDateTimeBytes(ChronoLocalDateTime<?> receiver, TemporalAmount amount) {
        return LOCAL_DATE_TIME_BYTES;
    }

    /** {@code ChronoLocalDateTime.minus}, {@code ChronoLocalDateTime.plus}. */
    public static long localDateTimeBytes(ChronoLocalDateTime<?> receiver, long amount, TemporalUnit unit) {
        return LOCAL_DATE_TIME_BYTES;
    }

    /** {@code ChronoLocalDateTime.toInstant}. */
    public static long flatValueBytes(ChronoLocalDateTime<?> receiver, ZoneOffset offset) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code ChronoLocalDateTime.with}. */
    public static long localDateTimeBytes(ChronoLocalDateTime<?> receiver, TemporalAdjuster adjuster) {
        return LOCAL_DATE_TIME_BYTES;
    }

    /** {@code ChronoLocalDateTime.with}. */
    public static long localDateTimeBytes(ChronoLocalDateTime<?> receiver, TemporalField field, long newValue) {
        return LOCAL_DATE_TIME_BYTES;
    }

    /** {@code ChronoPeriod.minus}, {@code ChronoPeriod.plus}. */
    public static long flatValueBytes(ChronoPeriod receiver, TemporalAmount amount) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code ChronoPeriod.multipliedBy}. */
    public static long flatValueBytes(ChronoPeriod receiver, int scalar) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code ChronoPeriod.negated}, {@code ChronoPeriod.normalized}. */
    public static long flatValueBytes(ChronoPeriod receiver) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code ChronoZonedDateTime.format}. */
    public static long formatBytes(ChronoZonedDateTime<?> receiver, DateTimeFormatter formatter) {
        return newStringBytes(FORMAT_CHARACTERS);
    }

    /** {@code ChronoZonedDateTime.minus}, {@code ChronoZonedDateTime.plus}. */
    public static long zonedDateTimeBytes(ChronoZonedDateTime<?> receiver, TemporalAmount amount) {
        return ZONED_DATE_TIME_BYTES;
    }

    /** {@code ChronoZonedDateTime.minus}, {@code ChronoZonedDateTime.plus}. */
    public static long zonedDateTimeBytes(ChronoZonedDateTime<?> receiver, long amount, TemporalUnit unit) {
        return ZONED_DATE_TIME_BYTES;
    }

    /** {@code ChronoZonedDateTime.toInstant}. */
    public static long flatValueBytes(ChronoZonedDateTime<?> receiver) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code ChronoZonedDateTime.with}. */
    public static long zonedDateTimeBytes(ChronoZonedDateTime<?> receiver, TemporalAdjuster adjuster) {
        return ZONED_DATE_TIME_BYTES;
    }

    /** {@code ChronoZonedDateTime.with}. */
    public static long zonedDateTimeBytes(ChronoZonedDateTime<?> receiver, TemporalField field, long newValue) {
        return ZONED_DATE_TIME_BYTES;
    }

    /** {@code ChronoZonedDateTime.withEarlierOffsetAtOverlap}, {@code ChronoZonedDateTime.withLaterOffsetAtOverlap}. */
    public static long zonedDateTimeBytes(ChronoZonedDateTime<?> receiver) {
        return ZONED_DATE_TIME_BYTES;
    }

    /** {@code ChronoZonedDateTime.withZoneSameLocal}, {@code ChronoZonedDateTime.withZoneSameInstant}. */
    public static long zonedDateTimeBytes(ChronoZonedDateTime<?> receiver, ZoneId zone) {
        return ZONED_DATE_TIME_BYTES;
    }

    // ---- Chrono classes and chronologies. Non-ISO dates wrap an ISO date; the ISO chronology hands back plain values. ----

    /** {@code HijrahDate.of}, {@code JapaneseDate.of}, {@code MinguoDate.of}, {@code ThaiBuddhistDate.of}. */
    public static long chronoDateBytes(int prolepticYear, int month, int dayOfMonth) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code HijrahDate.from}, {@code JapaneseDate.from}, {@code MinguoDate.from}, {@code ThaiBuddhistDate.from}. */
    public static long chronoDateBytes(TemporalAccessor temporal) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code HijrahDate.plus}, {@code HijrahDate.minus}. */
    public static long chronoDateBytes(HijrahDate receiver, TemporalAmount amount) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code HijrahDate.plus}, {@code HijrahDate.minus}. */
    public static long chronoDateBytes(HijrahDate receiver, long amount, TemporalUnit unit) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code HijrahDate.with}. */
    public static long chronoDateBytes(HijrahDate receiver, TemporalField field, long newValue) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code HijrahDate.with}. */
    public static long chronoDateBytes(HijrahDate receiver, TemporalAdjuster adjuster) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code HijrahDate.withVariant}. */
    public static long chronoDateBytes(HijrahDate receiver, HijrahChronology chrono) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code JapaneseDate.plus}, {@code JapaneseDate.minus}. */
    public static long chronoDateBytes(JapaneseDate receiver, TemporalAmount amount) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code JapaneseDate.plus}, {@code JapaneseDate.minus}. */
    public static long chronoDateBytes(JapaneseDate receiver, long amount, TemporalUnit unit) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code JapaneseDate.with}. */
    public static long chronoDateBytes(JapaneseDate receiver, TemporalField field, long newValue) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code JapaneseDate.with}. */
    public static long chronoDateBytes(JapaneseDate receiver, TemporalAdjuster adjuster) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code MinguoDate.plus}, {@code MinguoDate.minus}. */
    public static long chronoDateBytes(MinguoDate receiver, TemporalAmount amount) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code MinguoDate.plus}, {@code MinguoDate.minus}. */
    public static long chronoDateBytes(MinguoDate receiver, long amount, TemporalUnit unit) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code MinguoDate.with}. */
    public static long chronoDateBytes(MinguoDate receiver, TemporalField field, long newValue) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code MinguoDate.with}. */
    public static long chronoDateBytes(MinguoDate receiver, TemporalAdjuster adjuster) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code ThaiBuddhistDate.plus}, {@code ThaiBuddhistDate.minus}. */
    public static long chronoDateBytes(ThaiBuddhistDate receiver, TemporalAmount amount) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code ThaiBuddhistDate.plus}, {@code ThaiBuddhistDate.minus}. */
    public static long chronoDateBytes(ThaiBuddhistDate receiver, long amount, TemporalUnit unit) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code ThaiBuddhistDate.with}. */
    public static long chronoDateBytes(ThaiBuddhistDate receiver, TemporalField field, long newValue) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code ThaiBuddhistDate.with}. */
    public static long chronoDateBytes(ThaiBuddhistDate receiver, TemporalAdjuster adjuster) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code HijrahChronology.date}. */
    public static long chronoDateBytes(HijrahChronology receiver, TemporalAccessor temporal) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code HijrahChronology.date}. */
    public static long chronoDateBytes(HijrahChronology receiver, int prolepticYear, int month, int dayOfMonth) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code HijrahChronology.date}. */
    public static long chronoDateBytes(HijrahChronology receiver, Era era, int yearOfEra, int month, int dayOfMonth) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code HijrahChronology.dateEpochDay}. */
    public static long chronoDateBytes(HijrahChronology receiver, long epochDay) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code HijrahChronology.dateYearDay}. */
    public static long chronoDateBytes(HijrahChronology receiver, int prolepticYear, int dayOfYear) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code HijrahChronology.dateYearDay}. */
    public static long chronoDateBytes(HijrahChronology receiver, Era era, int yearOfEra, int dayOfYear) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code HijrahChronology.resolveDate}. */
    public static long chronoDateBytes(HijrahChronology receiver, Map<?, ?> fieldValues, ResolverStyle resolverStyle) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code JapaneseChronology.date}. */
    public static long chronoDateBytes(JapaneseChronology receiver, TemporalAccessor temporal) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code JapaneseChronology.date}. */
    public static long chronoDateBytes(JapaneseChronology receiver, int prolepticYear, int month, int dayOfMonth) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code JapaneseChronology.date}. */
    public static long chronoDateBytes(JapaneseChronology receiver, Era era, int yearOfEra, int month, int dayOfMonth) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code JapaneseChronology.dateEpochDay}. */
    public static long chronoDateBytes(JapaneseChronology receiver, long epochDay) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code JapaneseChronology.dateYearDay}. */
    public static long chronoDateBytes(JapaneseChronology receiver, int prolepticYear, int dayOfYear) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code JapaneseChronology.dateYearDay}. */
    public static long chronoDateBytes(JapaneseChronology receiver, Era era, int yearOfEra, int dayOfYear) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code JapaneseChronology.resolveDate}. */
    public static long chronoDateBytes(JapaneseChronology receiver, Map<?, ?> fieldValues, ResolverStyle resolverStyle) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code MinguoChronology.date}. */
    public static long chronoDateBytes(MinguoChronology receiver, TemporalAccessor temporal) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code MinguoChronology.date}. */
    public static long chronoDateBytes(MinguoChronology receiver, int prolepticYear, int month, int dayOfMonth) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code MinguoChronology.date}. */
    public static long chronoDateBytes(MinguoChronology receiver, Era era, int yearOfEra, int month, int dayOfMonth) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code MinguoChronology.dateEpochDay}. */
    public static long chronoDateBytes(MinguoChronology receiver, long epochDay) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code MinguoChronology.dateYearDay}. */
    public static long chronoDateBytes(MinguoChronology receiver, int prolepticYear, int dayOfYear) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code MinguoChronology.dateYearDay}. */
    public static long chronoDateBytes(MinguoChronology receiver, Era era, int yearOfEra, int dayOfYear) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code MinguoChronology.resolveDate}. */
    public static long chronoDateBytes(MinguoChronology receiver, Map<?, ?> fieldValues, ResolverStyle resolverStyle) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code ThaiBuddhistChronology.date}. */
    public static long chronoDateBytes(ThaiBuddhistChronology receiver, TemporalAccessor temporal) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code ThaiBuddhistChronology.date}. */
    public static long chronoDateBytes(ThaiBuddhistChronology receiver, int prolepticYear, int month, int dayOfMonth) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code ThaiBuddhistChronology.date}. */
    public static long chronoDateBytes(ThaiBuddhistChronology receiver, Era era, int yearOfEra, int month, int dayOfMonth) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code ThaiBuddhistChronology.dateEpochDay}. */
    public static long chronoDateBytes(ThaiBuddhistChronology receiver, long epochDay) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code ThaiBuddhistChronology.dateYearDay}. */
    public static long chronoDateBytes(ThaiBuddhistChronology receiver, int prolepticYear, int dayOfYear) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code ThaiBuddhistChronology.dateYearDay}. */
    public static long chronoDateBytes(ThaiBuddhistChronology receiver, Era era, int yearOfEra, int dayOfYear) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code ThaiBuddhistChronology.resolveDate}. */
    public static long chronoDateBytes(ThaiBuddhistChronology receiver, Map<?, ?> fieldValues, ResolverStyle resolverStyle) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code IsoChronology.date}. */
    public static long flatValueBytes(IsoChronology receiver, TemporalAccessor temporal) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code IsoChronology.date}, {@code IsoChronology.period}. */
    public static long flatValueBytes(IsoChronology receiver, int a, int b, int c) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code IsoChronology.date}. */
    public static long flatValueBytes(IsoChronology receiver, Era era, int yearOfEra, int month, int dayOfMonth) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code IsoChronology.dateEpochDay}. */
    public static long flatValueBytes(IsoChronology receiver, long epochDay) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code IsoChronology.dateYearDay}. */
    public static long flatValueBytes(IsoChronology receiver, int prolepticYear, int dayOfYear) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code IsoChronology.dateYearDay}. */
    public static long flatValueBytes(IsoChronology receiver, Era era, int yearOfEra, int dayOfYear) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code IsoChronology.localDateTime}. */
    public static long localDateTimeBytes(IsoChronology receiver, TemporalAccessor temporal) {
        return LOCAL_DATE_TIME_BYTES;
    }

    /** {@code IsoChronology.resolveDate}. */
    public static long flatValueBytes(IsoChronology receiver, Map<?, ?> fieldValues, ResolverStyle resolverStyle) {
        return FLAT_VALUE_BYTES;
    }

    /** {@code IsoChronology.zonedDateTime}. */
    public static long zonedDateTimeBytes(IsoChronology receiver, TemporalAccessor temporal) {
        return ZONED_DATE_TIME_BYTES;
    }

    /** {@code IsoChronology.zonedDateTime}. */
    public static long zonedDateTimeBytes(IsoChronology receiver, Instant instant, ZoneId zone) {
        return ZONED_DATE_TIME_BYTES;
    }

    /** {@code Chronology.date}. */
    public static long chronoDateBytes(Chronology receiver, TemporalAccessor temporal) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code Chronology.date}. */
    public static long chronoDateBytes(Chronology receiver, Era era, int yearOfEra, int month, int dayOfMonth) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code Chronology.date}. */
    public static long chronoDateBytes(Chronology receiver, int prolepticYear, int month, int dayOfMonth) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code Chronology.dateEpochDay}. */
    public static long chronoDateBytes(Chronology receiver, long epochDay) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code Chronology.dateYearDay}. */
    public static long chronoDateBytes(Chronology receiver, Era era, int yearOfEra, int dayOfYear) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code Chronology.dateYearDay}. */
    public static long chronoDateBytes(Chronology receiver, int prolepticYear, int dayOfYear) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code Chronology.localDateTime}. */
    public static long chronoLocalDateTimeBytes(Chronology receiver, TemporalAccessor temporal) {
        return CHRONO_LOCAL_DATE_TIME_BYTES;
    }

    /** {@code Chronology.period}. */
    public static long chronoPeriodBytes(Chronology receiver, int years, int months, int days) {
        return TWO_REFERENCE_BYTES;
    }

    /** {@code Chronology.resolveDate}. */
    public static long chronoDateBytes(Chronology receiver, Map<?, ?> fieldValues, ResolverStyle resolverStyle) {
        return CHRONO_DATE_BYTES;
    }

    /** {@code Chronology.zonedDateTime}. */
    public static long chronoZonedDateTimeBytes(Chronology receiver, TemporalAccessor temporal) {
        return CHRONO_ZONED_DATE_TIME_BYTES;
    }

    /** {@code Chronology.zonedDateTime}. */
    public static long chronoZonedDateTimeBytes(Chronology receiver, Instant instant, ZoneId zone) {
        return CHRONO_ZONED_DATE_TIME_BYTES;
    }
}
