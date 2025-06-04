/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.common.LocalTimeOffset.Gap;
import org.elasticsearch.common.LocalTimeOffset.Overlap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchException;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.IsoFields;
import java.time.temporal.TemporalField;
import java.time.temporal.TemporalQueries;
import java.time.zone.ZoneOffsetTransition;
import java.time.zone.ZoneRules;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * A strategy for rounding milliseconds since epoch.
 * <p>
 * There are two implementations for rounding.
 * The first one requires a date time unit and rounds to the supplied date time unit (i.e. quarter of year, day of month).
 * The second one allows you to specify an interval to round to.
 * <p>
 * See <a href="https://davecturner.github.io/2019/04/14/timezone-rounding.html">this</a>
 * blog for some background reading. Its super interesting and the links are
 * a comedy gold mine. If you like time zones. Or hate them.
 */
public abstract class Rounding implements Writeable {
    private static final Logger logger = LogManager.getLogger(Rounding.class);

    public enum DateTimeUnit {
        WEEK_OF_WEEKYEAR((byte) 1, "week", IsoFields.WEEK_OF_WEEK_BASED_YEAR, true, TimeUnit.DAYS.toMillis(7)) {
            private final long extraLocalOffsetLookup = TimeUnit.DAYS.toMillis(7);

            @Override
            long roundFloor(long utcMillis, int multiplier) {
                return DateUtils.roundWeekIntervalOfWeekYear(utcMillis, multiplier);
            }

            @Override
            long extraLocalOffsetLookup() {
                return extraLocalOffsetLookup;
            }
        },
        YEAR_OF_CENTURY((byte) 2, "year", ChronoField.YEAR_OF_ERA, false, 12) {
            private final long extraLocalOffsetLookup = TimeUnit.DAYS.toMillis(366);

            @Override
            long roundFloor(long utcMillis, int multiplier) {
                return multiplier == 1 ? DateUtils.roundYear(utcMillis) : DateUtils.roundYearInterval(utcMillis, multiplier);
            }

            @Override
            long extraLocalOffsetLookup() {
                return extraLocalOffsetLookup;
            }
        },
        QUARTER_OF_YEAR((byte) 3, "quarter", IsoFields.QUARTER_OF_YEAR, false, 3) {
            private final long extraLocalOffsetLookup = TimeUnit.DAYS.toMillis(92);

            @Override
            long roundFloor(long utcMillis, int multiplier) {
                return multiplier == 1
                    ? DateUtils.roundQuarterOfYear(utcMillis)
                    : DateUtils.roundIntervalMonthOfYear(utcMillis, multiplier * 3);
            }

            @Override
            long extraLocalOffsetLookup() {
                return extraLocalOffsetLookup;
            }
        },
        MONTH_OF_YEAR((byte) 4, "month", ChronoField.MONTH_OF_YEAR, false, 1) {
            private final long extraLocalOffsetLookup = TimeUnit.DAYS.toMillis(31);

            @Override
            long roundFloor(long utcMillis, int multiplier) {
                return multiplier == 1 ? DateUtils.roundMonthOfYear(utcMillis) : DateUtils.roundIntervalMonthOfYear(utcMillis, multiplier);
            }

            @Override
            long extraLocalOffsetLookup() {
                return extraLocalOffsetLookup;
            }
        },
        DAY_OF_MONTH((byte) 5, "day", ChronoField.DAY_OF_MONTH, true, ChronoField.DAY_OF_MONTH.getBaseUnit().getDuration().toMillis()) {
            @Override
            long roundFloor(long utcMillis, int multiplier) {
                return DateUtils.roundFloor(utcMillis, this.ratio * multiplier);
            }

            @Override
            long extraLocalOffsetLookup() {
                return ratio;
            }
        },
        HOUR_OF_DAY((byte) 6, "hour", ChronoField.HOUR_OF_DAY, true, ChronoField.HOUR_OF_DAY.getBaseUnit().getDuration().toMillis()) {
            @Override
            long roundFloor(long utcMillis, int multiplier) {
                return DateUtils.roundFloor(utcMillis, ratio * multiplier);
            }

            @Override
            long extraLocalOffsetLookup() {
                return ratio;
            }
        },
        MINUTES_OF_HOUR(
            (byte) 7,
            "minute",
            ChronoField.MINUTE_OF_HOUR,
            true,
            ChronoField.MINUTE_OF_HOUR.getBaseUnit().getDuration().toMillis()
        ) {
            @Override
            long roundFloor(long utcMillis, int multiplier) {
                return DateUtils.roundFloor(utcMillis, ratio * multiplier);
            }

            @Override
            long extraLocalOffsetLookup() {
                return ratio;
            }
        },
        SECOND_OF_MINUTE(
            (byte) 8,
            "second",
            ChronoField.SECOND_OF_MINUTE,
            true,
            ChronoField.SECOND_OF_MINUTE.getBaseUnit().getDuration().toMillis()
        ) {
            @Override
            long roundFloor(long utcMillis, int multiplier) {
                return DateUtils.roundFloor(utcMillis, ratio * multiplier);
            }

            @Override
            long extraLocalOffsetLookup() {
                return ratio;
            }
        };

        private final byte id;
        private final TemporalField field;
        private final boolean isMillisBased;
        private final String shortName;
        /**
         * ratio to milliseconds if isMillisBased == true or to month otherwise
         */
        protected final long ratio;

        DateTimeUnit(byte id, String shortName, TemporalField field, boolean isMillisBased, long ratio) {
            this.id = id;
            this.shortName = shortName;
            this.field = field;
            this.isMillisBased = isMillisBased;
            this.ratio = ratio;
        }

        /**
         * This rounds down the supplied milliseconds since the epoch down to the next unit. In order to retain performance this method
         * should be as fast as possible and not try to convert dates to java-time objects if possible
         *
         * @param utcMillis  the milliseconds since the epoch
         * @param multiplier the factor by which the unit is multiplied
         * @return     the rounded down milliseconds since the epoch
         */
        abstract long roundFloor(long utcMillis, int multiplier);

        /**
         * When looking up {@link LocalTimeOffset} go this many milliseconds
         * in the past from the minimum millis since epoch that we plan to
         * look up so that we can see transitions that we might have rounded
         * down beyond.
         */
        abstract long extraLocalOffsetLookup();

        public byte getId() {
            return id;
        }

        public TemporalField getField() {
            return field;
        }

        public String shortName() {
            return shortName;
        }

        public static DateTimeUnit resolve(byte id) {
            return switch (id) {
                case 1 -> WEEK_OF_WEEKYEAR;
                case 2 -> YEAR_OF_CENTURY;
                case 3 -> QUARTER_OF_YEAR;
                case 4 -> MONTH_OF_YEAR;
                case 5 -> DAY_OF_MONTH;
                case 6 -> HOUR_OF_DAY;
                case 7 -> MINUTES_OF_HOUR;
                case 8 -> SECOND_OF_MINUTE;
                default -> throw new ElasticsearchException("Unknown date time unit id [" + id + "]");
            };
        }
    }

    public abstract void innerWriteTo(StreamOutput out) throws IOException;

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByte(id());
        innerWriteTo(out);
    }

    public abstract byte id();

    /**
     * A strategy for rounding milliseconds since epoch.
     */
    public interface Prepared {
        /**
         * Rounds the given value.
         */
        long round(long utcMillis);

        /**
         * Given the rounded value (which was potentially generated by
         * {@link #round(long)}, returns the next rounding value. For
         * example, with interval based rounding, if the interval is
         * 3, {@code nextRoundValue(6) = 9}.
         */
        long nextRoundingValue(long utcMillis);

        /**
         * Given the rounded value, returns the size between this value and the
         * next rounded value in specified units if possible.
         */
        double roundingSize(long utcMillis, DateTimeUnit timeUnit);

        /**
         * Returns the size of each rounding bucket in timeUnits.
         */
        double roundingSize(DateTimeUnit timeUnit);

        /**
         * If this rounding mechanism precalculates rounding points then
         * this array stores dates such that each date between each entry.
         * if the rounding mechanism doesn't precalculate points then this
         * is {@code null}.
         */
        long[] fixedRoundingPoints();
    }

    /**
     * Prepare to round many times.
     */
    public abstract Prepared prepare(long minUtcMillis, long maxUtcMillis);

    /**
     * Prepare to round many dates over an unknown range. Prefer
     * {@link #prepare(long, long)} if you can find the range because
     * it'll be much more efficient.
     */
    public abstract Prepared prepareForUnknown();

    /**
     * Prepare rounding forcing the java time implementation. Prefer
     * {@link #prepare} or {@link #prepareForUnknown} which can be much
     * faster.
     */
    public abstract Prepared prepareJavaTime();

    /**
     * Rounds the given value.
     * @deprecated Prefer {@link #prepare} and then {@link Prepared#round(long)}
     */
    @Deprecated
    public final long round(long utcMillis) {
        return prepare(utcMillis, utcMillis).round(utcMillis);
    }

    /**
     * Given the rounded value (which was potentially generated by
     * {@link #round(long)}, returns the next rounding value. For
     * example, with interval based rounding, if the interval is
     * 3, {@code nextRoundValue(6) = 9}.
     * @deprecated Prefer {@link #prepare} and then {@link Prepared#nextRoundingValue(long)}
     */
    @Deprecated
    public final long nextRoundingValue(long utcMillis) {
        return prepare(utcMillis, utcMillis).nextRoundingValue(utcMillis);
    }

    /**
     * How "offset" this rounding is from the traditional "start" of the period.
     * @deprecated We're in the process of abstracting offset *into* Rounding
     *             so keep any usage to migratory shims
     */
    @Deprecated
    public abstract long offset();

    /**
     * Strip the {@code offset} from these bounds.
     */
    public abstract Rounding withoutOffset();

    @Override
    public abstract boolean equals(Object obj);

    @Override
    public abstract int hashCode();

    public static Builder builder(DateTimeUnit unit) {
        return new Builder(unit);
    }

    public static Builder builder(TimeValue interval) {
        return new Builder(interval);
    }

    public static class Builder {

        private final DateTimeUnit unit;
        private final long interval;
        private final int multiplier;

        private ZoneId timeZone = ZoneOffset.UTC;
        private long offset = 0;

        public Builder(DateTimeUnit unit) {
            this(unit, 1);
        }

        public Builder(DateTimeUnit unit, int multiplier) {
            this.unit = unit;
            this.multiplier = multiplier;
            this.interval = -1;
        }

        public Builder(TimeValue interval) {
            this.unit = null;
            this.multiplier = -1;
            if (interval.millis() < 1) throw new IllegalArgumentException("Zero or negative time interval not supported");
            this.interval = interval.millis();
        }

        public Builder timeZone(ZoneId timeZone) {
            if (timeZone == null) {
                throw new IllegalArgumentException("Setting null as timezone is not supported");
            }
            this.timeZone = timeZone;
            return this;
        }

        /**
         * Sets the offset of this rounding from the normal beginning of the interval. Use this
         * to start days at 6am or months on the 15th.
         * @param offset the offset, in milliseconds
         */
        public Builder offset(long offset) {
            this.offset = offset;
            return this;
        }

        public Rounding build() {
            Rounding rounding;
            if (unit != null) {
                rounding = new TimeUnitRounding(unit, multiplier, timeZone);
            } else {
                rounding = new TimeIntervalRounding(interval, timeZone);
            }
            if (offset != 0) {
                rounding = new OffsetRounding(rounding, offset);
            }
            return rounding;
        }
    }

    private abstract static class PreparedRounding implements Prepared {
        /**
         * Attempt to build a {@link Prepared} implementation that relies on pre-calcuated
         * "round down" points. If there would be more than {@code max} points then return
         * the original implementation, otherwise return the new, faster implementation.
         */
        protected Prepared maybeUseArray(long minUtcMillis, long maxUtcMillis, int max) {
            long[] values = new long[1];
            long rounded = round(minUtcMillis);
            int i = 0;
            values[i++] = rounded;
            while ((rounded = nextRoundingValue(rounded)) <= maxUtcMillis) {
                if (i >= max) {
                    logger.trace(
                        "can't realize [{}] to fixed rounding points, more than [{}] rounding points between [{}] and [{}]",
                        this,
                        max,
                        minUtcMillis,
                        maxUtcMillis
                    );
                    return this;
                }
                /*
                 * We expect a time in the last transition (rounded - 1) to round
                 * to the last value we calculated. If it doesn't then we're
                 * probably doing something wrong here....
                 */
                assert values[i - 1] == round(rounded - 1);
                values = ArrayUtil.grow(values, i + 1);
                values[i++] = rounded;
            }
            return new ArrayRounding(values, i, this);
        }

        @Override
        public long[] fixedRoundingPoints() {
            return null;
        }
    }

    static class TimeUnitRounding extends Rounding {
        static final byte ID = 1;

        private final DateTimeUnit unit;
        private final ZoneId timeZone;
        private final boolean unitRoundsToMidnight;
        private final int multiplier;

        TimeUnitRounding(DateTimeUnit unit, ZoneId timeZone) {
            this(unit, 1, timeZone);
        }

        TimeUnitRounding(DateTimeUnit unit, int multiplier, ZoneId timeZone) {
            this.unit = unit;
            this.timeZone = timeZone;
            this.unitRoundsToMidnight = this.unit.field.getBaseUnit().getDuration().toMillis() > 3600000L;
            this.multiplier = multiplier;
        }

        TimeUnitRounding(StreamInput in) throws IOException {
            this(DateTimeUnit.resolve(in.readByte()), in.readZoneId());
        }

        @Override
        public void innerWriteTo(StreamOutput out) throws IOException {
            out.writeByte(unit.getId());
            out.writeZoneId(timeZone);
        }

        @Override
        public byte id() {
            return ID;
        }

        private LocalDateTime truncateLocalDateTime(LocalDateTime localDateTime) {
            switch (unit) {
                case SECOND_OF_MINUTE:
                    return localDateTime.withNano(0);

                case MINUTES_OF_HOUR:
                    return LocalDateTime.of(
                        localDateTime.getYear(),
                        localDateTime.getMonthValue(),
                        localDateTime.getDayOfMonth(),
                        localDateTime.getHour(),
                        localDateTime.getMinute(),
                        0,
                        0
                    );

                case HOUR_OF_DAY:
                    return LocalDateTime.of(
                        localDateTime.getYear(),
                        localDateTime.getMonth(),
                        localDateTime.getDayOfMonth(),
                        localDateTime.getHour(),
                        0,
                        0
                    );

                case DAY_OF_MONTH:
                    LocalDate localDate = localDateTime.query(TemporalQueries.localDate());
                    return localDate.atStartOfDay();

                case WEEK_OF_WEEKYEAR:
                    return LocalDateTime.of(localDateTime.toLocalDate(), LocalTime.MIDNIGHT).with(ChronoField.DAY_OF_WEEK, 1);

                case MONTH_OF_YEAR:
                    return LocalDateTime.of(localDateTime.getYear(), localDateTime.getMonthValue(), 1, 0, 0);

                case QUARTER_OF_YEAR:
                    return LocalDateTime.of(localDateTime.getYear(), localDateTime.getMonth().firstMonthOfQuarter(), 1, 0, 0);

                case YEAR_OF_CENTURY:
                    return LocalDateTime.of(LocalDate.of(localDateTime.getYear(), 1, 1), LocalTime.MIDNIGHT);

                default:
                    throw new IllegalArgumentException("NOT YET IMPLEMENTED for unit " + unit);
            }
        }

        @Override
        public Prepared prepare(long minUtcMillis, long maxUtcMillis) {
            /*
             * 128 is a power of two that isn't huge. We might be able to do
             * better if the limit was based on the actual type of prepared
             * rounding but this'll do for now.
             */
            return prepareOffsetOrJavaTimeRounding(minUtcMillis, maxUtcMillis).maybeUseArray(minUtcMillis, maxUtcMillis, 128);
        }

        private TimeUnitPreparedRounding prepareOffsetOrJavaTimeRounding(long minUtcMillis, long maxUtcMillis) {
            /*
             minUtcMillis has to be decreased by 2 units.
             This is because if a minUtcMillis can be rounded down up to unit.extraLocalOffsetLookup
             and that rounding down might still fall within DST gap/overlap.
             Meaning that minUtcMillis has to be decreased by additional unit
             so that the transition just before the minUtcMillis is applied
             */
            long minLookup = minUtcMillis - 2 * unit.extraLocalOffsetLookup();
            long maxLookup = maxUtcMillis;

            long unitMillis = 0;
            if (false == unitRoundsToMidnight) {
                /*
                 * Units that round to midnight can round down from two
                 * units worth of millis in the future to find the
                 * nextRoundingValue.
                 */
                unitMillis = unit.field.getBaseUnit().getDuration().toMillis();
                maxLookup += 2 * unitMillis;
            }
            LocalTimeOffset.Lookup lookup = LocalTimeOffset.lookup(timeZone, minLookup, maxLookup);
            if (lookup == null) {
                // Range too long, just use java.time
                return prepareJavaTime();
            }
            LocalTimeOffset fixedOffset = lookup.fixedInRange(minLookup, maxLookup);
            if (fixedOffset != null) {
                // The time zone is effectively fixed
                if (unitRoundsToMidnight) {
                    return new FixedToMidnightRounding(fixedOffset);
                }
                return new FixedNotToMidnightRounding(fixedOffset, unitMillis);
            }

            if (unitRoundsToMidnight) {
                return new ToMidnightRounding(lookup);
            }
            return new NotToMidnightRounding(lookup, unitMillis);
        }

        @Override
        public Prepared prepareForUnknown() {
            LocalTimeOffset offset = LocalTimeOffset.fixedOffset(timeZone);
            if (offset != null) {
                if (unitRoundsToMidnight) {
                    return new FixedToMidnightRounding(offset);
                }
                return new FixedNotToMidnightRounding(offset, unit.field.getBaseUnit().getDuration().toMillis());
            }
            return prepareJavaTime();
        }

        @Override
        public TimeUnitPreparedRounding prepareJavaTime() {
            if (unitRoundsToMidnight) {
                return new JavaTimeToMidnightRounding();
            }
            return new JavaTimeNotToMidnightRounding(unit.field.getBaseUnit().getDuration().toMillis());
        }

        @Override
        public long offset() {
            return 0;
        }

        @Override
        public Rounding withoutOffset() {
            return this;
        }

        @Override
        public int hashCode() {
            return Objects.hash(unit, timeZone);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            TimeUnitRounding other = (TimeUnitRounding) obj;
            return Objects.equals(unit, other.unit) && Objects.equals(timeZone, other.timeZone);
        }

        @Override
        public String toString() {
            return "Rounding[" + unit + " in " + timeZone + "]";
        }

        private abstract class TimeUnitPreparedRounding extends PreparedRounding {
            @Override
            public double roundingSize(long utcMillis, DateTimeUnit timeUnit) {
                if (unit.isMillisBased) {
                    if (timeUnit.isMillisBased) {
                        return (double) unit.ratio / timeUnit.ratio;
                    } else {
                        throw new IllegalArgumentException(
                            "Cannot use month-based rate unit ["
                                + timeUnit.shortName
                                + "] with non-month based calendar interval histogram ["
                                + unit.shortName
                                + "] only week, day, hour, minute and second are supported for this histogram"
                        );
                    }
                } else {
                    if (timeUnit.isMillisBased) {
                        return (double) (nextRoundingValue(utcMillis) - utcMillis) / timeUnit.ratio;
                    } else {
                        return (double) unit.ratio / timeUnit.ratio;
                    }
                }
            }

            @Override
            public double roundingSize(DateTimeUnit timeUnit) {
                if (unit.isMillisBased) {
                    if (timeUnit.isMillisBased) {
                        return (double) unit.ratio / timeUnit.ratio;
                    } else {
                        throw new IllegalArgumentException(
                            "Cannot use month-based rate unit ["
                                + timeUnit.shortName
                                + "] with non-month based calendar interval histogram ["
                                + unit.shortName
                                + "] only week, day, hour, minute and second are supported for this histogram"
                        );
                    }
                } else {
                    if (timeUnit.isMillisBased) {
                        throw new IllegalArgumentException(
                            "Cannot use non month-based rate unit ["
                                + timeUnit.shortName
                                + "] with calendar interval histogram ["
                                + unit.shortName
                                + "] only month, quarter and year are supported for this histogram"
                        );
                    } else {
                        return (double) unit.ratio / timeUnit.ratio;
                    }
                }
            }

            @Override
            public abstract String toString();
        }

        private class FixedToMidnightRounding extends TimeUnitPreparedRounding {
            private final LocalTimeOffset offset;

            FixedToMidnightRounding(LocalTimeOffset offset) {
                this.offset = offset;
            }

            @Override
            public long round(long utcMillis) {
                return offset.localToUtcInThisOffset(unit.roundFloor(offset.utcToLocalTime(utcMillis), multiplier));
            }

            @Override
            public long nextRoundingValue(long utcMillis) {
                // TODO this is used in date range's collect so we should optimize it too
                return new JavaTimeToMidnightRounding().nextRoundingValue(utcMillis);
            }

            @Override
            public String toString() {
                return TimeUnitRounding.this + "[fixed to midnight]";
            }
        }

        private class FixedNotToMidnightRounding extends TimeUnitPreparedRounding {
            private final LocalTimeOffset offset;
            private final long unitMillis;

            FixedNotToMidnightRounding(LocalTimeOffset offset, long unitMillis) {
                this.offset = offset;
                this.unitMillis = unitMillis;
            }

            @Override
            public long round(long utcMillis) {
                return offset.localToUtcInThisOffset(unit.roundFloor(offset.utcToLocalTime(utcMillis), multiplier));
            }

            @Override
            public final long nextRoundingValue(long utcMillis) {
                return round(utcMillis + unitMillis);
            }

            @Override
            public String toString() {
                return TimeUnitRounding.this + "[fixed to " + unitMillis + "]";
            }
        }

        private class ToMidnightRounding extends TimeUnitPreparedRounding implements LocalTimeOffset.Strategy {
            private final LocalTimeOffset.Lookup lookup;

            ToMidnightRounding(LocalTimeOffset.Lookup lookup) {
                this.lookup = lookup;
            }

            @Override
            public long round(long utcMillis) {
                LocalTimeOffset offset = lookup.lookup(utcMillis);
                return offset.localToUtc(unit.roundFloor(offset.utcToLocalTime(utcMillis), multiplier), this);
            }

            @Override
            public long nextRoundingValue(long utcMillis) {
                // TODO this is actually used date range's collect so we should optimize it
                return new JavaTimeToMidnightRounding().nextRoundingValue(utcMillis);
            }

            @Override
            public long inGap(long localMillis, Gap gap) {
                return gap.startUtcMillis();
            }

            @Override
            public long beforeGap(long localMillis, Gap gap) {
                return gap.previous().localToUtc(localMillis, this);
            }

            @Override
            public long inOverlap(long localMillis, Overlap overlap) {
                return overlap.previous().localToUtc(localMillis, this);
            }

            @Override
            public long beforeOverlap(long localMillis, Overlap overlap) {
                return overlap.previous().localToUtc(localMillis, this);
            }

            @Override
            protected Prepared maybeUseArray(long minUtcMillis, long maxUtcMillis, int max) {
                if (lookup.anyMoveBackToPreviousDay()) {
                    return this;
                }
                return super.maybeUseArray(minUtcMillis, maxUtcMillis, max);
            }

            @Override
            public String toString() {
                return TimeUnitRounding.this + "[across DST to midnight]";
            }
        }

        private class NotToMidnightRounding extends AbstractNotToMidnightRounding implements LocalTimeOffset.Strategy {
            private final LocalTimeOffset.Lookup lookup;

            NotToMidnightRounding(LocalTimeOffset.Lookup lookup, long unitMillis) {
                super(unitMillis);
                this.lookup = lookup;
            }

            @Override
            public long round(long utcMillis) {
                LocalTimeOffset offset = lookup.lookup(utcMillis);
                long roundedLocalMillis = unit.roundFloor(offset.utcToLocalTime(utcMillis), multiplier);
                return offset.localToUtc(roundedLocalMillis, this);
            }

            @Override
            public long inGap(long localMillis, Gap gap) {
                // Round from just before the start of the gap
                return gap.previous().localToUtc(unit.roundFloor(gap.firstMissingLocalTime() - 1, multiplier), this);
            }

            @Override
            public long beforeGap(long localMillis, Gap gap) {
                return inGap(localMillis, gap);
            }

            @Override
            public long inOverlap(long localMillis, Overlap overlap) {
                // Convert the overlap at this offset because that'll produce the largest result.
                return overlap.localToUtcInThisOffset(localMillis);
            }

            @Override
            public long beforeOverlap(long localMillis, Overlap overlap) {
                if (overlap.firstNonOverlappingLocalTime() - overlap.firstOverlappingLocalTime() >= unitMillis) {
                    return overlap.localToUtcInThisOffset(localMillis);
                }
                return overlap.previous().localToUtc(localMillis, this); // This is mostly for Asia/Lord_Howe
            }

            @Override
            public String toString() {
                return TimeUnitRounding.this + "[across DST to " + unitMillis + "]";
            }
        }

        private class JavaTimeToMidnightRounding extends TimeUnitPreparedRounding {
            @Override
            public long round(long utcMillis) {
                LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(utcMillis), timeZone);
                LocalDateTime localMidnight = truncateLocalDateTime(localDateTime);
                return firstTimeOnDay(localMidnight);
            }

            @Override
            public long nextRoundingValue(long utcMillis) {
                LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(utcMillis), timeZone);
                LocalDateTime earlierLocalMidnight = truncateLocalDateTime(localDateTime);
                LocalDateTime localMidnight = nextRelevantMidnight(earlierLocalMidnight);
                return firstTimeOnDay(localMidnight);
            }

            @Override
            protected Prepared maybeUseArray(long minUtcMillis, long maxUtcMillis, int max) {
                // We don't have the right information needed to know if this is safe for this time zone so we always use java rounding
                return this;
            }

            private long firstTimeOnDay(LocalDateTime localMidnight) {
                assert localMidnight.toLocalTime().equals(LocalTime.of(0, 0, 0)) : "firstTimeOnDay should only be called at midnight";

                // Now work out what localMidnight actually means
                final List<ZoneOffset> currentOffsets = timeZone.getRules().getValidOffsets(localMidnight);
                if (currentOffsets.isEmpty() == false) {
                    // There is at least one midnight on this day, so choose the first
                    final ZoneOffset firstOffset = currentOffsets.get(0);
                    final OffsetDateTime offsetMidnight = localMidnight.atOffset(firstOffset);
                    return offsetMidnight.toInstant().toEpochMilli();
                } else {
                    // There were no midnights on this day, so we must have entered the day via an offset transition.
                    // Use the time of the transition as it is the earliest time on the right day.
                    ZoneOffsetTransition zoneOffsetTransition = timeZone.getRules().getTransition(localMidnight);
                    return zoneOffsetTransition.getInstant().toEpochMilli();
                }
            }

            private LocalDateTime nextRelevantMidnight(LocalDateTime localMidnight) {
                assert localMidnight.toLocalTime().equals(LocalTime.MIDNIGHT) : "nextRelevantMidnight should only be called at midnight";

                return switch (unit) {
                    case DAY_OF_MONTH -> localMidnight.plus(1, ChronoUnit.DAYS);
                    case WEEK_OF_WEEKYEAR -> localMidnight.plus(7, ChronoUnit.DAYS);
                    case MONTH_OF_YEAR -> localMidnight.plus(1, ChronoUnit.MONTHS);
                    case QUARTER_OF_YEAR -> localMidnight.plus(3, ChronoUnit.MONTHS);
                    case YEAR_OF_CENTURY -> localMidnight.plus(1, ChronoUnit.YEARS);
                    default -> throw new IllegalArgumentException("Unknown round-to-midnight unit: " + unit);
                };
            }

            @Override
            public String toString() {
                return TimeUnitRounding.this + "[java.time to midnight]";
            }
        }

        private class JavaTimeNotToMidnightRounding extends AbstractNotToMidnightRounding {
            JavaTimeNotToMidnightRounding(long unitMillis) {
                super(unitMillis);
            }

            @Override
            public long round(long utcMillis) {
                Instant instant = Instant.ofEpochMilli(utcMillis);
                final ZoneRules rules = timeZone.getRules();
                while (true) {
                    final Instant truncatedTime = truncateAsLocalTime(instant, rules);
                    final ZoneOffsetTransition previousTransition = rules.previousTransition(instant);

                    if (previousTransition == null) {
                        // truncateAsLocalTime cannot have failed if there were no previous transitions
                        return truncatedTime.toEpochMilli();
                    }

                    Instant previousTransitionInstant = previousTransition.getInstant();
                    if (truncatedTime != null && previousTransitionInstant.compareTo(truncatedTime) < 1) {
                        return truncatedTime.toEpochMilli();
                    }

                    // There was a transition in between the input time and the truncated time. Return to the transition time and
                    // round that down instead.
                    instant = previousTransitionInstant.minusNanos(1_000_000);
                }
            }

            private Instant truncateAsLocalTime(Instant instant, final ZoneRules rules) {
                assert unitRoundsToMidnight == false : "truncateAsLocalTime should not be called if unitRoundsToMidnight";

                LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, timeZone);
                final LocalDateTime truncatedLocalDateTime = truncateLocalDateTime(localDateTime);
                final List<ZoneOffset> currentOffsets = rules.getValidOffsets(truncatedLocalDateTime);

                if (currentOffsets.isEmpty() == false) {
                    // at least one possibilities - choose the latest one that's still no later than the input time
                    for (int offsetIndex = currentOffsets.size() - 1; offsetIndex >= 0; offsetIndex--) {
                        final Instant result = truncatedLocalDateTime.atOffset(currentOffsets.get(offsetIndex)).toInstant();
                        if (result.isAfter(instant) == false) {
                            return result;
                        }
                    }

                    assert false : "rounded time not found for " + instant + " with " + this;
                    return null;
                } else {
                    // The chosen local time didn't happen. This means we were given a time in an hour (or a minute) whose start
                    // is missing due to an offset transition, so the time cannot be truncated.
                    return null;
                }
            }

            @Override
            public String toString() {
                return TimeUnitRounding.this + "[java.time to " + unitMillis + "]";
            }
        }

        private abstract class AbstractNotToMidnightRounding extends TimeUnitPreparedRounding {
            protected final long unitMillis;

            AbstractNotToMidnightRounding(long unitMillis) {
                this.unitMillis = unitMillis;
            }

            @Override
            public final long nextRoundingValue(long utcMillis) {
                final long roundedAfterOneIncrement = round(utcMillis + unitMillis);
                if (utcMillis < roundedAfterOneIncrement) {
                    return roundedAfterOneIncrement;
                } else {
                    return round(utcMillis + 2 * unitMillis);
                }
            }
        }
    }

    static class TimeIntervalRounding extends Rounding {
        static final byte ID = 2;

        private final long interval;
        private final ZoneId timeZone;

        TimeIntervalRounding(long interval, ZoneId timeZone) {
            if (interval < 1) throw new IllegalArgumentException("Zero or negative time interval not supported");
            this.interval = interval;
            this.timeZone = timeZone;
        }

        TimeIntervalRounding(StreamInput in) throws IOException {
            this(in.readVLong(), in.readZoneId());
        }

        @Override
        public void innerWriteTo(StreamOutput out) throws IOException {
            out.writeVLong(interval);
            out.writeZoneId(timeZone);
        }

        @Override
        public byte id() {
            return ID;
        }

        @Override
        public Prepared prepare(long minUtcMillis, long maxUtcMillis) {
            /*
             * 128 is a power of two that isn't huge. We might be able to do
             * better if the limit was based on the actual type of prepared
             * rounding but this'll do for now.
             */
            return prepareOffsetOrJavaTimeRounding(minUtcMillis, maxUtcMillis).maybeUseArray(minUtcMillis, maxUtcMillis, 128);
        }

        private TimeIntervalPreparedRounding prepareOffsetOrJavaTimeRounding(long minUtcMillis, long maxUtcMillis) {
            long minLookup = minUtcMillis - interval;
            long maxLookup = maxUtcMillis;

            LocalTimeOffset.Lookup lookup = LocalTimeOffset.lookup(timeZone, minLookup, maxLookup);
            if (lookup == null) {
                return prepareJavaTime();
            }
            LocalTimeOffset fixedOffset = lookup.fixedInRange(minLookup, maxLookup);
            if (fixedOffset != null) {
                return new FixedRounding(fixedOffset);
            }
            return new VariableRounding(lookup);
        }

        @Override
        public Prepared prepareForUnknown() {
            LocalTimeOffset offset = LocalTimeOffset.fixedOffset(timeZone);
            if (offset != null) {
                return new FixedRounding(offset);
            }
            return prepareJavaTime();
        }

        @Override
        public TimeIntervalPreparedRounding prepareJavaTime() {
            return new JavaTimeRounding();
        }

        @Override
        public long offset() {
            return 0;
        }

        @Override
        public Rounding withoutOffset() {
            return this;
        }

        @Override
        public int hashCode() {
            return Objects.hash(interval, timeZone);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            TimeIntervalRounding other = (TimeIntervalRounding) obj;
            return Objects.equals(interval, other.interval) && Objects.equals(timeZone, other.timeZone);
        }

        @Override
        public String toString() {
            return "Rounding[" + interval + " in " + timeZone + "]";
        }

        private static long roundKey(long value, long interval) {
            if (value < 0) {
                return (value - interval + 1) / interval;
            } else {
                return value / interval;
            }
        }

        private abstract class TimeIntervalPreparedRounding extends PreparedRounding {
            @Override
            public double roundingSize(long utcMillis, DateTimeUnit timeUnit) {
                return roundingSize(timeUnit);
            }

            @Override
            public double roundingSize(DateTimeUnit timeUnit) {
                if (timeUnit.isMillisBased) {
                    return (double) interval / timeUnit.ratio;
                } else {
                    throw new IllegalArgumentException(
                        "Cannot use month-based rate unit ["
                            + timeUnit.shortName
                            + "] with fixed interval based histogram, only week, day, hour, minute and second are supported for "
                            + "this histogram"
                    );
                }
            }

            @Override
            public abstract String toString();
        }

        /**
         * Rounds to down inside of a time zone with an "effectively fixed"
         * time zone. A time zone can be "effectively fixed" if:
         * <ul>
         * <li>It is UTC</li>
         * <li>It is a fixed offset from UTC at all times (UTC-5, America/Phoenix)</li>
         * <li>It is fixed over the entire range of dates that will be rounded</li>
         * </ul>
         */
        private class FixedRounding extends TimeIntervalPreparedRounding {
            private final LocalTimeOffset offset;

            FixedRounding(LocalTimeOffset offset) {
                this.offset = offset;
            }

            @Override
            public long round(long utcMillis) {
                return offset.localToUtcInThisOffset(roundKey(offset.utcToLocalTime(utcMillis), interval) * interval);
            }

            @Override
            public long nextRoundingValue(long utcMillis) {
                // TODO this is used in date range's collect so we should optimize it too
                return new JavaTimeRounding().nextRoundingValue(utcMillis);
            }

            @Override
            public String toString() {
                return TimeIntervalRounding.this + "[fixed]";
            }
        }

        /**
         * Rounds down inside of any time zone, even if it is not
         * "effectively fixed". See {@link FixedRounding} for a description of
         * "effectively fixed".
         */
        private class VariableRounding extends TimeIntervalPreparedRounding implements LocalTimeOffset.Strategy {
            private final LocalTimeOffset.Lookup lookup;

            VariableRounding(LocalTimeOffset.Lookup lookup) {
                this.lookup = lookup;
            }

            @Override
            public long round(long utcMillis) {
                LocalTimeOffset offset = lookup.lookup(utcMillis);
                return offset.localToUtc(roundKey(offset.utcToLocalTime(utcMillis), interval) * interval, this);
            }

            @Override
            public long nextRoundingValue(long utcMillis) {
                // TODO this is used in date range's collect so we should optimize it too
                return new JavaTimeRounding().nextRoundingValue(utcMillis);
            }

            @Override
            public long inGap(long localMillis, Gap gap) {
                return gap.startUtcMillis();
            }

            @Override
            public long beforeGap(long localMillis, Gap gap) {
                return gap.previous().localToUtc(localMillis, this);
            }

            @Override
            public long inOverlap(long localMillis, Overlap overlap) {
                // Convert the overlap at this offset because that'll produce the largest result.
                return overlap.localToUtcInThisOffset(localMillis);
            }

            @Override
            public long beforeOverlap(long localMillis, Overlap overlap) {
                return overlap.previous().localToUtc(roundKey(overlap.firstNonOverlappingLocalTime() - 1, interval) * interval, this);
            }

            @Override
            public String toString() {
                return TimeIntervalRounding.this + "[lookup]";
            }
        }

        /**
         * Rounds down inside of any time zone using {@link LocalDateTime}
         * directly. It'll be slower than {@link VariableRounding} and much
         * slower than {@link FixedRounding}. We use it when we don' have an
         * "effectively fixed" time zone and we can't get a
         * {@link LocalTimeOffset.Lookup}. We might not be able to get one
         * because:
         * <ul>
         * <li>We don't know how to look up the minimum and maximum dates we
         * are going to round.</li>
         * <li>We expect to round over thousands and thousands of years worth
         * of dates with the same {@link Prepared} instance.</li>
         * </ul>
         */
        class JavaTimeRounding extends TimeIntervalPreparedRounding {
            @Override
            public long round(long originalUtcMillis) {
                /*
                 * We give up after 5000 attempts and throw an exception. The
                 * most attempts I could get running locally are 500 - for
                 * Asia/Tehran with an 80,000 day range. You just can't declare
                 * ranges much larger than that in ES right now.
                 */
                return round(originalUtcMillis, 5000);
            }

            long round(long originalUtcMillis, int maxAttempts) {
                long utcMillis = originalUtcMillis;
                int attempts = 0;
                attempt: while (attempts < maxAttempts) {
                    final Instant utcInstant = Instant.ofEpochMilli(utcMillis);
                    final LocalDateTime rawLocalDateTime = LocalDateTime.ofInstant(utcInstant, timeZone);

                    // a millisecond value with the same local time, in UTC, as `utcMillis` has in `timeZone`
                    final long localMillis = utcMillis + timeZone.getRules().getOffset(utcInstant).getTotalSeconds() * 1000;
                    assert localMillis == rawLocalDateTime.toInstant(ZoneOffset.UTC).toEpochMilli();

                    final long roundedMillis = roundKey(localMillis, interval) * interval;
                    final LocalDateTime roundedLocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(roundedMillis), ZoneOffset.UTC);

                    // Now work out what roundedLocalDateTime actually means
                    final List<ZoneOffset> currentOffsets = timeZone.getRules().getValidOffsets(roundedLocalDateTime);
                    if (currentOffsets.isEmpty() == false) {
                        // There is at least one instant with the desired local time. In general the desired result is
                        // the latest rounded time that's no later than the input time, but this could involve rounding across
                        // a timezone transition, which may yield the wrong result
                        final ZoneOffsetTransition previousTransition = timeZone.getRules().previousTransition(utcInstant.plusMillis(1));
                        for (int offsetIndex = currentOffsets.size() - 1; 0 <= offsetIndex; offsetIndex--) {
                            final OffsetDateTime offsetTime = roundedLocalDateTime.atOffset(currentOffsets.get(offsetIndex));
                            final Instant offsetInstant = offsetTime.toInstant();
                            if (previousTransition != null && offsetInstant.isBefore(previousTransition.getInstant())) {
                                /*
                                 * Rounding down across the transition can yield the
                                 * wrong result. It's best to return to the transition
                                 * time and round that down.
                                 */
                                attempts++;
                                utcMillis = previousTransition.getInstant().toEpochMilli() - 1;
                                continue attempt;
                            }

                            if (utcInstant.isBefore(offsetTime.toInstant()) == false) {
                                return offsetInstant.toEpochMilli();
                            }
                        }

                        final OffsetDateTime offsetTime = roundedLocalDateTime.atOffset(currentOffsets.get(0));
                        final Instant offsetInstant = offsetTime.toInstant();
                        assert false : this + " failed to round " + utcMillis + " down: " + offsetInstant + " is the earliest possible";
                        return offsetInstant.toEpochMilli(); // TODO or throw something?
                    } else {
                        // The desired time isn't valid because within a gap, so just return the start of the gap
                        ZoneOffsetTransition zoneOffsetTransition = timeZone.getRules().getTransition(roundedLocalDateTime);
                        return zoneOffsetTransition.getInstant().toEpochMilli();
                    }
                }
                throw new IllegalArgumentException(
                    this
                        + " failed to round "
                        + utcMillis
                        + " down: transitioned backwards through too many daylight savings time transitions"
                );
            }

            @Override
            public long nextRoundingValue(long utcMillis) {
                /*
                 * Ok. I'm not proud of this, but it gets the job done. So here is the deal:
                 * its super important that nextRoundingValue be *exactly* the next rounding
                 * value. And I can't come up with a nice way to use the java time API to figure
                 * it out. Thus, we treat "round" like a black box here and run a kind of whacky
                 * binary search, newton's method hybrid. We don't have a "slope" so we can't do
                 * a "real" newton's method, so we just sort of cut the diff in half. As janky
                 * as it looks, it tends to get the job done in under four iterations. Frankly,
                 * `round(round(utcMillis) + interval)` is usually a good guess so we mostly get
                 * it in a single iteration. But daylight savings time and other janky stuff can
                 * make it less likely.
                 */
                long prevRound = round(utcMillis);
                long increment = interval;
                long from = prevRound;
                int iterations = 0;
                while (++iterations < 100) {
                    from += increment;
                    long rounded = round(from);
                    boolean highEnough = rounded > prevRound;
                    if (false == highEnough) {
                        if (increment < 0) {
                            increment = -increment / 2;
                        }
                        continue;
                    }
                    long roundedRoundedDown = round(rounded - 1);
                    boolean tooHigh = roundedRoundedDown > prevRound;
                    if (tooHigh) {
                        if (increment > 0) {
                            increment = -increment / 2;
                        }
                        continue;
                    }
                    assert highEnough && (false == tooHigh);
                    assert roundedRoundedDown == prevRound;
                    if (iterations > 3 && logger.isDebugEnabled()) {
                        logger.debug("Iterated {} time for {} using {}", iterations, utcMillis, TimeIntervalRounding.this.toString());
                    }
                    return rounded;
                }
                /*
                 * After 100 iterations we still couldn't settle on something! Crazy!
                 * The most I've seen in tests is 20 and its usually 1 or 2. If we're
                 * not in a test let's log something and round from our best guess.
                 */
                assert false
                    : String.format(
                        Locale.ROOT,
                        "Expected to find the rounding in 100 iterations but didn't for [%d] with [%s]",
                        utcMillis,
                        TimeIntervalRounding.this.toString()
                    );
                logger.debug(
                    "Expected to find the rounding in 100 iterations but didn't for {} using {}",
                    utcMillis,
                    TimeIntervalRounding.this.toString()
                );
                return round(from);
            }

            @Override
            public String toString() {
                return TimeIntervalRounding.this + "[java.time]";
            }
        }
    }

    static class OffsetRounding extends Rounding {
        static final byte ID = 3;

        private final Rounding delegate;
        private final long offset;

        OffsetRounding(Rounding delegate, long offset) {
            this.delegate = delegate;
            this.offset = offset;
        }

        OffsetRounding(StreamInput in) throws IOException {
            // Versions before 7.6.0 will never send this type of rounding.
            delegate = Rounding.read(in);
            offset = in.readZLong();
        }

        @Override
        public void innerWriteTo(StreamOutput out) throws IOException {
            delegate.writeTo(out);
            out.writeZLong(offset);
        }

        @Override
        public byte id() {
            return ID;
        }

        @Override
        public Prepared prepare(long minUtcMillis, long maxUtcMillis) {
            return wrapPreparedRounding(delegate.prepare(minUtcMillis - offset, maxUtcMillis - offset));
        }

        @Override
        public Prepared prepareForUnknown() {
            return wrapPreparedRounding(delegate.prepareForUnknown());
        }

        @Override
        public Prepared prepareJavaTime() {
            return wrapPreparedRounding(delegate.prepareJavaTime());
        }

        private Prepared wrapPreparedRounding(Prepared delegatePrepared) {
            return new Prepared() {
                @Override
                public long round(long utcMillis) {
                    return delegatePrepared.round(utcMillis - offset) + offset;
                }

                @Override
                public long nextRoundingValue(long utcMillis) {
                    return delegatePrepared.nextRoundingValue(utcMillis - offset) + offset;
                }

                @Override
                public double roundingSize(long utcMillis, DateTimeUnit timeUnit) {
                    return delegatePrepared.roundingSize(utcMillis, timeUnit);
                }

                @Override
                public double roundingSize(DateTimeUnit timeUnit) {
                    return delegatePrepared.roundingSize(timeUnit);
                }

                @Override
                public long[] fixedRoundingPoints() {
                    // TODO we can likely translate here
                    return null;
                }
            };
        }

        @Override
        public long offset() {
            return offset;
        }

        @Override
        public Rounding withoutOffset() {
            return delegate;
        }

        @Override
        public int hashCode() {
            return Objects.hash(delegate, offset);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            OffsetRounding other = (OffsetRounding) obj;
            return delegate.equals(other.delegate) && offset == other.offset;
        }

        @Override
        public String toString() {
            return delegate + " offset by " + offset;
        }
    }

    public static Rounding read(StreamInput in) throws IOException {
        byte id = in.readByte();
        return switch (id) {
            case TimeUnitRounding.ID -> new TimeUnitRounding(in);
            case TimeIntervalRounding.ID -> new TimeIntervalRounding(in);
            case OffsetRounding.ID -> new OffsetRounding(in);
            default -> throw new ElasticsearchException("unknown rounding id [" + id + "]");
        };
    }

    /**
     * Implementation of {@link Prepared} using pre-calculated "round down" points.
     */
    private static class ArrayRounding implements Prepared {
        private final long[] values;
        private final int max;
        private final Prepared delegate;

        private ArrayRounding(long[] values, int max, Prepared delegate) {
            this.values = values;
            this.max = max;
            this.delegate = delegate;
        }

        @Override
        public long round(long utcMillis) {
            assert values[0] <= utcMillis : utcMillis + " must be after " + values[0];
            int idx = Arrays.binarySearch(values, 0, max, utcMillis);
            assert idx != -1 : "The insertion point is before the array! This should have tripped the assertion above.";
            assert -1 - idx <= values.length : "This insertion point is after the end of the array.";
            if (idx < 0) {
                idx = -2 - idx;
            }
            return values[idx];
        }

        @Override
        public long nextRoundingValue(long utcMillis) {
            return delegate.nextRoundingValue(utcMillis);
        }

        @Override
        public double roundingSize(long utcMillis, DateTimeUnit timeUnit) {
            return delegate.roundingSize(utcMillis, timeUnit);
        }

        @Override
        public double roundingSize(DateTimeUnit timeUnit) {
            return delegate.roundingSize(timeUnit);
        }

        @Override
        public long[] fixedRoundingPoints() {
            return Arrays.copyOf(values, max);
        }
    }
}
