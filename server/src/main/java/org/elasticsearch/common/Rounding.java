/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.common;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.IsoFields;
import java.time.temporal.TemporalField;
import java.time.temporal.TemporalQueries;
import java.time.zone.ZoneOffsetTransition;
import java.time.zone.ZoneRules;
import java.util.List;
import java.util.Objects;

/**
 * A strategy for rounding date/time based values.
 *
 * There are two implementations for rounding.
 * The first one requires a date time unit and rounds to the supplied date time unit (i.e. quarter of year, day of month)
 * The second one allows you to specify an interval to round to
 */
public abstract class Rounding implements Writeable {

    public enum DateTimeUnit {
        WEEK_OF_WEEKYEAR((byte) 1, IsoFields.WEEK_OF_WEEK_BASED_YEAR) {
            long roundFloor(long utcMillis) {
                return DateUtils.roundWeekOfWeekYear(utcMillis);
            }
        },
        YEAR_OF_CENTURY((byte) 2, ChronoField.YEAR_OF_ERA) {
            long roundFloor(long utcMillis) {
                return DateUtils.roundYear(utcMillis);
            }
        },
        QUARTER_OF_YEAR((byte) 3, IsoFields.QUARTER_OF_YEAR) {
            long roundFloor(long utcMillis) {
                return DateUtils.roundQuarterOfYear(utcMillis);
            }
        },
        MONTH_OF_YEAR((byte) 4, ChronoField.MONTH_OF_YEAR) {
            long roundFloor(long utcMillis) {
                return DateUtils.roundMonthOfYear(utcMillis);
            }
        },
        DAY_OF_MONTH((byte) 5, ChronoField.DAY_OF_MONTH) {
            final long unitMillis = ChronoField.DAY_OF_MONTH.getBaseUnit().getDuration().toMillis();
            long roundFloor(long utcMillis) {
                return DateUtils.roundFloor(utcMillis, unitMillis);
            }
        },
        HOUR_OF_DAY((byte) 6, ChronoField.HOUR_OF_DAY) {
            final long unitMillis = ChronoField.HOUR_OF_DAY.getBaseUnit().getDuration().toMillis();
            long roundFloor(long utcMillis) {
                return DateUtils.roundFloor(utcMillis, unitMillis);
            }
        },
        MINUTES_OF_HOUR((byte) 7, ChronoField.MINUTE_OF_HOUR) {
            final long unitMillis = ChronoField.MINUTE_OF_HOUR.getBaseUnit().getDuration().toMillis();
            long roundFloor(long utcMillis) {
                return DateUtils.roundFloor(utcMillis, unitMillis);
            }
        },
        SECOND_OF_MINUTE((byte) 8, ChronoField.SECOND_OF_MINUTE) {
            final long unitMillis = ChronoField.SECOND_OF_MINUTE.getBaseUnit().getDuration().toMillis();
            long roundFloor(long utcMillis) {
                return DateUtils.roundFloor(utcMillis, unitMillis);
            }
        };

        private final byte id;
        private final TemporalField field;

        DateTimeUnit(byte id, TemporalField field) {
            this.id = id;
            this.field = field;
        }

        /**
         * This rounds down the supplied milliseconds since the epoch down to the next unit. In order to retain performance this method
         * should be as fast as possible and not try to convert dates to java-time objects if possible
         *
         * @param utcMillis the milliseconds since the epoch
         * @return          the rounded down milliseconds since the epoch
         */
        abstract long roundFloor(long utcMillis);

        public byte getId() {
            return id;
        }

        public TemporalField getField() {
            return field;
        }

        public static DateTimeUnit resolve(byte id) {
            switch (id) {
                case 1: return WEEK_OF_WEEKYEAR;
                case 2: return YEAR_OF_CENTURY;
                case 3: return QUARTER_OF_YEAR;
                case 4: return MONTH_OF_YEAR;
                case 5: return DAY_OF_MONTH;
                case 6: return HOUR_OF_DAY;
                case 7: return MINUTES_OF_HOUR;
                case 8: return SECOND_OF_MINUTE;
                default: throw new ElasticsearchException("Unknown date time unit id [" + id + "]");
            }
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
     * Rounds the given value.
     */
    public abstract long round(long value);

    /**
     * Given the rounded value (which was potentially generated by {@link #round(long)}, returns the next rounding value. For example, with
     * interval based rounding, if the interval is 3, {@code nextRoundValue(6) = 9 }.
     *
     * @param value The current rounding value
     * @return The next rounding value
     */
    public abstract long nextRoundingValue(long value);

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

        private ZoneId timeZone = ZoneOffset.UTC;
        private long offset = 0;

        public Builder(DateTimeUnit unit) {
            this.unit = unit;
            this.interval = -1;
        }

        public Builder(TimeValue interval) {
            this.unit = null;
            if (interval.millis() < 1)
                throw new IllegalArgumentException("Zero or negative time interval not supported");
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
                rounding = new TimeUnitRounding(unit, timeZone);
            } else {
                rounding = new TimeIntervalRounding(interval, timeZone);
            }
            if (offset != 0) {
                rounding = new OffsetRounding(rounding, offset);
            }
            return rounding;
        }
    }

    static class TimeUnitRounding extends Rounding {

        static final byte ID = 1;
        /** Since, there is no offset of -1 ms, it is safe to use -1 for non-fixed timezones */
        static final long TZ_OFFSET_NON_FIXED = -1;

        private final DateTimeUnit unit;
        private final ZoneId timeZone;
        private final boolean unitRoundsToMidnight;
        /** For fixed offset time zones, this is the offset in milliseconds, otherwise TZ_OFFSET_NON_FIXED */
        private final long fixedOffsetMillis;

        TimeUnitRounding(DateTimeUnit unit, ZoneId timeZone) {
            this.unit = unit;
            this.timeZone = timeZone;
            this.unitRoundsToMidnight = this.unit.field.getBaseUnit().getDuration().toMillis() > 3600000L;
            this.fixedOffsetMillis = timeZone.getRules().isFixedOffset() == true ?
                timeZone.getRules().getOffset(Instant.EPOCH).getTotalSeconds() * 1000 : TZ_OFFSET_NON_FIXED;
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
                    return LocalDateTime.of(localDateTime.getYear(), localDateTime.getMonthValue(), localDateTime.getDayOfMonth(),
                        localDateTime.getHour(), localDateTime.getMinute(), 0, 0);

                case HOUR_OF_DAY:
                    return LocalDateTime.of(localDateTime.getYear(), localDateTime.getMonth(), localDateTime.getDayOfMonth(),
                        localDateTime.getHour(), 0, 0);

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
        public long round(long utcMillis) {
            // This works as long as the tz offset doesn't change. It is worth getting this case out of the way first,
            // as the calculations for fixing things near to offset changes are a little expensive and unnecessary
            // in the common case of working with fixed offset timezones (such as UTC).
            if (fixedOffsetMillis != TZ_OFFSET_NON_FIXED) {
                long localMillis = utcMillis + fixedOffsetMillis;
                return unit.roundFloor(localMillis) - fixedOffsetMillis;
            }

            Instant instant = Instant.ofEpochMilli(utcMillis);
            if (unitRoundsToMidnight) {
                final LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, timeZone);
                final LocalDateTime localMidnight = truncateLocalDateTime(localDateTime);
                return firstTimeOnDay(localMidnight);
            } else {
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
        }

        private long firstTimeOnDay(LocalDateTime localMidnight) {
            assert localMidnight.toLocalTime().equals(LocalTime.of(0, 0, 0)) : "firstTimeOnDay should only be called at midnight";
            assert unitRoundsToMidnight : "firstTimeOnDay should only be called if unitRoundsToMidnight";

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

        private LocalDateTime nextRelevantMidnight(LocalDateTime localMidnight) {
            assert localMidnight.toLocalTime().equals(LocalTime.MIDNIGHT) : "nextRelevantMidnight should only be called at midnight";
            assert unitRoundsToMidnight : "firstTimeOnDay should only be called if unitRoundsToMidnight";

            switch (unit) {
                case DAY_OF_MONTH:
                    return localMidnight.plus(1, ChronoUnit.DAYS);
                case WEEK_OF_WEEKYEAR:
                    return localMidnight.plus(7, ChronoUnit.DAYS);
                case MONTH_OF_YEAR:
                    return localMidnight.plus(1, ChronoUnit.MONTHS);
                case QUARTER_OF_YEAR:
                    return localMidnight.plus(3, ChronoUnit.MONTHS);
                case YEAR_OF_CENTURY:
                    return localMidnight.plus(1, ChronoUnit.YEARS);
                default:
                    throw new IllegalArgumentException("Unknown round-to-midnight unit: " + unit);
            }
        }

        @Override
        public long nextRoundingValue(long utcMillis) {
            if (unitRoundsToMidnight) {
                final LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(utcMillis), timeZone);
                final LocalDateTime earlierLocalMidnight = truncateLocalDateTime(localDateTime);
                final LocalDateTime localMidnight = nextRelevantMidnight(earlierLocalMidnight);
                return firstTimeOnDay(localMidnight);
            } else {
                final long unitSize = unit.field.getBaseUnit().getDuration().toMillis();
                final long roundedAfterOneIncrement = round(utcMillis + unitSize);
                if (utcMillis < roundedAfterOneIncrement) {
                    return roundedAfterOneIncrement;
                } else {
                    return round(utcMillis + 2 * unitSize);
                }
            }
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
    }

    static class TimeIntervalRounding extends Rounding {
        static final byte ID = 2;
        /** Since, there is no offset of -1 ms, it is safe to use -1 for non-fixed timezones */
        private static final long TZ_OFFSET_NON_FIXED = -1;

        private final long interval;
        private final ZoneId timeZone;
        /** For fixed offset timezones, this is the offset in milliseconds, otherwise TZ_OFFSET_NON_FIXED */
        private final long fixedOffsetMillis;

        TimeIntervalRounding(long interval, ZoneId timeZone) {
            if (interval < 1)
                throw new IllegalArgumentException("Zero or negative time interval not supported");
            this.interval = interval;
            this.timeZone = timeZone;
            this.fixedOffsetMillis = timeZone.getRules().isFixedOffset() == true ?
                timeZone.getRules().getOffset(Instant.EPOCH).getTotalSeconds() * 1000 : TZ_OFFSET_NON_FIXED;
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
        public long round(final long utcMillis) {
            // This works as long as the tz offset doesn't change. It is worth getting this case out of the way first,
            // as the calculations for fixing things near to offset changes are a little expensive and unnecessary
            // in the common case of working with fixed offset timezones (such as UTC).
            if (fixedOffsetMillis != TZ_OFFSET_NON_FIXED) {
                long localMillis = utcMillis + fixedOffsetMillis;
                return (roundKey(localMillis, interval) * interval) - fixedOffsetMillis;
            }

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
                        // Rounding down across the transition can yield the wrong result. It's best to return to the transition time
                        // and round that down.
                        return round(previousTransition.getInstant().toEpochMilli() - 1);
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
                // The desired time isn't valid because within a gap, so just return the gap time.
                ZoneOffsetTransition zoneOffsetTransition = timeZone.getRules().getTransition(roundedLocalDateTime);
                return zoneOffsetTransition.getInstant().toEpochMilli();
            }
        }

        private static long roundKey(long value, long interval) {
            if (value < 0) {
                return (value - interval + 1) / interval;
            } else {
                return value / interval;
            }
        }

        @Override
        public long nextRoundingValue(long time) {
            int offsetSeconds = timeZone.getRules().getOffset(Instant.ofEpochMilli(time)).getTotalSeconds();
            long millis = time + interval + offsetSeconds * 1000;
            return ZonedDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneOffset.UTC)
                .withZoneSameLocal(timeZone)
                .toInstant().toEpochMilli();
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
            if (out.getVersion().before(Version.V_7_6_0)) {
                throw new IllegalArgumentException("Offset rounding not supported before 7.6.0");
            }
            delegate.writeTo(out);
            out.writeZLong(offset);
        }

        @Override
        public byte id() {
            return ID;
        }

        @Override
        public long round(long value) {
            return delegate.round(value - offset) + offset;
        }

        @Override
        public long nextRoundingValue(long value) {
            return delegate.nextRoundingValue(value - offset) + offset;
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
        switch (id) {
            case TimeUnitRounding.ID:
                return new TimeUnitRounding(in);
            case TimeIntervalRounding.ID:
                return new TimeIntervalRounding(in);
            case OffsetRounding.ID:
                return new OffsetRounding(in);
            default:
                throw new ElasticsearchException("unknown rounding id [" + id + "]");
        }
    }
}
