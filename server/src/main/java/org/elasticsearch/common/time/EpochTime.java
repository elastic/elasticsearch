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

package org.elasticsearch.common.time;

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalField;
import java.time.temporal.TemporalUnit;
import java.time.temporal.ValueRange;
import java.util.Locale;
import java.util.Map;

/**
 * This class provides {@link DateTimeFormatter}s capable of parsing epoch seconds and milliseconds.
 * <p>
 * The seconds formatter is provided by {@link #SECONDS_FORMATTER}.
 * The milliseconds formatter is provided by {@link #MILLIS_FORMATTER}.
 * <p>
 * Both formatters support fractional time, up to nanosecond precision. Values must be positive numbers.
 */
class EpochTime {

    private static final ValueRange LONG_POSITIVE_RANGE = ValueRange.of(0, Long.MAX_VALUE);

    private static final EpochField SECONDS = new EpochField(ChronoUnit.SECONDS, ChronoUnit.FOREVER, LONG_POSITIVE_RANGE) {
        @Override
        public boolean isSupportedBy(TemporalAccessor temporal) {
            return temporal.isSupported(ChronoField.INSTANT_SECONDS);
        }
        @Override
        public long getFrom(TemporalAccessor temporal) {
            return temporal.getLong(ChronoField.INSTANT_SECONDS);
        }
        @Override
        public TemporalAccessor resolve(Map<TemporalField,Long> fieldValues,
                                        TemporalAccessor partialTemporal, ResolverStyle resolverStyle) {
            long seconds = fieldValues.remove(this);
            fieldValues.put(ChronoField.INSTANT_SECONDS, seconds);
            Long nanos = fieldValues.remove(NANOS_OF_SECOND);
            if (nanos != null) {
                fieldValues.put(ChronoField.NANO_OF_SECOND, nanos);
            }
            return null;
        }
    };

    private static final EpochField NANOS_OF_SECOND = new EpochField(ChronoUnit.NANOS, ChronoUnit.SECONDS, ValueRange.of(0, 999_999_999)) {
        @Override
        public boolean isSupportedBy(TemporalAccessor temporal) {
            return temporal.isSupported(ChronoField.NANO_OF_SECOND);
        }
        @Override
        public long getFrom(TemporalAccessor temporal) {
            return temporal.getLong(ChronoField.NANO_OF_SECOND);
        }
    };

    private static final EpochField MILLIS = new EpochField(ChronoUnit.MILLIS, ChronoUnit.FOREVER, LONG_POSITIVE_RANGE) {
        @Override
        public boolean isSupportedBy(TemporalAccessor temporal) {
            return temporal.isSupported(ChronoField.INSTANT_SECONDS) && temporal.isSupported(ChronoField.MILLI_OF_SECOND);
        }
        @Override
        public long getFrom(TemporalAccessor temporal) {
            return temporal.getLong(ChronoField.INSTANT_SECONDS) * 1_000 + temporal.getLong(ChronoField.MILLI_OF_SECOND);
        }
        @Override
        public TemporalAccessor resolve(Map<TemporalField,Long> fieldValues,
                                        TemporalAccessor partialTemporal, ResolverStyle resolverStyle) {
            long secondsAndMillis = fieldValues.remove(this);
            long seconds = secondsAndMillis / 1_000;
            long nanos = secondsAndMillis % 1000 * 1_000_000;
            Long nanosOfMilli = fieldValues.remove(NANOS_OF_MILLI);
            if (nanosOfMilli != null) {
                nanos += nanosOfMilli;
            }
            fieldValues.put(ChronoField.INSTANT_SECONDS, seconds);
            fieldValues.put(ChronoField.NANO_OF_SECOND, nanos);
            // if there is already a milli of second, we need to overwrite it
            if (fieldValues.containsKey(ChronoField.MILLI_OF_SECOND)) {
                fieldValues.put(ChronoField.MILLI_OF_SECOND, nanos / 1_000_000);
            }
            return null;
        }
    };

    private static final EpochField NANOS_OF_MILLI = new EpochField(ChronoUnit.NANOS, ChronoUnit.MILLIS, ValueRange.of(0, 999_999)) {
        @Override
        public boolean isSupportedBy(TemporalAccessor temporal) {
            return temporal.isSupported(ChronoField.INSTANT_SECONDS) && temporal.isSupported(ChronoField.NANO_OF_SECOND)
                && temporal.getLong(ChronoField.NANO_OF_SECOND) % 1_000_000 != 0;
        }
        @Override
        public long getFrom(TemporalAccessor temporal) {
            return temporal.getLong(ChronoField.NANO_OF_SECOND) % 1_000_000;
        }
    };

    // this supports seconds without any fraction
    private static final DateTimeFormatter SECONDS_FORMATTER1 = new DateTimeFormatterBuilder()
        .appendValue(SECONDS, 1, 19, SignStyle.NORMAL)
        .optionalStart() // optional is used so isSupported will be called when printing
        .appendFraction(NANOS_OF_SECOND, 0, 9, true)
        .optionalEnd()
        .toFormatter(Locale.ROOT);

    // this supports seconds ending in dot
    private static final DateTimeFormatter SECONDS_FORMATTER2 = new DateTimeFormatterBuilder()
        .appendValue(SECONDS, 1, 19, SignStyle.NORMAL)
        .appendLiteral('.')
        .toFormatter(Locale.ROOT);

    // this supports milliseconds without any fraction
    private static final DateTimeFormatter MILLISECONDS_FORMATTER1 = new DateTimeFormatterBuilder()
        .appendValue(MILLIS, 1, 19, SignStyle.NORMAL)
        .optionalStart()
        .appendFraction(NANOS_OF_MILLI, 0, 6, true)
        .optionalEnd()
        .toFormatter(Locale.ROOT);

    // this supports milliseconds ending in dot
    private static final DateTimeFormatter MILLISECONDS_FORMATTER2 = new DateTimeFormatterBuilder()
        .append(MILLISECONDS_FORMATTER1)
        .appendLiteral('.')
        .toFormatter(Locale.ROOT);

    static final DateFormatter SECONDS_FORMATTER = new JavaDateFormatter("epoch_second", SECONDS_FORMATTER1,
        builder -> builder.parseDefaulting(ChronoField.NANO_OF_SECOND, 999_999_999L),
        SECONDS_FORMATTER1, SECONDS_FORMATTER2);

    static final DateFormatter MILLIS_FORMATTER = new JavaDateFormatter("epoch_millis", MILLISECONDS_FORMATTER1,
        builder -> builder.parseDefaulting(EpochTime.NANOS_OF_MILLI, 999_999L),
        MILLISECONDS_FORMATTER1, MILLISECONDS_FORMATTER2);

    private abstract static class EpochField implements TemporalField {

        private final TemporalUnit baseUnit;
        private final TemporalUnit rangeUnit;
        private final ValueRange range;

        private EpochField(TemporalUnit baseUnit, TemporalUnit rangeUnit, ValueRange range) {
            this.baseUnit = baseUnit;
            this.rangeUnit = rangeUnit;
            this.range = range;
        }

        @Override
        public String getDisplayName(Locale locale) {
            return toString();
        }

        @Override
        public String toString() {
            return "Epoch" + baseUnit.toString() + (rangeUnit != ChronoUnit.FOREVER ? "Of" + rangeUnit.toString() : "");
        }

        @Override
        public TemporalUnit getBaseUnit() {
            return baseUnit;
        }

        @Override
        public TemporalUnit getRangeUnit() {
            return rangeUnit;
        }

        @Override
        public ValueRange range() {
            return range;
        }

        @Override
        public boolean isDateBased() {
            return false;
        }

        @Override
        public boolean isTimeBased() {
            return true;
        }

        @Override
        public ValueRange rangeRefinedBy(TemporalAccessor temporal) {
            return range();
        }

        @SuppressWarnings("unchecked")
        @Override
        public <R extends Temporal> R adjustInto(R temporal, long newValue) {
            return (R) temporal.with(this, newValue);
        }
    }
}
