/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
 * Both formatters support fractional time, up to nanosecond precision.
 */
class EpochTime {

    private static final ValueRange POSITIVE_LONG_INTEGER_RANGE = ValueRange.of(0, Long.MAX_VALUE);

    // TemporalField is only present in the presence of a negative (potentially fractional) timestamp.
    private static final long NEGATIVE_SIGN_PLACEHOLDER = -1;
    private static final EpochField NEGATIVE_SIGN_FIELD = new EpochField(
        ChronoUnit.FOREVER,
        ChronoUnit.FOREVER,
        ValueRange.of(NEGATIVE_SIGN_PLACEHOLDER, NEGATIVE_SIGN_PLACEHOLDER)
    ) {
        @Override
        public boolean isSupportedBy(TemporalAccessor temporal) {
            return temporal.isSupported(ChronoField.INSTANT_SECONDS) && temporal.getLong(ChronoField.INSTANT_SECONDS) < 0;
        }

        @Override
        public long getFrom(TemporalAccessor temporal) {
            return NEGATIVE_SIGN_PLACEHOLDER;
        }
    };

    private static final EpochField UNSIGNED_SECONDS = new EpochField(ChronoUnit.SECONDS, ChronoUnit.FOREVER, POSITIVE_LONG_INTEGER_RANGE) {
        @Override
        public boolean isSupportedBy(TemporalAccessor temporal) {
            return temporal.isSupported(ChronoField.INSTANT_SECONDS);
        }

        @Override
        public long getFrom(TemporalAccessor temporal) {
            long seconds = temporal.getLong(ChronoField.INSTANT_SECONDS);
            if (seconds >= 0) {
                return seconds;
            } else {
                long nanos = temporal.getLong(ChronoField.NANO_OF_SECOND);
                if (nanos != 0) {
                    // Fractional negative timestamp.
                    // This increases the seconds magnitude by 1 in the formatted value due to a rounding error when
                    // the nanos value is not evenly converted into a seconds value. Java 8's date-time API represents
                    // values as a negative seconds value + a positive nanos value. However, in this case there is
                    // precision loss. Thus, to account for this precision loss, we must use a seconds value that is
                    // rounded towards the epoch (i.e. to a higher magnitude).
                    seconds += 1;
                }
                return -seconds; // positive for formatting; sign handled by NEGATIVE_SIGN_FIELD
            }
        }

        @Override
        public TemporalAccessor resolve(
            Map<TemporalField, Long> fieldValues,
            TemporalAccessor partialTemporal,
            ResolverStyle resolverStyle
        ) {
            Long isNegative = fieldValues.remove(NEGATIVE_SIGN_FIELD);
            long seconds = fieldValues.remove(this);
            Long nanos = fieldValues.remove(NANOS_OF_SECOND);
            if (isNegative != null) {
                seconds = -seconds;
                if (nanos != null) {
                    // nanos must be positive. B/c the timestamp is represented by the
                    // (seconds, nanos) tuple, seconds moves 1s toward negative-infinity
                    // and nanos moves 1s toward positive-infinity
                    seconds -= 1;
                    nanos = 1_000_000_000 - nanos;
                }
            }

            fieldValues.put(ChronoField.INSTANT_SECONDS, seconds);
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
            if (temporal.getLong(ChronoField.INSTANT_SECONDS) < 0) {
                return (1_000_000_000 - temporal.getLong(ChronoField.NANO_OF_SECOND)) % 1_000_000_000;
            } else {
                return temporal.getLong(ChronoField.NANO_OF_SECOND);
            }
        }
    };

    private static final EpochField UNSIGNED_MILLIS = new EpochField(ChronoUnit.MILLIS, ChronoUnit.FOREVER, POSITIVE_LONG_INTEGER_RANGE) {
        @Override
        public boolean isSupportedBy(TemporalAccessor temporal) {
            return temporal.isSupported(ChronoField.INSTANT_SECONDS)
                && (temporal.isSupported(ChronoField.NANO_OF_SECOND) || temporal.isSupported(ChronoField.MILLI_OF_SECOND));
        }

        @Override
        public long getFrom(TemporalAccessor temporal) {
            long millis = temporal.getLong(ChronoField.INSTANT_SECONDS) * 1_000;
            if (millis >= 0 || temporal.isSupported(ChronoField.NANO_OF_SECOND) == false) {
                return millis + temporal.getLong(ChronoField.MILLI_OF_SECOND);
            } else {
                long nanos = temporal.getLong(ChronoField.NANO_OF_SECOND);
                if (nanos % 1_000_000 != 0) {
                    // Fractional negative timestamp.
                    // This increases the millis magnitude by 1 in the formatted value due to a rounding error when
                    // the nanos value is not evenly converted into a millis value. Java 8's date-time API represents
                    // values as a negative seconds value + a positive nanos value. However, in this case there is
                    // precision loss when converting the nanos to millis. Thus, to account for this precision loss,
                    // we must use a millis value that is rounded towards the epoch (i.e. to a higher magnitude).
                    millis += 1;
                }
                millis += (nanos / 1_000_000);
                return -millis; // positive for formatting; sign handled by NEGATIVE_SIGN_FIELD
            }
        }

        @Override
        public TemporalAccessor resolve(
            Map<TemporalField, Long> fieldValues,
            TemporalAccessor partialTemporal,
            ResolverStyle resolverStyle
        ) {
            Long isNegative = fieldValues.remove(NEGATIVE_SIGN_FIELD);
            Long nanosOfMilli = fieldValues.remove(NANOS_OF_MILLI);
            long secondsAndMillis = fieldValues.remove(this);

            long seconds;
            long nanos;
            if (isNegative != null) {
                secondsAndMillis = -secondsAndMillis;
                seconds = secondsAndMillis / 1_000;
                nanos = secondsAndMillis % 1000 * 1_000_000;
                // `secondsAndMillis < 0` implies negative timestamp; so `nanos < 0`
                if (nanosOfMilli != null) {
                    // aggregate fractional part of the input; subtract b/c `nanos < 0`
                    nanos -= nanosOfMilli;
                }
                if (nanos != 0) {
                    // nanos must be positive. B/c the timestamp is represented by the
                    // (seconds, nanos) tuple, seconds moves 1s toward negative-infinity
                    // and nanos moves 1s toward positive-infinity
                    seconds -= 1;
                    nanos = 1_000_000_000 + nanos;
                }
            } else {
                seconds = secondsAndMillis / 1_000;
                nanos = secondsAndMillis % 1000 * 1_000_000;

                if (nanosOfMilli != null) {
                    // aggregate fractional part of the input
                    nanos += nanosOfMilli;
                }
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
            return temporal.isSupported(ChronoField.INSTANT_SECONDS)
                && temporal.isSupported(ChronoField.NANO_OF_SECOND)
                && temporal.getLong(ChronoField.NANO_OF_SECOND) % 1_000_000 != 0;
        }

        @Override
        public long getFrom(TemporalAccessor temporal) {
            if (temporal.getLong(ChronoField.INSTANT_SECONDS) < 0) {
                return (1_000_000_000 - temporal.getLong(ChronoField.NANO_OF_SECOND)) % 1_000_000;
            } else {
                return temporal.getLong(ChronoField.NANO_OF_SECOND) % 1_000_000;
            }
        }
    };

    // this supports seconds without any fraction
    private static final DateTimeFormatter SECONDS_FORMATTER1 = new DateTimeFormatterBuilder().optionalStart()
        .appendText(NEGATIVE_SIGN_FIELD, Map.of(-1L, "-")) // field is only created in the presence of a '-' char.
        .optionalEnd()
        .appendValue(UNSIGNED_SECONDS, 1, 19, SignStyle.NOT_NEGATIVE)
        .optionalStart() // optional is used so isSupported will be called when printing
        .appendFraction(NANOS_OF_SECOND, 0, 9, true)
        .optionalEnd()
        .toFormatter(Locale.ROOT);

    // this supports seconds ending in dot
    private static final DateTimeFormatter SECONDS_FORMATTER2 = new DateTimeFormatterBuilder().optionalStart()
        .appendText(NEGATIVE_SIGN_FIELD, Map.of(-1L, "-")) // field is only created in the presence of a '-' char.
        .optionalEnd()
        .appendValue(UNSIGNED_SECONDS, 1, 19, SignStyle.NOT_NEGATIVE)
        .appendLiteral('.')
        .toFormatter(Locale.ROOT);

    // this supports milliseconds
    public static final DateTimeFormatter MILLISECONDS_FORMATTER1 = new DateTimeFormatterBuilder().optionalStart()
        .appendText(NEGATIVE_SIGN_FIELD, Map.of(-1L, "-")) // field is only created in the presence of a '-' char.
        .optionalEnd()
        .appendValue(UNSIGNED_MILLIS, 1, 19, SignStyle.NOT_NEGATIVE)
        .optionalStart()
        .appendFraction(NANOS_OF_MILLI, 0, 6, true)
        .optionalEnd()
        .toFormatter(Locale.ROOT);

    // this supports milliseconds ending in dot
    private static final DateTimeFormatter MILLISECONDS_FORMATTER2 = new DateTimeFormatterBuilder().optionalStart()
        .appendText(NEGATIVE_SIGN_FIELD, Map.of(-1L, "-")) // field is only created in the presence of a '-' char.
        .optionalEnd()
        .appendValue(UNSIGNED_MILLIS, 1, 19, SignStyle.NOT_NEGATIVE)
        .appendLiteral('.')
        .toFormatter(Locale.ROOT);

    static final DateFormatter SECONDS_FORMATTER = new JavaDateFormatter(
        "epoch_second",
        new JavaTimeDateTimePrinter(SECONDS_FORMATTER1),
        JavaTimeDateTimeParser.createRoundUpParserGenerator(builder -> builder.parseDefaulting(ChronoField.NANO_OF_SECOND, 999_999_999L)),
        new JavaTimeDateTimeParser(SECONDS_FORMATTER1),
        new JavaTimeDateTimeParser(SECONDS_FORMATTER2)
    );

    static final DateFormatter MILLIS_FORMATTER = new JavaDateFormatter(
        "epoch_millis",
        new JavaTimeDateTimePrinter(MILLISECONDS_FORMATTER1),
        JavaTimeDateTimeParser.createRoundUpParserGenerator(builder -> builder.parseDefaulting(EpochTime.NANOS_OF_MILLI, 999_999L)),
        new JavaTimeDateTimeParser(MILLISECONDS_FORMATTER1),
        new JavaTimeDateTimeParser(MILLISECONDS_FORMATTER2)
    );

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
