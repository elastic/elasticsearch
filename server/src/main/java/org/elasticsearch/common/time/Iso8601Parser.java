/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.time;

import org.elasticsearch.core.Nullable;

import java.time.DateTimeException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Parses datetimes in ISO8601 format (and subsequences thereof).
 * <p>
 * This is faster than the generic parsing in {@link DateTimeFormatter}, as this is hard-coded and specific to ISO-8601.
 * Various public libraries provide their own variant of this mechanism. We use our own for a few reasons:
 * <ul>
 *     <li>
 *         We are historically a bit more lenient with strings that are invalid according to the strict specification
 *         (eg using a zone region instead of offset for timezone)
 *     </li>
 *     <li>Various built-in formats specify some fields as mandatory and some as optional</li>
 *     <li>Callers can specify defaults for fields that are not present (eg for roundup parsers)</li>
 * </ul>
 * We also do not use exceptions here, instead returning {@code null} for any invalid values, that are then
 * checked and propagated as appropriate.
 */
class Iso8601Parser {

    private static final Set<ChronoField> VALID_SPECIFIED_FIELDS = EnumSet.of(
        ChronoField.YEAR,
        ChronoField.MONTH_OF_YEAR,
        ChronoField.DAY_OF_MONTH,
        ChronoField.HOUR_OF_DAY,
        ChronoField.MINUTE_OF_HOUR,
        ChronoField.SECOND_OF_MINUTE,
        ChronoField.NANO_OF_SECOND
    );

    private static final Set<ChronoField> VALID_DEFAULT_FIELDS = EnumSet.of(
        ChronoField.MONTH_OF_YEAR,
        ChronoField.DAY_OF_MONTH,
        ChronoField.HOUR_OF_DAY,
        ChronoField.MINUTE_OF_HOUR,
        ChronoField.SECOND_OF_MINUTE,
        ChronoField.NANO_OF_SECOND
    );

    private final Set<ChronoField> mandatoryFields;
    private final boolean optionalTime;
    @Nullable
    private final ChronoField maxAllowedField;
    private final DecimalSeparator decimalSeparator;
    private final TimezonePresence timezonePresence;
    private final Map<ChronoField, Integer> defaults;

    /**
     * Constructs a new {@code Iso8601Parser} object
     *
     * @param mandatoryFields  The set of fields that must be present for a valid parse. These should be specified in field order
     *                         (eg if {@link ChronoField#DAY_OF_MONTH} is specified,
     *                         {@link ChronoField#MONTH_OF_YEAR} should also be specified).
     *                         {@link ChronoField#YEAR} is always mandatory.
     * @param optionalTime     {@code false} if the presence of time fields follows {@code mandatoryFields},
     *                         {@code true} if a time component is always optional,
     *                         despite the presence of time fields in {@code mandatoryFields}.
     *                         This makes it possible to specify 'time is optional, but if it is present, it must have these fields'
     *                         by settings {@code optionalTime = true} and putting time fields such as {@link ChronoField#HOUR_OF_DAY}
     *                         and {@link ChronoField#MINUTE_OF_HOUR} in {@code mandatoryFields}.
     * @param maxAllowedField  The most-specific field allowed in the parsed string,
     *                         or {@code null} if everything up to nanoseconds is allowed.
     * @param decimalSeparator The decimal separator that is allowed.
     * @param timezonePresence Specifies if the timezone is optional, mandatory, or forbidden.
     * @param defaults         Map of default field values, if they are not present in the parsed string.
     */
    Iso8601Parser(
        Set<ChronoField> mandatoryFields,
        boolean optionalTime,
        @Nullable ChronoField maxAllowedField,
        DecimalSeparator decimalSeparator,
        TimezonePresence timezonePresence,
        Map<ChronoField, Integer> defaults
    ) {
        checkChronoFields(mandatoryFields, VALID_SPECIFIED_FIELDS);
        if (maxAllowedField != null && VALID_SPECIFIED_FIELDS.contains(maxAllowedField) == false) {
            throw new IllegalArgumentException("Invalid chrono field specified " + maxAllowedField);
        }
        checkChronoFields(defaults.keySet(), VALID_DEFAULT_FIELDS);

        this.mandatoryFields = EnumSet.of(ChronoField.YEAR); // year is always mandatory
        this.mandatoryFields.addAll(mandatoryFields);
        this.optionalTime = optionalTime;
        this.maxAllowedField = maxAllowedField;
        this.decimalSeparator = Objects.requireNonNull(decimalSeparator);
        this.timezonePresence = Objects.requireNonNull(timezonePresence);
        this.defaults = defaults.isEmpty() ? Map.of() : new EnumMap<>(defaults);
    }

    private static void checkChronoFields(Set<ChronoField> fields, Set<ChronoField> validFields) {
        if (fields.isEmpty()) return;   // nothing to check

        fields = EnumSet.copyOf(fields);
        fields.removeAll(validFields);
        if (fields.isEmpty() == false) {
            throw new IllegalArgumentException("Invalid chrono fields specified " + fields);
        }
    }

    boolean optionalTime() {
        return optionalTime;
    }

    Set<ChronoField> mandatoryFields() {
        return mandatoryFields;
    }

    ChronoField maxAllowedField() {
        return maxAllowedField;
    }

    DecimalSeparator decimalSeparator() {
        return decimalSeparator;
    }

    TimezonePresence timezonePresence() {
        return timezonePresence;
    }

    private boolean isOptional(ChronoField field) {
        return mandatoryFields.contains(field) == false;
    }

    private Integer defaultZero(ChronoField field) {
        return defaults.getOrDefault(field, 0);
    }

    /**
     * Attempts to parse {@code str} as an ISO-8601 datetime, returning a {@link ParseResult} indicating if the parse
     * was successful or not, and what fields were present.
     * @param str             The string to parse
     * @param defaultTimezone The default timezone to return, if no timezone is present in the string
     * @return                The {@link ParseResult} of the parse.
     */
    ParseResult tryParse(CharSequence str, @Nullable ZoneId defaultTimezone) {
        if (str.charAt(0) == '-') {
            // the year is negative. This is most unusual.
            // Instead of always adding offsets and dynamically calculating position in the main parser code below,
            // just in case it starts with a -, just parse the substring, then adjust the output appropriately
            ParseResult result = parse(new CharSubSequence(str, 1, str.length()), defaultTimezone);

            if (result.errorIndex() >= 0) {
                return ParseResult.error(result.errorIndex() + 1);
            } else {
                DateTime dt = (DateTime) result.result();
                return new ParseResult(
                    new DateTime(
                        -dt.years(),
                        dt.months(),
                        dt.days(),
                        dt.hours(),
                        dt.minutes(),
                        dt.seconds(),
                        dt.nanos(),
                        dt.zoneId(),
                        dt.offset()
                    )
                );
            }
        } else {
            return parse(str, defaultTimezone);
        }
    }

    /**
     * Index {@code i} is the multiplicand to get the number of nanos from the fractional second with {@code i=9-d} digits.
     */
    private static final int[] NANO_MULTIPLICANDS = new int[] { 1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000 };

    /**
     * Parses {@code str} in ISO8601 format.
     * <p>
     * This parses the string using fixed offsets (it does not support variable-width fields) and separators,
     * sequentially parsing each field and looking for the correct separator.
     * This enables it to be very fast, as all the fields are in fixed places in the string.
     * The only variable aspect comes from the timezone, which (fortunately) is only present at the end of the string,
     * at any point after a time field.
     * It also does not use exceptions, instead returning {@code null} where a value cannot be parsed.
     */
    private ParseResult parse(CharSequence str, @Nullable ZoneId defaultTimezone) {
        int len = str.length();

        // YEARS
        Integer years = parseInt(str, 0, 4);
        if (years == null) return ParseResult.error(0);
        if (len == 4) {
            return isOptional(ChronoField.MONTH_OF_YEAR)
                ? new ParseResult(
                    withZoneOffset(
                        years,
                        defaults.get(ChronoField.MONTH_OF_YEAR),
                        defaults.get(ChronoField.DAY_OF_MONTH),
                        defaults.get(ChronoField.HOUR_OF_DAY),
                        defaults.get(ChronoField.MINUTE_OF_HOUR),
                        defaults.get(ChronoField.SECOND_OF_MINUTE),
                        defaults.get(ChronoField.NANO_OF_SECOND),
                        defaultTimezone
                    )
                )
                : ParseResult.error(4);
        }

        if (str.charAt(4) != '-' || maxAllowedField == ChronoField.YEAR) return ParseResult.error(4);

        // MONTHS
        Integer months = parseInt(str, 5, 7);
        if (months == null || months > 12) return ParseResult.error(5);
        if (len == 7) {
            return isOptional(ChronoField.DAY_OF_MONTH)
                ? new ParseResult(
                    withZoneOffset(
                        years,
                        months,
                        defaults.get(ChronoField.DAY_OF_MONTH),
                        defaults.get(ChronoField.HOUR_OF_DAY),
                        defaults.get(ChronoField.MINUTE_OF_HOUR),
                        defaults.get(ChronoField.SECOND_OF_MINUTE),
                        defaults.get(ChronoField.NANO_OF_SECOND),
                        defaultTimezone
                    )
                )
                : ParseResult.error(7);
        }

        if (str.charAt(7) != '-' || maxAllowedField == ChronoField.MONTH_OF_YEAR) return ParseResult.error(7);

        // DAYS
        Integer days = parseInt(str, 8, 10);
        if (days == null || days > 31) return ParseResult.error(8);
        if (len == 10) {
            return optionalTime || isOptional(ChronoField.HOUR_OF_DAY)
                ? new ParseResult(
                    withZoneOffset(
                        years,
                        months,
                        days,
                        defaults.get(ChronoField.HOUR_OF_DAY),
                        defaults.get(ChronoField.MINUTE_OF_HOUR),
                        defaults.get(ChronoField.SECOND_OF_MINUTE),
                        defaults.get(ChronoField.NANO_OF_SECOND),
                        defaultTimezone
                    )
                )
                : ParseResult.error(10);
        }

        if (str.charAt(10) != 'T' || maxAllowedField == ChronoField.DAY_OF_MONTH) return ParseResult.error(10);
        if (len == 11) {
            return isOptional(ChronoField.HOUR_OF_DAY)
                ? new ParseResult(
                    withZoneOffset(
                        years,
                        months,
                        days,
                        defaults.get(ChronoField.HOUR_OF_DAY),
                        defaults.get(ChronoField.MINUTE_OF_HOUR),
                        defaults.get(ChronoField.SECOND_OF_MINUTE),
                        defaults.get(ChronoField.NANO_OF_SECOND),
                        defaultTimezone
                    )
                )
                : ParseResult.error(11);
        }

        // HOURS + timezone
        Integer hours = parseInt(str, 11, 13);
        if (hours == null || hours > 23) return ParseResult.error(11);
        if (len == 13) {
            return isOptional(ChronoField.MINUTE_OF_HOUR) && timezonePresence != TimezonePresence.MANDATORY
                ? new ParseResult(
                    withZoneOffset(
                        years,
                        months,
                        days,
                        hours,
                        defaultZero(ChronoField.MINUTE_OF_HOUR),
                        defaultZero(ChronoField.SECOND_OF_MINUTE),
                        defaultZero(ChronoField.NANO_OF_SECOND),
                        defaultTimezone
                    )
                )
                : ParseResult.error(13);
        }
        if (isZoneId(str, 13)) {
            ZoneId timezone = parseZoneId(str, 13);
            return timezone != null && isOptional(ChronoField.MINUTE_OF_HOUR)
                ? new ParseResult(
                    withZoneOffset(
                        years,
                        months,
                        days,
                        hours,
                        defaultZero(ChronoField.MINUTE_OF_HOUR),
                        defaultZero(ChronoField.SECOND_OF_MINUTE),
                        defaultZero(ChronoField.NANO_OF_SECOND),
                        timezone
                    )
                )
                : ParseResult.error(13);
        }

        if (str.charAt(13) != ':' || maxAllowedField == ChronoField.HOUR_OF_DAY) return ParseResult.error(13);

        // MINUTES + timezone
        Integer minutes = parseInt(str, 14, 16);
        if (minutes == null || minutes > 59) return ParseResult.error(14);
        if (len == 16) {
            return isOptional(ChronoField.SECOND_OF_MINUTE) && timezonePresence != TimezonePresence.MANDATORY
                ? new ParseResult(
                    withZoneOffset(
                        years,
                        months,
                        days,
                        hours,
                        minutes,
                        defaultZero(ChronoField.SECOND_OF_MINUTE),
                        defaultZero(ChronoField.NANO_OF_SECOND),
                        defaultTimezone
                    )
                )
                : ParseResult.error(16);
        }
        if (isZoneId(str, 16)) {
            ZoneId timezone = parseZoneId(str, 16);
            return timezone != null && isOptional(ChronoField.SECOND_OF_MINUTE)
                ? new ParseResult(
                    withZoneOffset(
                        years,
                        months,
                        days,
                        hours,
                        minutes,
                        defaultZero(ChronoField.SECOND_OF_MINUTE),
                        defaultZero(ChronoField.NANO_OF_SECOND),
                        timezone
                    )
                )
                : ParseResult.error(16);
        }

        if (str.charAt(16) != ':' || maxAllowedField == ChronoField.MINUTE_OF_HOUR) return ParseResult.error(16);

        // SECONDS + timezone
        Integer seconds = parseInt(str, 17, 19);
        if (seconds == null || seconds > 59) return ParseResult.error(17);
        if (len == 19) {
            return isOptional(ChronoField.NANO_OF_SECOND) && timezonePresence != TimezonePresence.MANDATORY
                ? new ParseResult(
                    withZoneOffset(years, months, days, hours, minutes, seconds, defaultZero(ChronoField.NANO_OF_SECOND), defaultTimezone)
                )
                : ParseResult.error(19);
        }
        if (isZoneId(str, 19)) {
            ZoneId timezone = parseZoneId(str, 19);
            return timezone != null
                ? new ParseResult(
                    withZoneOffset(years, months, days, hours, minutes, seconds, defaultZero(ChronoField.NANO_OF_SECOND), timezone)
                )
                : ParseResult.error(19);
        }

        if (checkDecimalSeparator(str.charAt(19)) == false || maxAllowedField == ChronoField.SECOND_OF_MINUTE) return ParseResult.error(19);

        // NANOS + timezone
        // the last number could be millis or nanos, or any combination in the middle
        // so we keep parsing numbers until we get to not a number
        int nanos = 0;
        int pos;
        for (pos = 20; pos < len && pos < 29; pos++) {
            char c = str.charAt(pos);
            if (c < ZERO || c > NINE) break;
            nanos = nanos * 10 + (c - ZERO);
        }

        if (pos == 20) return ParseResult.error(20);   // didn't find a number at all

        // multiply it by the correct multiplicand to get the nanos
        nanos *= NANO_MULTIPLICANDS[29 - pos];

        if (len == pos) {
            return timezonePresence != TimezonePresence.MANDATORY
                ? new ParseResult(withZoneOffset(years, months, days, hours, minutes, seconds, nanos, defaultTimezone))
                : ParseResult.error(pos);
        }
        if (isZoneId(str, pos)) {
            ZoneId timezone = parseZoneId(str, pos);
            return timezone != null
                ? new ParseResult(withZoneOffset(years, months, days, hours, minutes, seconds, nanos, timezone))
                : ParseResult.error(pos);
        }

        // still chars left at the end - string is not valid
        return ParseResult.error(pos);
    }

    private boolean checkDecimalSeparator(char separator) {
        boolean isDot = separator == '.';
        boolean isComma = separator == ',';
        return switch (decimalSeparator) {
            case DOT -> isDot;
            case COMMA -> isComma;
            case BOTH -> isDot || isComma;
        };
    }

    private static boolean isZoneId(CharSequence str, int pos) {
        // all region zoneIds must start with [A-Za-z] (see ZoneId#of)
        // this also covers Z and UT/UTC/GMT zone variants
        char c = str.charAt(pos);
        return c == '+' || c == '-' || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z');
    }

    /**
     * This parses the zone offset, which is of the format accepted by {@link ZoneId#of(String)}.
     * It has fast paths for numerical offsets, but falls back on {@code ZoneId.of} for non-trivial zone ids.
     */
    private ZoneId parseZoneId(CharSequence str, int pos) {
        if (timezonePresence == TimezonePresence.FORBIDDEN) {
            return null;
        }

        int len = str.length();
        char first = str.charAt(pos);

        if (first == 'Z' && len == pos + 1) {
            return ZoneOffset.UTC;
        }

        boolean positive;
        switch (first) {
            case '+' -> positive = true;
            case '-' -> positive = false;
            default -> {
                // non-trivial zone offset, fallback on the built-in java zoneid parser
                try {
                    return ZoneId.of(str.subSequence(pos, str.length()).toString());
                } catch (DateTimeException e) {
                    return null;
                }
            }
        }
        pos++;  // read the + or -

        Integer hours = parseInt(str, pos, pos += 2);
        if (hours == null || hours > 23) return null;
        if (len == pos) return ofHoursMinutesSeconds(hours, 0, 0, positive);

        boolean hasColon = false;
        if (str.charAt(pos) == ':') {
            pos++;
            hasColon = true;
        }

        Integer minutes = parseInt(str, pos, pos += 2);
        if (minutes == null || minutes > 59) return null;
        if (len == pos) return ofHoursMinutesSeconds(hours, minutes, 0, positive);

        // either both dividers have a colon, or neither do
        if ((str.charAt(pos) == ':') != hasColon) return null;
        if (hasColon) {
            pos++;
        }

        Integer seconds = parseInt(str, pos, pos += 2);
        if (seconds == null || seconds > 59) return null;
        if (len == pos) return ofHoursMinutesSeconds(hours, minutes, seconds, positive);

        // there's some text left over...
        return null;
    }

    /*
     * ZoneOffset.ofTotalSeconds has a ConcurrentHashMap cache of offsets. This is fine,
     * but it does mean there's an expensive map lookup every time we call ofTotalSeconds.
     * There's no way to get round that, but we can at least have a very quick last-value cache here
     * to avoid doing a full map lookup when there's lots of timestamps with the same offset being parsed
     */
    private final ThreadLocal<ZoneOffset> lastOffset = ThreadLocal.withInitial(() -> ZoneOffset.UTC);

    private ZoneOffset ofHoursMinutesSeconds(int hours, int minutes, int seconds, boolean positive) {
        int totalSeconds = hours * 3600 + minutes * 60 + seconds;
        if (positive == false) {
            totalSeconds = -totalSeconds;
        }

        // check the lastOffset value
        ZoneOffset lastOffset = this.lastOffset.get();
        if (totalSeconds == lastOffset.getTotalSeconds()) {
            return lastOffset;
        }

        try {
            ZoneOffset offset = ZoneOffset.ofTotalSeconds(totalSeconds);
            this.lastOffset.set(lastOffset);
            return offset;
        } catch (DateTimeException e) {
            // zoneoffset is out of range
            return null;
        }
    }

    /**
     * Create a {@code DateTime} object, with the ZoneOffset field set when the zone is an offset, not just an id.
     */
    private static DateTime withZoneOffset(
        int years,
        Integer months,
        Integer days,
        Integer hours,
        Integer minutes,
        Integer seconds,
        Integer nanos,
        ZoneId zoneId
    ) {
        if (zoneId instanceof ZoneOffset zo) {
            return new DateTime(years, months, days, hours, minutes, seconds, nanos, zoneId, zo);
        } else {
            return new DateTime(years, months, days, hours, minutes, seconds, nanos, zoneId, null);
        }
    }

    private static final char ZERO = '0';
    private static final char NINE = '9';

    private static Integer parseInt(CharSequence str, int startInclusive, int endExclusive) {
        if (str.length() < endExclusive) return null;

        int result = 0;
        for (int i = startInclusive; i < endExclusive; i++) {
            char c = str.charAt(i);
            if (c < ZERO || c > NINE) return null;
            result = result * 10 + (c - ZERO);
        }
        return result;
    }
}
