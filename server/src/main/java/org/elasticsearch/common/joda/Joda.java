/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.joda;

import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.common.util.LazyInitializable;
import org.joda.time.Chronology;
import org.joda.time.DateTime;
import org.joda.time.DateTimeField;
import org.joda.time.DateTimeFieldType;
import org.joda.time.DateTimeZone;
import org.joda.time.DurationField;
import org.joda.time.DurationFieldType;
import org.joda.time.ReadablePartial;
import org.joda.time.field.DividedDateTimeField;
import org.joda.time.field.OffsetDateTimeField;
import org.joda.time.field.ScaledDurationField;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;
import org.joda.time.format.DateTimeParserBucket;
import org.joda.time.format.DateTimePrinter;
import org.joda.time.format.ISODateTimeFormat;
import org.joda.time.format.StrictISODateTimeFormat;

import java.io.IOException;
import java.io.Writer;
import java.math.BigDecimal;
import java.util.Locale;
import java.util.regex.Pattern;

@Deprecated
public class Joda {
    // Joda.forPattern could be used even before the logging is initialized.
    // If LogManager.getLogger is called before logging config is loaded
    // it results in errors sent to status logger and startup to fail.
    // Hence a lazy initialization.
    private static final LazyInitializable<DeprecationLogger, RuntimeException> deprecationLogger
        = new LazyInitializable<>(() -> DeprecationLogger.getLogger(Joda.class));
    /**
     * Parses a joda based pattern, including some named ones (similar to the built in Joda ISO ones).
     */
    public static JodaDateFormatter forPattern(String input) {
        if (Strings.hasLength(input)) {
            input = input.trim();
        }
        if (input == null || input.length() == 0) {
            throw new IllegalArgumentException("No date pattern provided");
        }

        FormatNames formatName = FormatNames.forName(input);
        if (formatName != null && formatName.isCamelCase(input)) {
            String msg = "Camel case format name {} is deprecated and will be removed in a future version. " +
                "Use snake case name {} instead.";
            getDeprecationLogger()
                .deprecate(DeprecationCategory.PARSING,"camelCaseDateFormat", msg, formatName.getCamelCaseName(),
                    formatName.getSnakeCaseName());
        }

        DateTimeFormatter formatter;
        if (FormatNames.BASIC_DATE.matches(input)) {
            formatter = ISODateTimeFormat.basicDate();
        } else if (FormatNames.BASIC_DATE_TIME.matches(input) ) {
            formatter = ISODateTimeFormat.basicDateTime();
        } else if (FormatNames.BASIC_DATE_TIME_NO_MILLIS.matches(input) ) {
            formatter = ISODateTimeFormat.basicDateTimeNoMillis();
        } else if (FormatNames.BASIC_ORDINAL_DATE.matches(input) ) {
            formatter = ISODateTimeFormat.basicOrdinalDate();
        } else if (FormatNames.BASIC_ORDINAL_DATE_TIME.matches(input) ) {
            formatter = ISODateTimeFormat.basicOrdinalDateTime();
        } else if (FormatNames.BASIC_ORDINAL_DATE_TIME_NO_MILLIS.matches(input) ) {
            formatter = ISODateTimeFormat.basicOrdinalDateTimeNoMillis();
        } else if (FormatNames.BASIC_TIME.matches(input) ) {
            formatter = ISODateTimeFormat.basicTime();
        } else if (FormatNames.BASIC_TIME_NO_MILLIS.matches(input) ) {
            formatter = ISODateTimeFormat.basicTimeNoMillis();
        } else if (FormatNames.BASIC_T_TIME.matches(input) ) {
            formatter = ISODateTimeFormat.basicTTime();
        } else if (FormatNames.BASIC_T_TIME_NO_MILLIS.matches(input) ) {
            formatter = ISODateTimeFormat.basicTTimeNoMillis();
        } else if (FormatNames.BASIC_WEEK_DATE.matches(input)) {
            formatter = ISODateTimeFormat.basicWeekDate();
        } else if (FormatNames.BASIC_WEEK_DATE_TIME.matches(input) ) {
            formatter = ISODateTimeFormat.basicWeekDateTime();
        } else if (FormatNames.BASIC_WEEK_DATE_TIME_NO_MILLIS.matches(input)) {
            formatter = ISODateTimeFormat.basicWeekDateTimeNoMillis();
        } else if (FormatNames.DATE.matches(input)) {
            formatter = ISODateTimeFormat.date();
        } else if (FormatNames.DATE_HOUR.matches(input)) {
            formatter = ISODateTimeFormat.dateHour();
        } else if (FormatNames.DATE_HOUR_MINUTE.matches(input)) {
            formatter = ISODateTimeFormat.dateHourMinute();
        } else if (FormatNames.DATE_HOUR_MINUTE_SECOND.matches(input)) {
            formatter = ISODateTimeFormat.dateHourMinuteSecond();
        } else if (FormatNames.DATE_HOUR_MINUTE_SECOND_FRACTION.matches(input)) {
            formatter = ISODateTimeFormat.dateHourMinuteSecondFraction();
        } else if (FormatNames.DATE_HOUR_MINUTE_SECOND_MILLIS.matches(input) ) {
            formatter = ISODateTimeFormat.dateHourMinuteSecondMillis();
        } else if (FormatNames.DATE_OPTIONAL_TIME.matches(input)) {
            // in this case, we have a separate parser and printer since the dataOptionalTimeParser can't print
            // this sucks we should use the root local by default and not be dependent on the node
            return new JodaDateFormatter(input,
                    ISODateTimeFormat.dateOptionalTimeParser().withLocale(Locale.ROOT).withZone(DateTimeZone.UTC).withDefaultYear(1970),
                    ISODateTimeFormat.dateTime().withLocale(Locale.ROOT).withZone(DateTimeZone.UTC).withDefaultYear(1970));
        } else if (FormatNames.DATE_TIME.matches(input)) {
            formatter = ISODateTimeFormat.dateTime();
        } else if (FormatNames.DATE_TIME_NO_MILLIS.matches(input) ) {
            formatter = ISODateTimeFormat.dateTimeNoMillis();
        } else if (FormatNames.HOUR.matches(input)) {
            formatter = ISODateTimeFormat.hour();
        } else if (FormatNames.HOUR_MINUTE.matches(input)) {
            formatter = ISODateTimeFormat.hourMinute();
        } else if (FormatNames.HOUR_MINUTE_SECOND.matches(input) ) {
            formatter = ISODateTimeFormat.hourMinuteSecond();
        } else if (FormatNames.HOUR_MINUTE_SECOND_FRACTION.matches(input)) {
            formatter = ISODateTimeFormat.hourMinuteSecondFraction();
        } else if (FormatNames.HOUR_MINUTE_SECOND_MILLIS.matches(input)) {
            formatter = ISODateTimeFormat.hourMinuteSecondMillis();
        } else if (FormatNames.ORDINAL_DATE.matches(input)) {
            formatter = ISODateTimeFormat.ordinalDate();
        } else if (FormatNames.ORDINAL_DATE_TIME.matches(input)) {
            formatter = ISODateTimeFormat.ordinalDateTime();
        } else if (FormatNames.ORDINAL_DATE_TIME_NO_MILLIS.matches(input) ) {
            formatter = ISODateTimeFormat.ordinalDateTimeNoMillis();
        } else if (FormatNames.TIME.matches(input)) {
            formatter = ISODateTimeFormat.time();
        } else if (FormatNames.TIME_NO_MILLIS.matches(input)) {
            formatter = ISODateTimeFormat.timeNoMillis();
        } else if (FormatNames.T_TIME.matches(input)) {
            formatter = ISODateTimeFormat.tTime();
        } else if (FormatNames.T_TIME_NO_MILLIS.matches(input)) {
            formatter = ISODateTimeFormat.tTimeNoMillis();
        } else if (FormatNames.WEEK_DATE.matches(input)) {
            formatter = ISODateTimeFormat.weekDate();
        } else if (FormatNames.WEEK_DATE_TIME.matches(input)) {
            formatter = ISODateTimeFormat.weekDateTime();
        } else if (FormatNames.WEEK_DATE_TIME_NO_MILLIS.matches(input)) {
            formatter = ISODateTimeFormat.weekDateTimeNoMillis();
        } else if (FormatNames.WEEKYEAR.matches(input)) {
            getDeprecationLogger()
                .deprecate(DeprecationCategory.PARSING, "week_year_format_name",
                    "Format name \"week_year\" is deprecated and will be removed in a future version. Use \"weekyear\" format instead");
            formatter = ISODateTimeFormat.weekyear();
        } else if (FormatNames.WEEK_YEAR.matches(input)) {
            formatter = ISODateTimeFormat.weekyear();
        } else if (FormatNames.WEEK_YEAR_WEEK.matches(input)) {
            formatter = ISODateTimeFormat.weekyearWeek();
        } else if (FormatNames.WEEKYEAR_WEEK_DAY.matches(input)) {
            formatter = ISODateTimeFormat.weekyearWeekDay();
        } else if (FormatNames.YEAR.matches(input)) {
            formatter = ISODateTimeFormat.year();
        } else if (FormatNames.YEAR_MONTH.matches(input) ) {
            formatter = ISODateTimeFormat.yearMonth();
        } else if (FormatNames.YEAR_MONTH_DAY.matches(input)) {
            formatter = ISODateTimeFormat.yearMonthDay();
        } else if (FormatNames.EPOCH_SECOND.matches(input)) {
            formatter = new DateTimeFormatterBuilder().append(new EpochTimePrinter(false),
                new EpochTimeParser(false)).toFormatter();
        } else if (FormatNames.EPOCH_MILLIS.matches(input)) {
            formatter = new DateTimeFormatterBuilder().append(new EpochTimePrinter(true),
                new EpochTimeParser(true)).toFormatter();
        // strict date formats here, must be at least 4 digits for year and two for months and two for day
        } else if (FormatNames.STRICT_BASIC_WEEK_DATE.matches(input) ) {
            formatter = StrictISODateTimeFormat.basicWeekDate();
        } else if (FormatNames.STRICT_BASIC_WEEK_DATE_TIME.matches(input)) {
            formatter = StrictISODateTimeFormat.basicWeekDateTime();
        } else if (FormatNames.STRICT_BASIC_WEEK_DATE_TIME_NO_MILLIS.matches(input)) {
            formatter = StrictISODateTimeFormat.basicWeekDateTimeNoMillis();
        } else if (FormatNames.STRICT_DATE.matches(input)) {
            formatter = StrictISODateTimeFormat.date();
        } else if (FormatNames.STRICT_DATE_HOUR.matches(input)) {
            formatter = StrictISODateTimeFormat.dateHour();
        } else if (FormatNames.STRICT_DATE_HOUR_MINUTE.matches(input)) {
            formatter = StrictISODateTimeFormat.dateHourMinute();
        } else if (FormatNames.STRICT_DATE_HOUR_MINUTE_SECOND.matches(input)) {
            formatter = StrictISODateTimeFormat.dateHourMinuteSecond();
        } else if (FormatNames.STRICT_DATE_HOUR_MINUTE_SECOND_FRACTION.matches(input)) {
            formatter = StrictISODateTimeFormat.dateHourMinuteSecondFraction();
        } else if (FormatNames.STRICT_DATE_HOUR_MINUTE_SECOND_MILLIS.matches(input)) {
            formatter = StrictISODateTimeFormat.dateHourMinuteSecondMillis();
        } else if (FormatNames.STRICT_DATE_OPTIONAL_TIME.matches(input)) {
            // in this case, we have a separate parser and printer since the dataOptionalTimeParser can't print
            // this sucks we should use the root local by default and not be dependent on the node
            return new JodaDateFormatter(input,
                    StrictISODateTimeFormat.dateOptionalTimeParser().withLocale(Locale.ROOT).withZone(DateTimeZone.UTC)
                        .withDefaultYear(1970),
                    StrictISODateTimeFormat.dateTime().withLocale(Locale.ROOT).withZone(DateTimeZone.UTC).withDefaultYear(1970));
        } else if (FormatNames.STRICT_DATE_TIME.matches(input)) {
            formatter = StrictISODateTimeFormat.dateTime();
        } else if (FormatNames.STRICT_DATE_TIME_NO_MILLIS.matches(input)) {
            formatter = StrictISODateTimeFormat.dateTimeNoMillis();
        } else if (FormatNames.STRICT_HOUR.matches(input)) {
            formatter = StrictISODateTimeFormat.hour();
        } else if (FormatNames.STRICT_HOUR_MINUTE.matches(input)) {
            formatter = StrictISODateTimeFormat.hourMinute();
        } else if (FormatNames.STRICT_HOUR_MINUTE_SECOND.matches(input)) {
            formatter = StrictISODateTimeFormat.hourMinuteSecond();
        } else if (FormatNames.STRICT_HOUR_MINUTE_SECOND_FRACTION.matches(input)) {
            formatter = StrictISODateTimeFormat.hourMinuteSecondFraction();
        } else if (FormatNames.STRICT_HOUR_MINUTE_SECOND_MILLIS.matches(input)) {
            formatter = StrictISODateTimeFormat.hourMinuteSecondMillis();
        } else if (FormatNames.STRICT_ORDINAL_DATE.matches(input)) {
            formatter = StrictISODateTimeFormat.ordinalDate();
        } else if (FormatNames.STRICT_ORDINAL_DATE_TIME.matches(input)) {
            formatter = StrictISODateTimeFormat.ordinalDateTime();
        } else if (FormatNames.STRICT_ORDINAL_DATE_TIME_NO_MILLIS.matches(input)) {
            formatter = StrictISODateTimeFormat.ordinalDateTimeNoMillis();
        } else if (FormatNames.STRICT_TIME.matches(input)) {
            formatter = StrictISODateTimeFormat.time();
        } else if (FormatNames.STRICT_TIME_NO_MILLIS.matches(input)) {
            formatter = StrictISODateTimeFormat.timeNoMillis();
        } else if (FormatNames.STRICT_T_TIME.matches(input) ) {
            formatter = StrictISODateTimeFormat.tTime();
        } else if (FormatNames.STRICT_T_TIME_NO_MILLIS.matches(input)) {
            formatter = StrictISODateTimeFormat.tTimeNoMillis();
        } else if (FormatNames.STRICT_WEEK_DATE.matches(input)) {
            formatter = StrictISODateTimeFormat.weekDate();
        } else if (FormatNames.STRICT_WEEK_DATE_TIME.matches(input)) {
            formatter = StrictISODateTimeFormat.weekDateTime();
        } else if (FormatNames.STRICT_WEEK_DATE_TIME_NO_MILLIS.matches(input)) {
            formatter = StrictISODateTimeFormat.weekDateTimeNoMillis();
        } else if (FormatNames.STRICT_WEEKYEAR.matches(input) ) {
            formatter = StrictISODateTimeFormat.weekyear();
        } else if (FormatNames.STRICT_WEEKYEAR_WEEK.matches(input)) {
            formatter = StrictISODateTimeFormat.weekyearWeek();
        } else if (FormatNames.STRICT_WEEKYEAR_WEEK_DAY.matches(input)) {
            formatter = StrictISODateTimeFormat.weekyearWeekDay();
        } else if (FormatNames.STRICT_YEAR.matches(input)) {
            formatter = StrictISODateTimeFormat.year();
        } else if (FormatNames.STRICT_YEAR_MONTH.matches(input)) {
            formatter = StrictISODateTimeFormat.yearMonth();
        } else if (FormatNames.STRICT_YEAR_MONTH_DAY.matches(input)) {
            formatter = StrictISODateTimeFormat.yearMonthDay();
        } else if (Strings.hasLength(input) && input.contains("||")) {
            String[] formats = Strings.delimitedListToStringArray(input, "||");
            DateTimeParser[] parsers = new DateTimeParser[formats.length];

            if (formats.length == 1) {
                formatter = forPattern(input).parser;
            } else {
                DateTimeFormatter dateTimeFormatter = null;
                for (int i = 0; i < formats.length; i++) {
                    JodaDateFormatter currentFormatter = forPattern(formats[i]);
                    DateTimeFormatter currentParser = currentFormatter.parser;
                    if (dateTimeFormatter == null) {
                        dateTimeFormatter = currentFormatter.printer;
                    }
                    parsers[i] = currentParser.getParser();
                }

                DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder()
                    .append(dateTimeFormatter.withZone(DateTimeZone.UTC).getPrinter(), parsers);
                formatter = builder.toFormatter();
            }
        } else {
            try {
                maybeLogJodaDeprecation(input);
                formatter = DateTimeFormat.forPattern(input);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid format: [" + input + "]: " + e.getMessage(), e);
            }
        }

        formatter = formatter.withLocale(Locale.ROOT).withZone(DateTimeZone.UTC).withDefaultYear(1970);
        return new JodaDateFormatter(input, formatter, formatter);
    }

    private static void maybeLogJodaDeprecation(String format) {
        if (JodaDeprecationPatterns.isDeprecatedPattern(format)) {
            String suggestion = JodaDeprecationPatterns.formatSuggestion(format);
            getDeprecationLogger().deprecate(DeprecationCategory.PARSING, "joda-pattern-deprecation",
                suggestion + " " + JodaDeprecationPatterns.USE_NEW_FORMAT_SPECIFIERS);
        }
    }

    public static DateFormatter getStrictStandardDateFormatter() {
        // 2014/10/10
        DateTimeFormatter shortFormatter = new DateTimeFormatterBuilder()
                .appendFixedDecimal(DateTimeFieldType.year(), 4)
                .appendLiteral('/')
                .appendFixedDecimal(DateTimeFieldType.monthOfYear(), 2)
                .appendLiteral('/')
                .appendFixedDecimal(DateTimeFieldType.dayOfMonth(), 2)
                .toFormatter()
                .withZoneUTC();

        // 2014/10/10 12:12:12
        DateTimeFormatter longFormatter = new DateTimeFormatterBuilder()
                .appendFixedDecimal(DateTimeFieldType.year(), 4)
                .appendLiteral('/')
                .appendFixedDecimal(DateTimeFieldType.monthOfYear(), 2)
                .appendLiteral('/')
                .appendFixedDecimal(DateTimeFieldType.dayOfMonth(), 2)
                .appendLiteral(' ')
                .appendFixedSignedDecimal(DateTimeFieldType.hourOfDay(), 2)
                .appendLiteral(':')
                .appendFixedSignedDecimal(DateTimeFieldType.minuteOfHour(), 2)
                .appendLiteral(':')
                .appendFixedSignedDecimal(DateTimeFieldType.secondOfMinute(), 2)
                .toFormatter()
                .withZoneUTC();

        DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder().append(longFormatter.withZone(DateTimeZone.UTC).getPrinter(),
            new DateTimeParser[]{longFormatter.getParser(), shortFormatter.getParser(), new EpochTimeParser(true)});

        DateTimeFormatter formatter = builder.toFormatter().withLocale(Locale.ROOT).withZone(DateTimeZone.UTC).withDefaultYear(1970);
        return new JodaDateFormatter("yyyy/MM/dd HH:mm:ss||yyyy/MM/dd||epoch_millis", formatter, formatter);
    }


    public static final DurationFieldType Quarters = new DurationFieldType("quarters") {
        @Override
        public DurationField getField(Chronology chronology) {
            return new ScaledDurationField(chronology.months(), Quarters, 3);
        }
    };

    public static final DateTimeFieldType QuarterOfYear = new DateTimeFieldType("quarterOfYear") {
        @Override
        public DurationFieldType getDurationType() {
            return Quarters;
        }

        @Override
        public DurationFieldType getRangeDurationType() {
            return DurationFieldType.years();
        }

        @Override
        public DateTimeField getField(Chronology chronology) {
            return new OffsetDateTimeField(
                new DividedDateTimeField(new OffsetDateTimeField(chronology.monthOfYear(), -1), QuarterOfYear, 3), 1);
        }
    };

    /**
     * Checks if a pattern is Joda-style.
     * Joda style patterns are not always compatible with java.time patterns.
     * @param version - creation version of the index where pattern was used
     * @param pattern - the pattern to check
     * @return - true if pattern is joda style, otherwise false
     */
    public static boolean isJodaPattern(Version version, String pattern) {
        return version.before(Version.V_7_0_0)
            && pattern.startsWith("8") == false;
    }

    public static class EpochTimeParser implements DateTimeParser {

        private static final Pattern scientificNotation = Pattern.compile("[Ee]");

        private final boolean hasMilliSecondPrecision;

        public EpochTimeParser(boolean hasMilliSecondPrecision) {
            this.hasMilliSecondPrecision = hasMilliSecondPrecision;
        }

        @Override
        public int estimateParsedLength() {
            return hasMilliSecondPrecision ? 19 : 16;
        }

        @Override
        public int parseInto(DateTimeParserBucket bucket, String text, int position) {
            boolean isPositive = text.startsWith("-") == false;
            int firstDotIndex = text.indexOf('.');
            boolean isTooLong = (firstDotIndex == -1 ? text.length() : firstDotIndex) > estimateParsedLength();

            if (bucket.getZone() != DateTimeZone.UTC) {
                String format = hasMilliSecondPrecision ? "epoch_millis" : "epoch_second";
                throw new IllegalArgumentException("time_zone must be UTC for format [" + format + "]");
            } else if (isPositive && isTooLong) {
                return -1;
            }

            int factor = hasMilliSecondPrecision ? 1 : 1000;
            try {
                long millis = new BigDecimal(text).longValue() * factor;
                // check for deprecations, but after it has parsed correctly so invalid values aren't counted as deprecated
                if (millis < 0) {
                    getDeprecationLogger().deprecate(DeprecationCategory.PARSING, "epoch-negative", "Use of negative values" +
                        " in epoch time formats is deprecated and will not be supported in the next major version of Elasticsearch.");
                }
                if (scientificNotation.matcher(text).find()) {
                    getDeprecationLogger().deprecate(DeprecationCategory.PARSING, "epoch-scientific-notation",
                        "Use of scientific notation in epoch time formats is deprecated and will not be supported in the "
                            + "next major version of Elasticsearch.");
                }
                DateTime dt = new DateTime(millis, DateTimeZone.UTC);
                bucket.saveField(DateTimeFieldType.year(), dt.getYear());
                bucket.saveField(DateTimeFieldType.monthOfYear(), dt.getMonthOfYear());
                bucket.saveField(DateTimeFieldType.dayOfMonth(), dt.getDayOfMonth());
                bucket.saveField(DateTimeFieldType.hourOfDay(), dt.getHourOfDay());
                bucket.saveField(DateTimeFieldType.minuteOfHour(), dt.getMinuteOfHour());
                bucket.saveField(DateTimeFieldType.secondOfMinute(), dt.getSecondOfMinute());
                bucket.saveField(DateTimeFieldType.millisOfSecond(), dt.getMillisOfSecond());
                bucket.setZone(DateTimeZone.UTC);
            } catch (Exception e) {
                return -1;
            }
            return text.length();
        }
    }

    private static DeprecationLogger getDeprecationLogger() {
        return deprecationLogger.getOrCompute();
    }

    public static class EpochTimePrinter implements DateTimePrinter {

        private boolean hasMilliSecondPrecision;

        public EpochTimePrinter(boolean hasMilliSecondPrecision) {
            this.hasMilliSecondPrecision = hasMilliSecondPrecision;
        }

        @Override
        public int estimatePrintedLength() {
            return hasMilliSecondPrecision ? 19 : 16;
        }


        /**
         * We adjust the instant by displayOffset to adjust for the offset that might have been added in
         * {@link DateTimeFormatter#printTo(Appendable, long, Chronology)} when using a time zone.
         */
        @Override
        public void printTo(StringBuffer buf, long instant, Chronology chrono, int displayOffset, DateTimeZone displayZone, Locale locale) {
            if (hasMilliSecondPrecision) {
                buf.append(instant - displayOffset);
            } else {
                buf.append((instant  - displayOffset) / 1000);
            }
        }

        /**
         * We adjust the instant by displayOffset to adjust for the offset that might have been added in
         * {@link DateTimeFormatter#printTo(Appendable, long, Chronology)} when using a time zone.
         */
        @Override
        public void printTo(Writer out, long instant, Chronology chrono, int displayOffset,
                            DateTimeZone displayZone, Locale locale) throws IOException {
            if (hasMilliSecondPrecision) {
                out.write(String.valueOf(instant - displayOffset));
            } else {
                out.append(String.valueOf((instant - displayOffset) / 1000));
            }
        }

        @Override
        public void printTo(StringBuffer buf, ReadablePartial partial, Locale locale) {
            if (hasMilliSecondPrecision) {
                buf.append(String.valueOf(getDateTimeMillis(partial)));
            } else {
                buf.append(String.valueOf(getDateTimeMillis(partial) / 1000));
            }
        }

        @Override
        public void printTo(Writer out, ReadablePartial partial, Locale locale) throws IOException {
            if (hasMilliSecondPrecision) {
                out.append(String.valueOf(getDateTimeMillis(partial)));
            } else {
                out.append(String.valueOf(getDateTimeMillis(partial) / 1000));
            }
        }

        private long getDateTimeMillis(ReadablePartial partial) {
            int year = partial.get(DateTimeFieldType.year());
            int monthOfYear = partial.get(DateTimeFieldType.monthOfYear());
            int dayOfMonth = partial.get(DateTimeFieldType.dayOfMonth());
            int hourOfDay = partial.get(DateTimeFieldType.hourOfDay());
            int minuteOfHour = partial.get(DateTimeFieldType.minuteOfHour());
            int secondOfMinute = partial.get(DateTimeFieldType.secondOfMinute());
            int millisOfSecond = partial.get(DateTimeFieldType.millisOfSecond());
            return partial.getChronology().getDateTimeMillis(year, monthOfYear, dayOfMonth,
                hourOfDay, minuteOfHour, secondOfMinute, millisOfSecond);
        }
    }
}
