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

package org.elasticsearch.common.joda;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.time.DateFormatter;
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

public class Joda {

    private static DeprecationLogger deprecationLogger = new DeprecationLogger(LogManager.getLogger(Joda.class));

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

        DateTimeFormatter formatter;
        if ("basicDate".equals(input) || "basic_date".equals(input)) {
            formatter = ISODateTimeFormat.basicDate();
        } else if ("basicDateTime".equals(input) || "basic_date_time".equals(input)) {
            formatter = ISODateTimeFormat.basicDateTime();
        } else if ("basicDateTimeNoMillis".equals(input) || "basic_date_time_no_millis".equals(input)) {
            formatter = ISODateTimeFormat.basicDateTimeNoMillis();
        } else if ("basicOrdinalDate".equals(input) || "basic_ordinal_date".equals(input)) {
            formatter = ISODateTimeFormat.basicOrdinalDate();
        } else if ("basicOrdinalDateTime".equals(input) || "basic_ordinal_date_time".equals(input)) {
            formatter = ISODateTimeFormat.basicOrdinalDateTime();
        } else if ("basicOrdinalDateTimeNoMillis".equals(input) || "basic_ordinal_date_time_no_millis".equals(input)) {
            formatter = ISODateTimeFormat.basicOrdinalDateTimeNoMillis();
        } else if ("basicTime".equals(input) || "basic_time".equals(input)) {
            formatter = ISODateTimeFormat.basicTime();
        } else if ("basicTimeNoMillis".equals(input) || "basic_time_no_millis".equals(input)) {
            formatter = ISODateTimeFormat.basicTimeNoMillis();
        } else if ("basicTTime".equals(input) || "basic_t_time".equals(input)) {
            formatter = ISODateTimeFormat.basicTTime();
        } else if ("basicTTimeNoMillis".equals(input) || "basic_t_time_no_millis".equals(input)) {
            formatter = ISODateTimeFormat.basicTTimeNoMillis();
        } else if ("basicWeekDate".equals(input) || "basic_week_date".equals(input)) {
            formatter = ISODateTimeFormat.basicWeekDate();
        } else if ("basicWeekDateTime".equals(input) || "basic_week_date_time".equals(input)) {
            formatter = ISODateTimeFormat.basicWeekDateTime();
        } else if ("basicWeekDateTimeNoMillis".equals(input) || "basic_week_date_time_no_millis".equals(input)) {
            formatter = ISODateTimeFormat.basicWeekDateTimeNoMillis();
        } else if ("date".equals(input)) {
            formatter = ISODateTimeFormat.date();
        } else if ("dateHour".equals(input) || "date_hour".equals(input)) {
            formatter = ISODateTimeFormat.dateHour();
        } else if ("dateHourMinute".equals(input) || "date_hour_minute".equals(input)) {
            formatter = ISODateTimeFormat.dateHourMinute();
        } else if ("dateHourMinuteSecond".equals(input) || "date_hour_minute_second".equals(input)) {
            formatter = ISODateTimeFormat.dateHourMinuteSecond();
        } else if ("dateHourMinuteSecondFraction".equals(input) || "date_hour_minute_second_fraction".equals(input)) {
            formatter = ISODateTimeFormat.dateHourMinuteSecondFraction();
        } else if ("dateHourMinuteSecondMillis".equals(input) || "date_hour_minute_second_millis".equals(input)) {
            formatter = ISODateTimeFormat.dateHourMinuteSecondMillis();
        } else if ("dateOptionalTime".equals(input) || "date_optional_time".equals(input)) {
            // in this case, we have a separate parser and printer since the dataOptionalTimeParser can't print
            // this sucks we should use the root local by default and not be dependent on the node
            return new JodaDateFormatter(input,
                    ISODateTimeFormat.dateOptionalTimeParser().withLocale(Locale.ROOT).withZone(DateTimeZone.UTC).withDefaultYear(1970),
                    ISODateTimeFormat.dateTime().withLocale(Locale.ROOT).withZone(DateTimeZone.UTC).withDefaultYear(1970));
        } else if ("dateTime".equals(input) || "date_time".equals(input)) {
            formatter = ISODateTimeFormat.dateTime();
        } else if ("dateTimeNoMillis".equals(input) || "date_time_no_millis".equals(input)) {
            formatter = ISODateTimeFormat.dateTimeNoMillis();
        } else if ("hour".equals(input)) {
            formatter = ISODateTimeFormat.hour();
        } else if ("hourMinute".equals(input) || "hour_minute".equals(input)) {
            formatter = ISODateTimeFormat.hourMinute();
        } else if ("hourMinuteSecond".equals(input) || "hour_minute_second".equals(input)) {
            formatter = ISODateTimeFormat.hourMinuteSecond();
        } else if ("hourMinuteSecondFraction".equals(input) || "hour_minute_second_fraction".equals(input)) {
            formatter = ISODateTimeFormat.hourMinuteSecondFraction();
        } else if ("hourMinuteSecondMillis".equals(input) || "hour_minute_second_millis".equals(input)) {
            formatter = ISODateTimeFormat.hourMinuteSecondMillis();
        } else if ("ordinalDate".equals(input) || "ordinal_date".equals(input)) {
            formatter = ISODateTimeFormat.ordinalDate();
        } else if ("ordinalDateTime".equals(input) || "ordinal_date_time".equals(input)) {
            formatter = ISODateTimeFormat.ordinalDateTime();
        } else if ("ordinalDateTimeNoMillis".equals(input) || "ordinal_date_time_no_millis".equals(input)) {
            formatter = ISODateTimeFormat.ordinalDateTimeNoMillis();
        } else if ("time".equals(input)) {
            formatter = ISODateTimeFormat.time();
        } else if ("timeNoMillis".equals(input) || "time_no_millis".equals(input)) {
            formatter = ISODateTimeFormat.timeNoMillis();
        } else if ("tTime".equals(input) || "t_time".equals(input)) {
            formatter = ISODateTimeFormat.tTime();
        } else if ("tTimeNoMillis".equals(input) || "t_time_no_millis".equals(input)) {
            formatter = ISODateTimeFormat.tTimeNoMillis();
        } else if ("weekDate".equals(input) || "week_date".equals(input)) {
            formatter = ISODateTimeFormat.weekDate();
        } else if ("weekDateTime".equals(input) || "week_date_time".equals(input)) {
            formatter = ISODateTimeFormat.weekDateTime();
        } else if ("weekDateTimeNoMillis".equals(input) || "week_date_time_no_millis".equals(input)) {
            formatter = ISODateTimeFormat.weekDateTimeNoMillis();
        } else if ("weekyear".equals(input) || "week_year".equals(input)) {
            formatter = ISODateTimeFormat.weekyear();
        } else if ("weekyearWeek".equals(input) || "weekyear_week".equals(input)) {
            formatter = ISODateTimeFormat.weekyearWeek();
        } else if ("weekyearWeekDay".equals(input) || "weekyear_week_day".equals(input)) {
            formatter = ISODateTimeFormat.weekyearWeekDay();
        } else if ("year".equals(input)) {
            formatter = ISODateTimeFormat.year();
        } else if ("yearMonth".equals(input) || "year_month".equals(input)) {
            formatter = ISODateTimeFormat.yearMonth();
        } else if ("yearMonthDay".equals(input) || "year_month_day".equals(input)) {
            formatter = ISODateTimeFormat.yearMonthDay();
        } else if ("epoch_second".equals(input)) {
            formatter = new DateTimeFormatterBuilder().append(new EpochTimePrinter(false),
                new EpochTimeParser(false)).toFormatter();
        } else if ("epoch_millis".equals(input)) {
            formatter = new DateTimeFormatterBuilder().append(new EpochTimePrinter(true),
                new EpochTimeParser(true)).toFormatter();
        // strict date formats here, must be at least 4 digits for year and two for months and two for day
        } else if ("strictBasicWeekDate".equals(input) || "strict_basic_week_date".equals(input)) {
            formatter = StrictISODateTimeFormat.basicWeekDate();
        } else if ("strictBasicWeekDateTime".equals(input) || "strict_basic_week_date_time".equals(input)) {
            formatter = StrictISODateTimeFormat.basicWeekDateTime();
        } else if ("strictBasicWeekDateTimeNoMillis".equals(input) || "strict_basic_week_date_time_no_millis".equals(input)) {
            formatter = StrictISODateTimeFormat.basicWeekDateTimeNoMillis();
        } else if ("strictDate".equals(input) || "strict_date".equals(input)) {
            formatter = StrictISODateTimeFormat.date();
        } else if ("strictDateHour".equals(input) || "strict_date_hour".equals(input)) {
            formatter = StrictISODateTimeFormat.dateHour();
        } else if ("strictDateHourMinute".equals(input) || "strict_date_hour_minute".equals(input)) {
            formatter = StrictISODateTimeFormat.dateHourMinute();
        } else if ("strictDateHourMinuteSecond".equals(input) || "strict_date_hour_minute_second".equals(input)) {
            formatter = StrictISODateTimeFormat.dateHourMinuteSecond();
        } else if ("strictDateHourMinuteSecondFraction".equals(input) || "strict_date_hour_minute_second_fraction".equals(input)) {
            formatter = StrictISODateTimeFormat.dateHourMinuteSecondFraction();
        } else if ("strictDateHourMinuteSecondMillis".equals(input) || "strict_date_hour_minute_second_millis".equals(input)) {
            formatter = StrictISODateTimeFormat.dateHourMinuteSecondMillis();
        } else if ("strictDateOptionalTime".equals(input) || "strict_date_optional_time".equals(input)) {
            // in this case, we have a separate parser and printer since the dataOptionalTimeParser can't print
            // this sucks we should use the root local by default and not be dependent on the node
            return new JodaDateFormatter(input,
                    StrictISODateTimeFormat.dateOptionalTimeParser().withLocale(Locale.ROOT).withZone(DateTimeZone.UTC)
                        .withDefaultYear(1970),
                    StrictISODateTimeFormat.dateTime().withLocale(Locale.ROOT).withZone(DateTimeZone.UTC).withDefaultYear(1970));
        } else if ("strictDateTime".equals(input) || "strict_date_time".equals(input)) {
            formatter = StrictISODateTimeFormat.dateTime();
        } else if ("strictDateTimeNoMillis".equals(input) || "strict_date_time_no_millis".equals(input)) {
            formatter = StrictISODateTimeFormat.dateTimeNoMillis();
        } else if ("strictHour".equals(input) || "strict_hour".equals(input)) {
            formatter = StrictISODateTimeFormat.hour();
        } else if ("strictHourMinute".equals(input) || "strict_hour_minute".equals(input)) {
            formatter = StrictISODateTimeFormat.hourMinute();
        } else if ("strictHourMinuteSecond".equals(input) || "strict_hour_minute_second".equals(input)) {
            formatter = StrictISODateTimeFormat.hourMinuteSecond();
        } else if ("strictHourMinuteSecondFraction".equals(input) || "strict_hour_minute_second_fraction".equals(input)) {
            formatter = StrictISODateTimeFormat.hourMinuteSecondFraction();
        } else if ("strictHourMinuteSecondMillis".equals(input) || "strict_hour_minute_second_millis".equals(input)) {
            formatter = StrictISODateTimeFormat.hourMinuteSecondMillis();
        } else if ("strictOrdinalDate".equals(input) || "strict_ordinal_date".equals(input)) {
            formatter = StrictISODateTimeFormat.ordinalDate();
        } else if ("strictOrdinalDateTime".equals(input) || "strict_ordinal_date_time".equals(input)) {
            formatter = StrictISODateTimeFormat.ordinalDateTime();
        } else if ("strictOrdinalDateTimeNoMillis".equals(input) || "strict_ordinal_date_time_no_millis".equals(input)) {
            formatter = StrictISODateTimeFormat.ordinalDateTimeNoMillis();
        } else if ("strictTime".equals(input) || "strict_time".equals(input)) {
            formatter = StrictISODateTimeFormat.time();
        } else if ("strictTimeNoMillis".equals(input) || "strict_time_no_millis".equals(input)) {
            formatter = StrictISODateTimeFormat.timeNoMillis();
        } else if ("strictTTime".equals(input) || "strict_t_time".equals(input)) {
            formatter = StrictISODateTimeFormat.tTime();
        } else if ("strictTTimeNoMillis".equals(input) || "strict_t_time_no_millis".equals(input)) {
            formatter = StrictISODateTimeFormat.tTimeNoMillis();
        } else if ("strictWeekDate".equals(input) || "strict_week_date".equals(input)) {
            formatter = StrictISODateTimeFormat.weekDate();
        } else if ("strictWeekDateTime".equals(input) || "strict_week_date_time".equals(input)) {
            formatter = StrictISODateTimeFormat.weekDateTime();
        } else if ("strictWeekDateTimeNoMillis".equals(input) || "strict_week_date_time_no_millis".equals(input)) {
            formatter = StrictISODateTimeFormat.weekDateTimeNoMillis();
        } else if ("strictWeekyear".equals(input) || "strict_weekyear".equals(input)) {
            formatter = StrictISODateTimeFormat.weekyear();
        } else if ("strictWeekyearWeek".equals(input) || "strict_weekyear_week".equals(input)) {
            formatter = StrictISODateTimeFormat.weekyearWeek();
        } else if ("strictWeekyearWeekDay".equals(input) || "strict_weekyear_week_day".equals(input)) {
            formatter = StrictISODateTimeFormat.weekyearWeekDay();
        } else if ("strictYear".equals(input) || "strict_year".equals(input)) {
            formatter = StrictISODateTimeFormat.year();
        } else if ("strictYearMonth".equals(input) || "strict_year_month".equals(input)) {
            formatter = StrictISODateTimeFormat.yearMonth();
        } else if ("strictYearMonthDay".equals(input) || "strict_year_month_day".equals(input)) {
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

    private static void maybeLogJodaDeprecation(String input) {
        if (input.contains("CC")) {
            deprecationLogger.deprecatedAndMaybeLog("joda-century-of-era-format",
                "Use of 'C' (century-of-era) is deprecated and will not be supported in the next major version of Elasticsearch.");
        }
        if (input.contains("YY")) {
            deprecationLogger.deprecatedAndMaybeLog("joda-year-of-era-format", "Use of 'Y' (year-of-era) will change to 'y' in the" +
                " next major version of Elasticsearch. Prefix your date format with '8' to use the new specifier.");
        }
        if (input.contains("xx")) {
            deprecationLogger.deprecatedAndMaybeLog("joda-week-based-year-format","Use of 'x' (week-based-year) will change" +
                " to 'Y' in the next major version of Elasticsearch. Prefix your date format with '8' to use the new specifier.");
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
                    deprecationLogger.deprecatedAndMaybeLog("epoch-negative", "Use of negative values" +
                        " in epoch time formats is deprecated and will not be supported in the next major version of Elasticsearch.");
                }
                if (scientificNotation.matcher(text).find()) {
                    deprecationLogger.deprecatedAndMaybeLog("epoch-scientific-notation", "Use of scientific notation" +
                        " in epoch time formats is deprecated and will not be supported in the next major version of Elasticsearch.");
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
