/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.watcher.support;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.script.JodaCompatibleZonedDateTime;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.time.Clock;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
public class WatcherDateTimeUtils {

    public static final DateFormatter dateTimeFormatter = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER;
    public static final DateMathParser dateMathParser = dateTimeFormatter.toDateMathParser();

    private WatcherDateTimeUtils() {
    }

    public static DateTime convertToDate(Object value, Clock clock) {
        if (value instanceof DateTime) {
            return (DateTime) value;
        }
        if (value instanceof JodaCompatibleZonedDateTime) {
            return new DateTime(((JodaCompatibleZonedDateTime) value).toInstant().toEpochMilli(), DateTimeZone.UTC);
        }
        if (value instanceof String) {
            return parseDateMath((String) value, DateTimeZone.UTC, clock);
        }
        if (value instanceof Number) {
            return new DateTime(((Number) value).longValue(), DateTimeZone.UTC);
        }
        return null;
    }

    public static DateTime parseDate(String dateAsText) {
        return parseDate(dateAsText, null);
    }

    public static DateTime parseDate(String format, DateTimeZone timeZone) {
        DateTime dateTime = dateTimeFormatter.parseJoda(format);
        return timeZone != null ? dateTime.toDateTime(timeZone) : dateTime;
    }

    public static String formatDate(DateTime date) {
        return dateTimeFormatter.formatJoda(date);
    }

    public static DateTime parseDateMath(String fieldName, XContentParser parser, DateTimeZone timeZone, Clock clock) throws IOException {
        if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
            throw new ElasticsearchParseException("could not parse date/time expected date field [{}] to not be null but was null",
                    fieldName);
        }
        return parseDateMathOrNull(fieldName, parser, timeZone, clock);
    }

    public static DateTime parseDateMathOrNull(String fieldName, XContentParser parser, DateTimeZone timeZone,
                                               Clock clock) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.VALUE_NUMBER) {
            return new DateTime(parser.longValue(), timeZone);
        }
        if (token == XContentParser.Token.VALUE_STRING) {
            try {
                return parseDateMath(parser.text(), timeZone, clock);
            } catch (ElasticsearchParseException epe) {
                throw new ElasticsearchParseException("could not parse date/time. expected date field [{}] to be either a number or a " +
                        "DateMath string but found [{}] instead", epe, fieldName, parser.text());
            }
        }
        if (token == XContentParser.Token.VALUE_NULL) {
            return null;
        }
        throw new ElasticsearchParseException("could not parse date/time. expected date field [{}] to be either a number or a string but " +
                "found [{}] instead", fieldName, token);
    }

    public static DateTime parseDateMath(String valueString, DateTimeZone timeZone, final Clock clock) {
        return new DateTime(dateMathParser.parse(valueString, clock::millis), timeZone);
    }

    public static DateTime parseDate(String fieldName, XContentParser parser, DateTimeZone timeZone) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.VALUE_NUMBER) {
            return new DateTime(parser.longValue(), timeZone);
        }
        if (token == XContentParser.Token.VALUE_STRING) {
            return parseDate(parser.text(), timeZone);
        }
        if (token == XContentParser.Token.VALUE_NULL) {
            return null;
        }
        throw new ElasticsearchParseException("could not parse date/time. expected date field [{}] to be either a number or a string but " +
                "found [{}] instead", fieldName, token);
    }

    public static XContentBuilder writeDate(String fieldName, XContentBuilder builder, DateTime date) throws IOException {
        if (date == null) {
            return builder.nullField(fieldName);
        }
        return builder.field(fieldName, formatDate(date));
    }

    public static void writeDate(StreamOutput out, DateTime date) throws IOException {
        out.writeLong(date.getMillis());
    }

    public static DateTime readDate(StreamInput in, DateTimeZone timeZone) throws IOException {
        return new DateTime(in.readLong(), timeZone);
    }

    public static void writeOptionalDate(StreamOutput out, DateTime date) throws IOException {
        if (date == null) {
            out.writeBoolean(false);
            return;
        }
        out.writeBoolean(true);
        out.writeLong(date.getMillis());
    }

    public static DateTime readOptionalDate(StreamInput in, DateTimeZone timeZone) throws IOException {
        return in.readBoolean() ? new DateTime(in.readLong(), timeZone) : null;
    }

    public static TimeValue parseTimeValue(XContentParser parser, String settingName) throws IOException {
        final XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.VALUE_NULL) {
            return null;
        }
        if (token == XContentParser.Token.VALUE_STRING) {
            try {
                TimeValue value = parseTimeValueSupportingFractional(parser.text(), settingName);
                if (value.millis() < 0) {
                    throw new ElasticsearchParseException("could not parse time value [{}]. Time value cannot be negative.", parser.text());
                }
                return value;
            } catch (ElasticsearchParseException epe) {
                throw new ElasticsearchParseException("failed to parse time unit", epe);
            }

        }
        throw new ElasticsearchParseException("could not parse time value. expected either a string or a null value but found [{}] " +
                "instead", token);
    }

    /**
     * Parse a {@link TimeValue} with support for fractional values.
     */
    public static TimeValue parseTimeValueSupportingFractional(@Nullable String sValue, String settingName) {
        // TODO we can potentially remove this in 6.x
        // This code is lifted almost straight from 2.x's TimeValue.java
        Objects.requireNonNull(settingName);
        if (sValue == null) {
            return null;
        }
        try {
            long millis;
            String lowerSValue = sValue.toLowerCase(Locale.ROOT).trim();
            if (lowerSValue.endsWith("ms")) {
                millis = (long) (Double.parseDouble(lowerSValue.substring(0, lowerSValue.length() - 2)));
            } else if (lowerSValue.endsWith("s")) {
                millis = (long) (Double.parseDouble(lowerSValue.substring(0, lowerSValue.length() - 1)) * 1000);
            } else if (lowerSValue.endsWith("m")) {
                millis = (long) (Double.parseDouble(lowerSValue.substring(0, lowerSValue.length() - 1)) * 60 * 1000);
            } else if (lowerSValue.endsWith("h")) {
                millis = (long) (Double.parseDouble(lowerSValue.substring(0, lowerSValue.length() - 1)) * 60 * 60 * 1000);
            } else if (lowerSValue.endsWith("d")) {
                millis = (long) (Double.parseDouble(lowerSValue.substring(0, lowerSValue.length() - 1)) * 24 * 60 * 60 * 1000);
            } else if (lowerSValue.endsWith("w")) {
                millis = (long) (Double.parseDouble(lowerSValue.substring(0, lowerSValue.length() - 1)) * 7 * 24 * 60 * 60 * 1000);
            } else if (lowerSValue.equals("-1")) {
                // Allow this special value to be unit-less:
                millis = -1;
            } else if (lowerSValue.equals("0")) {
                // Allow this special value to be unit-less:
                millis = 0;
            } else {
                throw new ElasticsearchParseException(
                        "Failed to parse setting [{}] with value [{}] as a time value: unit is missing or unrecognized",
                        settingName, sValue);
            }
            return new TimeValue(millis, TimeUnit.MILLISECONDS);
        } catch (NumberFormatException e) {
            throw new ElasticsearchParseException("Failed to parse [{}]", e, sValue);
        }
    }
}
