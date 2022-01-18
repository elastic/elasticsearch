/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.support;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class WatcherDateTimeUtils {

    public static final DateFormatter dateTimeFormatter = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER;
    public static final DateMathParser dateMathParser = dateTimeFormatter.toDateMathParser();

    private WatcherDateTimeUtils() {}

    public static ZonedDateTime convertToDate(Object value, Clock clock) {
        if (value instanceof ZonedDateTime) {
            return (ZonedDateTime) value;
        }
        if (value instanceof String) {
            return parseDateMath((String) value, ZoneOffset.UTC, clock);
        }
        if (value instanceof Number) {
            return Instant.ofEpochMilli(((Number) value).longValue()).atZone(ZoneOffset.UTC);
        }
        return null;
    }

    public static ZonedDateTime parseDate(String dateAsText) {
        return parseDate(dateAsText, null);
    }

    public static ZonedDateTime parseDate(String format, ZoneId timeZone) {
        ZonedDateTime zonedDateTime = DateFormatters.from(dateTimeFormatter.parse(format));
        return timeZone != null ? zonedDateTime.withZoneSameInstant(timeZone) : zonedDateTime;
    }

    public static String formatDate(ZonedDateTime date) {
        return dateTimeFormatter.format(date);
    }

    public static ZonedDateTime parseDateMath(String fieldName, XContentParser parser, ZoneId timeZone, Clock clock) throws IOException {
        if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
            throw new ElasticsearchParseException(
                "could not parse date/time expected date field [{}] to not be null but was null",
                fieldName
            );
        }
        return parseDateMathOrNull(fieldName, parser, timeZone, clock);
    }

    public static ZonedDateTime parseDateMathOrNull(String fieldName, XContentParser parser, ZoneId timeZone, Clock clock)
        throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.VALUE_NUMBER) {
            return Instant.ofEpochMilli(parser.longValue()).atZone(timeZone);
        }
        if (token == XContentParser.Token.VALUE_STRING) {
            try {
                return parseDateMath(parser.text(), timeZone, clock);
            } catch (ElasticsearchParseException epe) {
                throw new ElasticsearchParseException(
                    "could not parse date/time. expected date field [{}] to be either a number or a "
                        + "DateMath string but found [{}] instead",
                    epe,
                    fieldName,
                    parser.text()
                );
            }
        }
        if (token == XContentParser.Token.VALUE_NULL) {
            return null;
        }
        throw new ElasticsearchParseException(
            "could not parse date/time. expected date field [{}] to be either a number or a string but " + "found [{}] instead",
            fieldName,
            token
        );
    }

    public static ZonedDateTime parseDateMath(String valueString, ZoneId timeZone, final Clock clock) {
        return dateMathParser.parse(valueString, clock::millis).atZone(timeZone);
    }

    public static ZonedDateTime parseDate(String fieldName, XContentParser parser, ZoneId timeZone) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.VALUE_NUMBER) {
            return Instant.ofEpochMilli(parser.longValue()).atZone(timeZone);
        }
        if (token == XContentParser.Token.VALUE_STRING) {
            return parseDate(parser.text(), timeZone);
        }
        if (token == XContentParser.Token.VALUE_NULL) {
            return null;
        }
        throw new ElasticsearchParseException(
            "could not parse date/time. expected date field [{}] to be either a number or a string but " + "found [{}] instead",
            fieldName,
            token
        );
    }

    public static XContentBuilder writeDate(String fieldName, XContentBuilder builder, ZonedDateTime date) throws IOException {
        if (date == null) {
            return builder.nullField(fieldName);
        }
        return builder.field(fieldName, formatDate(date));
    }

    public static void writeDate(StreamOutput out, ZonedDateTime date) throws IOException {
        out.writeLong(date.toInstant().toEpochMilli());
    }

    public static void writeOptionalDate(StreamOutput out, ZonedDateTime date) throws IOException {
        if (date == null) {
            out.writeBoolean(false);
            return;
        }
        out.writeBoolean(true);
        out.writeLong(date.toInstant().toEpochMilli());
    }

    public static ZonedDateTime readOptionalDate(StreamInput in) throws IOException {
        return in.readBoolean() ? Instant.ofEpochMilli(in.readLong()).atZone(ZoneOffset.UTC) : null;
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
        throw new ElasticsearchParseException(
            "could not parse time value. expected either a string or a null value but found [{}] " + "instead",
            token
        );
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
                    settingName,
                    sValue
                );
            }
            return new TimeValue(millis, TimeUnit.MILLISECONDS);
        } catch (NumberFormatException e) {
            throw new ElasticsearchParseException("Failed to parse [{}]", e, sValue);
        }
    }
}
