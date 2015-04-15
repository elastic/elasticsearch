/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.DateTimeZone;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.watcher.WatcherSettingsException;

import java.io.IOException;

/**
 *
 */
public class WatcherDateUtils {

    public static final FormatDateTimeFormatter dateTimeFormatter = DateFieldMapper.Defaults.DATE_TIME_FORMATTER;

    private WatcherDateUtils() {
    }

    public static DateTime parseDate(String format, DateTimeZone timeZone) {
        DateTime dateTime = dateTimeFormatter.parser().parseDateTime(format);
        return dateTime.toDateTime(timeZone);
    }

    public static String formatDate(DateTime date) {
        return dateTimeFormatter.printer().print(date);
    }

    public static DateTime parseDate(String fieldName, XContentParser.Token token, XContentParser parser, DateTimeZone timeZone) throws IOException {
        if (token == XContentParser.Token.VALUE_NUMBER) {
            return new DateTime(parser.longValue(), timeZone);
        }
        if (token == XContentParser.Token.VALUE_STRING) {
            return parseDate(parser.text(), timeZone);
        }
        if (token == XContentParser.Token.VALUE_NULL) {
            return null;
        }
        throw new WatcherSettingsException("could not parse date/time. expected [" + fieldName +
                    "] to be either a number or a string but was [" + token + "] instead");
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
}
