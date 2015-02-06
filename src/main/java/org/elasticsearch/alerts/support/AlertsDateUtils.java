/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.support;

import org.elasticsearch.alerts.AlertsSettingsException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.core.DateFieldMapper;

import java.io.IOException;

/**
 *
 */
public class AlertsDateUtils {

    public static final FormatDateTimeFormatter dateTimeFormatter = DateFieldMapper.Defaults.DATE_TIME_FORMATTER;

    private AlertsDateUtils() {
    }

    public static DateTime parseDate(String format) {
        return dateTimeFormatter.parser().parseDateTime(format);
    }

    public static String formatDate(DateTime date) {
        return dateTimeFormatter.printer().print(date);
    }

    public static DateTime parseDate(String fieldName, XContentParser.Token token, XContentParser parser) throws IOException {
        if (token == XContentParser.Token.VALUE_NUMBER) {
            return new DateTime(parser.longValue());
        }
        if (token == XContentParser.Token.VALUE_STRING) {
            return dateTimeFormatter.parser().parseDateTime(parser.text());
        }
        if (token == XContentParser.Token.VALUE_NULL) {
            return null;
        }
        throw new AlertsSettingsException("could not parse date/time. expected [" + fieldName +
                    "] to be either a number or a string but was [" + token + "] instead");
    }

    public static void writeDate(StreamOutput out, DateTime date) throws IOException {
        out.writeLong(date.getMillis());
    }

    public static DateTime readDate(StreamInput in) throws IOException {
        return new DateTime(in.readLong());
    }

    public static void writeOptionalDate(StreamOutput out, DateTime date) throws IOException {
        if (date == null) {
            out.writeBoolean(false);
            return;
        }
        out.writeBoolean(true);
        out.writeLong(date.getMillis());
    }

    public static DateTime readOptionalDate(StreamInput in) throws IOException {
        return in.readBoolean() ? new DateTime(in.readLong()) : null;
    }
}
