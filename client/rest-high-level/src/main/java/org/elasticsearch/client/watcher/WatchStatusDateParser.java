/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.client.watcher;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;

public final class WatchStatusDateParser {

    private static final FormatDateTimeFormatter FORMATTER = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER;

    private WatchStatusDateParser() {
        // Prevent instantiation.
    }

    public static DateTime parseDate(String fieldName, XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.VALUE_NUMBER) {
            return new DateTime(parser.longValue(), DateTimeZone.UTC);
        }
        if (token == XContentParser.Token.VALUE_STRING) {
            DateTime dateTime = parseDate(parser.text());
            return dateTime.toDateTime(DateTimeZone.UTC);
        }
        if (token == XContentParser.Token.VALUE_NULL) {
            return null;
        }
        throw new ElasticsearchParseException("could not parse date/time. expected date field [{}] " +
            "to be either a number or a string but found [{}] instead", fieldName, token);
    }

    public static DateTime parseDate(String text) {
        return FORMATTER.parser().parseDateTime(text);
    }
}
