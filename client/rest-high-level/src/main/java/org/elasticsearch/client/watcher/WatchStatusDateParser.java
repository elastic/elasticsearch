/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.watcher;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.DateFieldMapper;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

public final class WatchStatusDateParser {

    private static final DateFormatter FORMATTER = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER;

    private WatchStatusDateParser() {
        // Prevent instantiation.
    }

    public static ZonedDateTime parseDate(String fieldName, XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.VALUE_NUMBER) {
            return Instant.ofEpochMilli(parser.longValue()).atZone(ZoneOffset.UTC);
        }
        if (token == XContentParser.Token.VALUE_STRING) {
            ZonedDateTime dateTime = parseDate(parser.text());
            return dateTime.withZoneSameInstant(ZoneOffset.UTC);
        }
        if (token == XContentParser.Token.VALUE_NULL) {
            return null;
        }
        throw new ElasticsearchParseException("could not parse date/time. expected date field [{}] " +
            "to be either a number or a string but found [{}] instead", fieldName, token);
    }

    public static ZonedDateTime parseDate(String text) {
        return DateFormatters.from(FORMATTER.parse(text));
    }
}
