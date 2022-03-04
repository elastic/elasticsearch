/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.common;

import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public final class TimeUtil {

    /**
     * Parse out a Date object given the current parser and field name.
     *
     * @param parser current XContentParser
     * @param fieldName the field's preferred name (utilized in exception)
     * @return parsed Date object
     * @throws IOException from XContentParser
     */
    public static Date parseTimeField(XContentParser parser, String fieldName) throws IOException {
        if (parser.currentToken() == XContentParser.Token.VALUE_NUMBER) {
            return new Date(parser.longValue());
        } else if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
            return new Date(DateFormatters.from(DateTimeFormatter.ISO_INSTANT.parse(parser.text())).toInstant().toEpochMilli());
        }
        throw new IllegalArgumentException("unexpected token [" + parser.currentToken() + "] for [" + fieldName + "]");
    }

    /**
     * Parse out an Instant object given the current parser and field name.
     *
     * @param parser current XContentParser
     * @param fieldName the field's preferred name (utilized in exception)
     * @return parsed Instant object
     * @throws IOException from XContentParser
     */
    public static Instant parseTimeFieldToInstant(XContentParser parser, String fieldName) throws IOException {
        if (parser.currentToken() == XContentParser.Token.VALUE_NUMBER) {
            return Instant.ofEpochMilli(parser.longValue());
        } else if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
            return DateFormatters.from(DateTimeFormatter.ISO_INSTANT.parse(parser.text())).toInstant();
        }
        throw new IllegalArgumentException("unexpected token [" + parser.currentToken() + "] for [" + fieldName + "]");
    }

}
