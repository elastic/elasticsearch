/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.xcontent;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class WatcherXContentUtils {

    private WatcherXContentUtils() {
    }

    public static Tuple<XContentType, Object> convertToObject(BytesReference bytes) throws ElasticsearchParseException {
        try {
            XContentParser parser = XContentHelper.createParser(bytes);
            return Tuple.tuple(parser.contentType(), readValue(parser, parser.nextToken()));
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse content to map", e);
        }
    }

    public static String[] readStringArray(XContentParser parser, boolean allowNull) throws IOException {
        if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
            if (allowNull) {
                return null;
            }
            throw new ElasticsearchParseException("could not parse [{}] field. expected a string array but found null value instead",
                    parser.currentName());
        }
        if (parser.currentToken() != XContentParser.Token.START_ARRAY) {
            throw new ElasticsearchParseException("could not parse [{}] field. expected a string array but found [{}] value instead",
                    parser.currentName(), parser.currentToken());
        }

        List<String> list = new ArrayList<>();
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            if (token == XContentParser.Token.VALUE_STRING) {
                list.add(parser.text());
            } else {
                throw new ElasticsearchParseException("could not parse [{}] field. expected a string array but one of the value in the " +
                        "array is [{}]", parser.currentName(), token);
            }
        }
        return list.toArray(new String[list.size()]);
    }

    // TODO open this up in core
    public static List<Object> readList(XContentParser parser, XContentParser.Token token) throws IOException {
        List<Object> list = new ArrayList<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            list.add(readValue(parser, token));
        }
        return list;
    }

    // TODO open this up in core
    public static Object readValue(XContentParser parser, XContentParser.Token token) throws IOException {
        if (token == XContentParser.Token.VALUE_NULL) {
            return null;
        } else if (token == XContentParser.Token.VALUE_STRING) {
            return parser.text();
        } else if (token == XContentParser.Token.VALUE_NUMBER) {
            XContentParser.NumberType numberType = parser.numberType();
            if (numberType == XContentParser.NumberType.INT) {
                return parser.intValue();
            } else if (numberType == XContentParser.NumberType.LONG) {
                return parser.longValue();
            } else if (numberType == XContentParser.NumberType.FLOAT) {
                return parser.floatValue();
            } else if (numberType == XContentParser.NumberType.DOUBLE) {
                return parser.doubleValue();
            }
        } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
            return parser.booleanValue();
        } else if (token == XContentParser.Token.START_OBJECT) {
            return parser.map();
        } else if (token == XContentParser.Token.START_ARRAY) {
            return readList(parser, token);
        } else if (token == XContentParser.Token.VALUE_EMBEDDED_OBJECT) {
            return parser.binaryValue();
        }
        return null;
    }
}
