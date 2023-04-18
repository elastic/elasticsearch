/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.proto.content;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import org.elasticsearch.xpack.sql.proto.core.Booleans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Utility class providing equivalent functionality for XContentParser.
 * Contains snippets of code from AbstractObjectParsers.
 */
public class ParserUtils {

    private ParserUtils() {}

    public static boolean booleanValue(JsonParser p) throws IOException {
        JsonToken token = p.currentToken();
        if (token == JsonToken.VALUE_STRING) {
            return Booleans.parseBoolean(p.getTextCharacters(), p.getTextOffset(), p.getTextLength(), false /* irrelevant */);
        }
        return p.getBooleanValue();
    }

    public static Integer intValue(JsonParser p) throws IOException {
        JsonToken token = p.currentToken();
        if (token == JsonToken.VALUE_STRING) {
            String text = text(p);
            double doubleValue = Double.parseDouble(text);

            if (doubleValue < Integer.MIN_VALUE || doubleValue > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Value [" + text + "] is out of range for an integer");
            }

            return (int) doubleValue;
        }
        return p.getIntValue();
    }

    public static String text(JsonParser p) throws IOException {
        JsonToken current = p.currentToken();
        if (current.isScalarValue()) {
            return p.getText();
        }
        throw new IllegalStateException("Can't get text on a " + current + " at " + location(p));
    }

    public static Map<String, Object> map(JsonParser p) throws IOException {
        return readMapSafe(p, HashMap::new);
    }

    public static Map<String, Object> mapOrdered(JsonParser p) throws IOException {
        return readMapSafe(p, LinkedHashMap::new);
    }

    public static Map<String, Object> readMapSafe(JsonParser p, Supplier<Map<String, Object>> mapFactory) throws IOException {
        final Map<String, Object> map = mapFactory.get();
        return findNonEmptyMapStart(p) ? readMapEntries(p, mapFactory, map) : map;
    }

    private static boolean findNonEmptyMapStart(JsonParser parser) throws IOException {
        JsonToken token = parser.currentToken();
        if (token == null) {
            token = parser.nextToken();
        }
        if (token == JsonToken.START_OBJECT) {
            token = parser.nextToken();
        }
        return token == JsonToken.FIELD_NAME;
    }

    // Read a map without bounds checks from a parser that is assumed to be at the map's first field's name token
    private static Map<String, Object> readMapEntries(JsonParser parser, Supplier<Map<String, Object>> mapFactory, Map<String, Object> map)
        throws IOException {
        assert parser.currentToken() == JsonToken.FIELD_NAME : "Expected field name but saw [" + parser.currentToken() + "]";
        do {
            // Must point to field name
            String fieldName = parser.currentName();
            // And then the value...
            Object value = readValueUnsafe(parser.nextToken(), parser, mapFactory);
            map.put(fieldName, value);
        } while (parser.nextToken() == JsonToken.FIELD_NAME);
        return map;
    }

    public static List<Object> list(JsonParser parser) throws IOException {
        skipToListStart(parser);
        return readListUnsafe(parser, HashMap::new);
    }

    public static List<Object> listOrderedMap(JsonParser parser) throws IOException {
        skipToListStart(parser);
        return readListUnsafe(parser, LinkedHashMap::new);
    }

    // Skips the current parser to the next array start. Assumes that the parser is either positioned before an array field's name token or
    // on the start array token.
    private static void skipToListStart(JsonParser parser) throws IOException {
        JsonToken token = parser.currentToken();
        if (token == null) {
            token = parser.nextToken();
        }
        if (token == JsonToken.FIELD_NAME) {
            token = parser.nextToken();
        }
        if (token != JsonToken.START_ARRAY) {
            throw new ParseException(location(parser), "Failed to parse list:  expecting " + JsonToken.START_ARRAY + " but got " + token);
        }
    }

    private static Object readValueUnsafe(JsonToken currentToken, JsonParser parser, Supplier<Map<String, Object>> mapFactory)
        throws IOException {
        if (currentToken != parser.currentToken()) {
            throw new ParseException(
                "Supplied current token ["
                    + currentToken
                    + "] is different from actual parser current token ["
                    + parser.currentToken()
                    + "]"
            );
        }
        switch (currentToken) {
            case VALUE_STRING:
                return text(parser);
            case VALUE_NUMBER_FLOAT:
            case VALUE_NUMBER_INT:
                return parser.getNumberValue();
            case VALUE_FALSE:
            case VALUE_TRUE:
                return parser.getBooleanValue();
            case START_OBJECT: {
                final Map<String, Object> map = mapFactory.get();
                return parser.nextToken() != JsonToken.FIELD_NAME ? map : readMapEntries(parser, mapFactory, map);
            }
            case START_ARRAY:
                return readListUnsafe(parser, mapFactory);
            case VALUE_EMBEDDED_OBJECT:
                return parser.getBinaryValue();
            case VALUE_NULL:
            default:
                return null;
        }
    }

    private static List<Object> readListUnsafe(JsonParser parser, Supplier<Map<String, Object>> mapFactory) throws IOException {
        if (parser.currentToken() != JsonToken.START_ARRAY) {
            throw new ParseException(location(parser), "Expected START_ARRAY but got [" + parser.currentToken() + "]");
        }
        ArrayList<Object> list = new ArrayList<>();
        for (JsonToken token = parser.nextToken(); token != null && token != JsonToken.END_ARRAY; token = parser.nextToken()) {
            list.add(readValueUnsafe(token, parser, mapFactory));
        }
        return list;
    }

    public static ContentLocation location(JsonParser p) {
        return location(p.getTokenLocation());
    }

    public static ContentLocation location(JsonLocation tokenLocation) {
        return tokenLocation != null
            ? new ContentLocation(tokenLocation.getLineNr(), tokenLocation.getColumnNr())
            : ContentLocation.UNKNOWN;
    }
}
