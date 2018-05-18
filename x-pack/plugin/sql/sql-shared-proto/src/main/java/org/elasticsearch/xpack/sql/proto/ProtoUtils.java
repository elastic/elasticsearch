/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.proto;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

public final class ProtoUtils {

    private ProtoUtils() {

    }

    /**
     * Parses a generic value from the XContent stream
     */
    public static Object parseFieldsValue(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        Object value = null;
        if (token == XContentParser.Token.VALUE_STRING) {
            //binary values will be parsed back and returned as base64 strings when reading from json and yaml
            value = parser.text();
        } else if (token == XContentParser.Token.VALUE_NUMBER) {
            value = parser.numberValue();
        } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
            value = parser.booleanValue();
        } else if (token == XContentParser.Token.VALUE_NULL) {
            value = null;
        } else if (token == XContentParser.Token.START_OBJECT) {
            value = parser.mapOrdered();
        } else if (token == XContentParser.Token.START_ARRAY) {
            value = parser.listOrderedMap();
        } else {
            String message = "Failed to parse object: unexpected token [%s] found";
            throw new IllegalStateException(String.format(Locale.ROOT, message, token));
        }
        return value;
    }

    /**
     * Returns a string representation of the builder (only applicable for text based xcontent).
     *
     * @param xContentBuilder builder containing an object to converted to a string
     */
    public static String toString(XContentBuilder xContentBuilder) {
        byte[] byteArray = ((ByteArrayOutputStream) xContentBuilder.getOutputStream()).toByteArray();
        return new String(byteArray, StandardCharsets.UTF_8);
    }

    public static String toString(ToXContent toXContent) {
        try {
            XContentBuilder builder = JsonXContent.contentBuilder();
            if (toXContent.isFragment()) {
                builder.startObject();
            }
            toXContent.toXContent(builder, ToXContent.EMPTY_PARAMS);
            if (toXContent.isFragment()) {
                builder.endObject();
            }
            builder.close();
            return toString(builder);
        } catch (IOException e) {
            try {
                XContentBuilder builder = JsonXContent.contentBuilder();
                builder.startObject();
                builder.field("error", "error building toString out of XContent: " + e.getMessage());
                builder.endObject();
                builder.close();
                return toString(builder);
            } catch (IOException e2) {
                throw new IllegalArgumentException("cannot generate error message for deserialization", e);
            }
        }
    }

}
