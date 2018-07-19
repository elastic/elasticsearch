/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Locale;
import java.util.Map;

public final class Processors {

    public static long bytes(String value) {
        return ByteSizeValue.parseBytesSizeValue(value, null, "Ingest Field").getBytes();
    }

    public static String lowercase(String value) {
        return value.toLowerCase(Locale.ROOT);
    }

    public static String uppercase(String value) {
        return value.toUpperCase(Locale.ROOT);
    }

    public static String trim(String value) {
        return value.trim();
    }

    public static Object json(Object fieldValue) {
        BytesReference bytesRef = fieldValue == null ? new BytesArray("null") : new BytesArray(fieldValue.toString());
        try (InputStream stream = bytesRef.streamInput();
             XContentParser parser = JsonXContent.jsonXContent
                 .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, stream)) {
            XContentParser.Token token = parser.nextToken();
            Object value = null;
            if (token == XContentParser.Token.VALUE_NULL) {
                value = null;
            } else if (token == XContentParser.Token.VALUE_STRING) {
                value = parser.text();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                value = parser.numberValue();
            } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                value = parser.booleanValue();
            } else if (token == XContentParser.Token.START_OBJECT) {
                value = parser.map();
            } else if (token == XContentParser.Token.START_ARRAY) {
                value = parser.list();
            } else if (token == XContentParser.Token.VALUE_EMBEDDED_OBJECT) {
                throw new IllegalArgumentException("cannot read binary value");
            }
            return value;
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public static void json(Map<String, Object> ctx, String field) {
        Object value = json(ctx.get(field));
        if (value instanceof Map) {
            ctx.putAll((Map<String, Object>) value);
        } else {
            throw new IllegalArgumentException("cannot add non-map fields to root of document");
        }
    }

    public static String urlDecode(String value) {
        try {
            return URLDecoder.decode(value, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException("Could not URL-decode value.", e);
        }
    }
}
