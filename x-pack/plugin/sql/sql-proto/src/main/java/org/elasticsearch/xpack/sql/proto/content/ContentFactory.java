/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.proto.content;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static org.elasticsearch.xpack.sql.proto.content.ContentFactory.ContentType.CBOR;
import static org.elasticsearch.xpack.sql.proto.content.ContentFactory.ContentType.JSON;

public class ContentFactory {
    public enum ContentType {
        JSON {
            @Override
            public String mediaTypeWithoutParameters() {
                return "application/json";
            }
        },
        CBOR {
            @Override
            public String mediaTypeWithoutParameters() {
                return "application/cbor";
            }
        };

        public abstract String mediaTypeWithoutParameters();
    }

    public static ContentType parseMediaType(String headerValue) {
        ParsedMediaType parsedMediaType = ParsedMediaType.parseMediaType(headerValue);
        if (parsedMediaType == null) {
            return null;
        }
        String parsedType = parsedMediaType.mediaTypeWithoutParameters();
        if (JSON.mediaTypeWithoutParameters().equals(parsedType)) {
            return JSON;
        }
        if (CBOR.mediaTypeWithoutParameters().equals(parsedType)) {
            return CBOR;
        }
        return null;
    }

    public static JsonParser parser(ContentType type, InputStream in) throws IOException {
        if (type == JSON) {
            return JsonFactory.parser(in);
        } else {
            return CborFactory.parser(in);
        }
    }

    public static JsonGenerator generator(ContentType type, OutputStream out) throws IOException {
        if (type == JSON) {
            return JsonFactory.generator(out);
        } else {
            return CborFactory.generator(out);
        }
    }
}
