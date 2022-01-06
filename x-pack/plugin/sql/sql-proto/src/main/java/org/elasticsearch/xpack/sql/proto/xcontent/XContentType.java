/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.proto.xcontent;

import org.elasticsearch.xpack.sql.proto.xcontent.cbor.CborXContent;
import org.elasticsearch.xpack.sql.proto.xcontent.json.JsonXContent;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * NB: Light-clone from XContent library to keep JDBC driver independent.
 *
 * The content type of {@link XContent}.
 */
public enum XContentType implements MediaType {

    /**
     * A JSON based content type.
     */
    JSON(0) {
        @Override
        public String mediaTypeWithoutParameters() {
            return "application/json";
        }

        @Override
        public String mediaType() {
            return "application/json;charset=utf-8";
        }

        @Override
        public String queryParameter() {
            return "json";
        }

        @Override
        public XContent xContent() {
            return JsonXContent.jsonXContent;
        }

        @Override
        public Set<HeaderValue> headerValues() {
            return Collections.unmodifiableSet(
                new HashSet<>(
                    Arrays.asList(
                        new HeaderValue("application/json"),
                        new HeaderValue("application/x-ndjson"),
                        new HeaderValue("application/*")
                    )
                )
            );
        }
    },
    /**
     * A CBOR based content type.
     */
    CBOR(3) {
        @Override
        public String mediaTypeWithoutParameters() {
            return "application/cbor";
        }

        @Override
        public String queryParameter() {
            return "cbor";
        }

        @Override
        public XContent xContent() {
            return CborXContent.cborXContent;
        }

        @Override
        public Set<HeaderValue> headerValues() {
            return Collections.singleton(new HeaderValue("application/cbor"));
        }
    };

    public static final MediaTypeRegistry<XContentType> MEDIA_TYPE_REGISTRY = new MediaTypeRegistry<XContentType>().register(
        XContentType.values()
    );

    /**
     * Attempts to match the given media type with the known {@link XContentType} values. This match is done in a case-insensitive manner.
     * The provided media type can optionally has parameters.
     * This method is suitable for parsing of the {@code Content-Type} and {@code Accept} HTTP headers.
     * This method will return {@code null} if no match is found
     */
    public static XContentType fromMediaType(String mediaTypeHeaderValue) throws IllegalArgumentException {
        ParsedMediaType parsedMediaType = ParsedMediaType.parseMediaType(mediaTypeHeaderValue);
        if (parsedMediaType != null) {
            return parsedMediaType.toMediaType(MEDIA_TYPE_REGISTRY);
        }
        return null;
    }

    private int index;

    XContentType(int index) {
        this.index = index;
    }

    public int index() {
        return index;
    }

    public String mediaType() {
        return mediaTypeWithoutParameters();
    }

    public abstract XContent xContent();

    public abstract String mediaTypeWithoutParameters();

    /**
     * Returns a canonical XContentType for this XContentType.
     * A canonical XContentType is used to serialize or deserialize the data from/to for HTTP.
     * More specialized XContentType types such as vnd* variants still use the general data structure,
     * but may have semantic differences.
     * Example: XContentType.VND_JSON has a canonical XContentType.JSON
     * XContentType.JSON has a canonical XContentType.JSON
     */
    public XContentType canonical() {
        return this;
    }
}
