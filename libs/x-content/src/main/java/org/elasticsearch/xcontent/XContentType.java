/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent;

import org.elasticsearch.xcontent.cbor.CborXContent;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xcontent.smile.SmileXContent;
import org.elasticsearch.xcontent.yaml.YamlXContent;

import java.util.Locale;
import java.util.Objects;

/**
 * The content type of {@link org.elasticsearch.xcontent.XContent}.
 */
public enum XContentType {

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
            return "application/json; charset=UTF-8";
        }

        @Override
        public String shortName() {
            return "json";
        }

        @Override
        public XContent xContent() {
            return JsonXContent.jsonXContent;
        }
    },
    /**
     * The jackson based smile binary format. Fast and compact binary format.
     */
    SMILE(1) {
        @Override
        public String mediaTypeWithoutParameters() {
            return "application/smile";
        }

        @Override
        public String shortName() {
            return "smile";
        }

        @Override
        public XContent xContent() {
            return SmileXContent.smileXContent;
        }
    },
    /**
     * A YAML based content type.
     */
    YAML(2) {
        @Override
        public String mediaTypeWithoutParameters() {
            return "application/yaml";
        }

        @Override
        public String shortName() {
            return "yaml";
        }

        @Override
        public XContent xContent() {
            return YamlXContent.yamlXContent;
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
        public String shortName() {
            return "cbor";
        }

        @Override
        public XContent xContent() {
            return CborXContent.cborXContent;
        }
    };

    /**
     * Accepts either a format string, which is equivalent to {@link XContentType#shortName()} or a media type that optionally has
     * parameters and attempts to match the value to an {@link XContentType}. The comparisons are done in lower case format and this method
     * also supports a wildcard accept for {@code application/*}. This method can be used to parse the {@code Accept} HTTP header or a
     * format query string parameter. This method will return {@code null} if no match is found
     */
    public static XContentType fromMediaTypeOrFormat(String mediaType) {
        if (mediaType == null) {
            return null;
        }

        mediaType = removeVersionInMediaType(mediaType);
        for (XContentType type : values()) {
            if (isSameMediaTypeOrFormatAs(mediaType, type)) {
                return type;
            }
        }
        final String lowercaseMediaType = mediaType.toLowerCase(Locale.ROOT);
        if (lowercaseMediaType.startsWith("application/*")) {
            return JSON;
        }

        return null;
    }

    /**
     * Clients compatible with ES 7.x might start sending media types with versioned media type
     * in a form of application/vnd.elasticsearch+json;compatible-with=7.
     * This has to be removed in order to be used in 7.x server.
     * The same client connecting using that media type will be able to communicate with ES 8 thanks to compatible API.
     * @param mediaType - a media type used on Content-Type header, might contain versioned media type.
     *
     * @return a media type string without
     */
    private static String removeVersionInMediaType(String mediaType) {
        if (mediaType.contains("vnd.elasticsearch")) {
            return mediaType.replaceAll("vnd.elasticsearch\\+", "")
                .replaceAll("\\s*;\\s*compatible-with=\\d+", "")
                .replaceAll("\\s*;\\s*", ";"); // to allow matching using startsWith
        }
        return mediaType;
    }

    /**
     * Attempts to match the given media type with the known {@link XContentType} values. This match is done in a case-insensitive manner.
     * The provided media type should not include any parameters. This method is suitable for parsing part of the {@code Content-Type}
     * HTTP header. This method will return {@code null} if no match is found
     */
    public static XContentType fromMediaType(String mediaType) {
        String lowercaseMediaType = Objects.requireNonNull(mediaType, "mediaType cannot be null").toLowerCase(Locale.ROOT);
        lowercaseMediaType = removeVersionInMediaType(lowercaseMediaType);

        for (XContentType type : values()) {
            if (type.mediaTypeWithoutParameters().equals(lowercaseMediaType)) {
                return type;
            }
        }
        // we also support newline delimited JSON: http://specs.okfnlabs.org/ndjson/
        if (lowercaseMediaType.toLowerCase(Locale.ROOT).equals("application/x-ndjson")) {
            return XContentType.JSON;
        }

        return null;
    }

    private static boolean isSameMediaTypeOrFormatAs(String stringType, XContentType type) {
        return type.mediaTypeWithoutParameters().equalsIgnoreCase(stringType)
            || stringType.toLowerCase(Locale.ROOT).startsWith(type.mediaTypeWithoutParameters().toLowerCase(Locale.ROOT) + ";")
            || type.shortName().equalsIgnoreCase(stringType);
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

    public abstract String shortName();

    public abstract XContent xContent();

    public abstract String mediaTypeWithoutParameters();

}
