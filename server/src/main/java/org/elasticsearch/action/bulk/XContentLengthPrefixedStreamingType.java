/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.xcontent.MediaType;
import org.elasticsearch.xcontent.MediaTypeRegistry;
import org.elasticsearch.xcontent.ParsedMediaType;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.cbor.CborXContent;
import org.elasticsearch.xcontent.smile.SmileXContent;
import org.elasticsearch.xcontent.yaml.YamlXContent;

import java.util.Map;
import java.util.Set;

/**
 * The streaming format using length prefixed format for bulk requests.
 */
public enum XContentLengthPrefixedStreamingType implements MediaType {
    /**
     * Json length prefixed type.
     */
    JSON(SmileXContent.smileXContent) {

        @Override
        public String queryParameter() {
            return null;
        }

        @Override
        public Set<HeaderValue> headerValues() {
            return Set.of(
                new HeaderValue(
                    XContentType.VENDOR_APPLICATION_PREFIX + "stream.json",
                    Map.of(COMPATIBLE_WITH_PARAMETER_NAME, VERSION_PATTERN)
                )
            );
        }
    },
    /**
     * SMILE length prefixed type.
     */
    SMILE(SmileXContent.smileXContent) {

        @Override
        public String queryParameter() {
            return null;
        }

        @Override
        public Set<HeaderValue> headerValues() {
            return Set.of(
                new HeaderValue(
                    XContentType.VENDOR_APPLICATION_PREFIX + "stream.smile",
                    Map.of(COMPATIBLE_WITH_PARAMETER_NAME, VERSION_PATTERN)
                )
            );
        }
    },
    /**
     * YAML length prefixed type.
     */
    YAML(YamlXContent.yamlXContent) {

        @Override
        public String queryParameter() {
            return null;
        }

        @Override
        public Set<HeaderValue> headerValues() {
            return Set.of(
                new HeaderValue(
                    XContentType.VENDOR_APPLICATION_PREFIX + "stream.yaml",
                    Map.of(COMPATIBLE_WITH_PARAMETER_NAME, VERSION_PATTERN)
                )
            );
        }
    },
    /**
     * CBOR length prefixed type.
     */
    CBOR(CborXContent.cborXContent) {

        @Override
        public String queryParameter() {
            return null;
        }

        @Override
        public Set<HeaderValue> headerValues() {
            return Set.of(
                new HeaderValue(
                    XContentType.VENDOR_APPLICATION_PREFIX + "stream.cbor",
                    Map.of(COMPATIBLE_WITH_PARAMETER_NAME, VERSION_PATTERN)
                )
            );
        }
    };

    private static final MediaTypeRegistry<XContentLengthPrefixedStreamingType> MEDIA_TYPE_REGISTRY = new MediaTypeRegistry<
        XContentLengthPrefixedStreamingType>().register(XContentLengthPrefixedStreamingType.values());

    /**
     * Parses the given media type header value and returns the corresponding {@link XContentLengthPrefixedStreamingType},
     * or null if there is no match.
     */
    public static XContentLengthPrefixedStreamingType fromMediaType(String mediaTypeHeaderValue) throws IllegalArgumentException {
        ParsedMediaType parsedMediaType = ParsedMediaType.parseMediaType(mediaTypeHeaderValue);
        if (parsedMediaType != null) {
            return parsedMediaType.toMediaType(MEDIA_TYPE_REGISTRY);
        }
        return null;
    }

    private final XContentType xContentType;

    XContentLengthPrefixedStreamingType(XContent xContent) {
        this.xContentType = xContent.type();
    }

    public final XContentType xContentType() {
        return xContentType;
    }
}
