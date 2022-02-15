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

import java.util.Map;
import java.util.Set;

/**
 * The content type of {@link org.elasticsearch.xcontent.XContent}.
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
            return Set.of(new HeaderValue("application/json"), new HeaderValue("application/x-ndjson"), new HeaderValue("application/*"));
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
        public String queryParameter() {
            return "smile";
        }

        @Override
        public XContent xContent() {
            return SmileXContent.smileXContent;
        }

        @Override
        public Set<HeaderValue> headerValues() {
            return Set.of(new HeaderValue("application/smile"));
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
        public String queryParameter() {
            return "yaml";
        }

        @Override
        public XContent xContent() {
            return YamlXContent.yamlXContent;
        }

        @Override
        public Set<HeaderValue> headerValues() {
            return Set.of(new HeaderValue("application/yaml"));
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
            return Set.of(new HeaderValue("application/cbor"));
        }
    },
    /**
     * A versioned JSON based content type.
     */
    VND_JSON(4) {
        @Override
        public String mediaTypeWithoutParameters() {
            return VENDOR_APPLICATION_PREFIX + "json";
        }

        @Override
        public String queryParameter() {
            return "vnd_json";
        }

        @Override
        public XContent xContent() {
            return JsonXContent.jsonXContent;
        }

        @Override
        public Set<HeaderValue> headerValues() {
            return Set.of(
                new HeaderValue(VENDOR_APPLICATION_PREFIX + "json", Map.of(COMPATIBLE_WITH_PARAMETER_NAME, VERSION_PATTERN)),
                new HeaderValue(VENDOR_APPLICATION_PREFIX + "x-ndjson", Map.of(COMPATIBLE_WITH_PARAMETER_NAME, VERSION_PATTERN))
            );
        }

        @Override
        public XContentType canonical() {
            return JSON;
        }
    },
    /**
     * Versioned jackson based smile binary format. Fast and compact binary format.
     */
    VND_SMILE(5) {
        @Override
        public String mediaTypeWithoutParameters() {
            return VENDOR_APPLICATION_PREFIX + "smile";
        }

        @Override
        public String queryParameter() {
            return "vnd_smile";
        }

        @Override
        public XContent xContent() {
            return SmileXContent.smileXContent;
        }

        @Override
        public Set<HeaderValue> headerValues() {
            return Set.of(new HeaderValue(VENDOR_APPLICATION_PREFIX + "smile", Map.of(COMPATIBLE_WITH_PARAMETER_NAME, VERSION_PATTERN)));
        }

        @Override
        public XContentType canonical() {
            return SMILE;
        }
    },
    /**
     * A Versioned YAML based content type.
     */
    VND_YAML(6) {
        @Override
        public String mediaTypeWithoutParameters() {
            return VENDOR_APPLICATION_PREFIX + "yaml";
        }

        @Override
        public String queryParameter() {
            return "vnd_yaml";
        }

        @Override
        public XContent xContent() {
            return YamlXContent.yamlXContent;
        }

        @Override
        public Set<HeaderValue> headerValues() {
            return Set.of(new HeaderValue(VENDOR_APPLICATION_PREFIX + "yaml", Map.of(COMPATIBLE_WITH_PARAMETER_NAME, VERSION_PATTERN)));
        }

        @Override
        public XContentType canonical() {
            return YAML;
        }
    },
    /**
     * A Versioned CBOR based content type.
     */
    VND_CBOR(7) {
        @Override
        public String mediaTypeWithoutParameters() {
            return VENDOR_APPLICATION_PREFIX + "cbor";
        }

        @Override
        public String queryParameter() {
            return "vnd_cbor";
        }

        @Override
        public XContent xContent() {
            return CborXContent.cborXContent;
        }

        @Override
        public Set<HeaderValue> headerValues() {
            return Set.of(new HeaderValue(VENDOR_APPLICATION_PREFIX + "cbor", Map.of(COMPATIBLE_WITH_PARAMETER_NAME, VERSION_PATTERN)));
        }

        @Override
        public XContentType canonical() {
            return CBOR;
        }
    };

    public static final MediaTypeRegistry<XContentType> MEDIA_TYPE_REGISTRY = new MediaTypeRegistry<XContentType>().register(
        XContentType.values()
    );
    public static final String VENDOR_APPLICATION_PREFIX = "application/vnd.elasticsearch+";

    private final ParsedMediaType mediaType = ParsedMediaType.parseMediaType(mediaTypeWithoutParameters());

    /**
     * Accepts a format string, which is most of the time is equivalent to MediaType's subtype i.e. <code>application/<b>json</b></code>
     * and attempts to match the value to an {@link XContentType}.
     * The comparisons are done in lower case format.
     * This method will return {@code null} if no match is found
     */
    public static XContentType fromFormat(String format) {
        return MEDIA_TYPE_REGISTRY.queryParamToMediaType(format);
    }

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

    public static Byte parseVersion(String mediaType) {
        ParsedMediaType parsedMediaType = ParsedMediaType.parseMediaType(mediaType);
        if (parsedMediaType != null) {
            String version = parsedMediaType.getParameters().get(COMPATIBLE_WITH_PARAMETER_NAME);
            return version != null ? Byte.parseByte(version) : null;
        }
        return null;
    }

    public int index() {
        return index;
    }

    public String mediaType() {
        return mediaTypeWithoutParameters();
    }

    public abstract XContent xContent();

    public abstract String mediaTypeWithoutParameters();

    public ParsedMediaType toParsedMediaType() {
        return mediaType;
    }

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
