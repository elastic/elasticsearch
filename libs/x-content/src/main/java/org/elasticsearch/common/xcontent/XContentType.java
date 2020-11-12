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

package org.elasticsearch.common.xcontent;

import org.elasticsearch.common.xcontent.cbor.CborXContent;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.smile.SmileXContent;
import org.elasticsearch.common.xcontent.yaml.YamlXContent;

import java.util.Map;
import java.util.Set;

/**
 * The content type of {@link org.elasticsearch.common.xcontent.XContent}.
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
            return "application/json; charset=UTF-8";
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
            return Set.of(
                new HeaderValue("application/json"),
                new HeaderValue("application/x-ndjson"),
                new HeaderValue("application/*"),
                new HeaderValue(VENDOR_APPLICATION_PREFIX + "json",
                    Map.of(COMPATIBLE_WITH_PARAMETER_NAME, VERSION_PATTERN)),
                new HeaderValue(VENDOR_APPLICATION_PREFIX + "x-ndjson",
                    Map.of(COMPATIBLE_WITH_PARAMETER_NAME, VERSION_PATTERN)));
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
            return Set.of(
                new HeaderValue("application/smile"),
                new HeaderValue(VENDOR_APPLICATION_PREFIX + "smile",
                    Map.of(COMPATIBLE_WITH_PARAMETER_NAME, VERSION_PATTERN)));
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
            return Set.of(
                new HeaderValue("application/yaml"),
                new HeaderValue(VENDOR_APPLICATION_PREFIX + "yaml",
                    Map.of(COMPATIBLE_WITH_PARAMETER_NAME, VERSION_PATTERN)));
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
            return Set.of(
                new HeaderValue("application/cbor"),
                new HeaderValue(VENDOR_APPLICATION_PREFIX + "cbor",
                    Map.of(COMPATIBLE_WITH_PARAMETER_NAME, VERSION_PATTERN)));
        }
    };

    public static final MediaTypeRegistry<XContentType> MEDIA_TYPE_REGISTRY = new MediaTypeRegistry<XContentType>()
        .register(XContentType.values());
    public static final String VENDOR_APPLICATION_PREFIX = "application/vnd.elasticsearch+";

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
            return parsedMediaType
                .toMediaType(MEDIA_TYPE_REGISTRY);
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
            String version = parsedMediaType
                .getParameters()
                .get(COMPATIBLE_WITH_PARAMETER_NAME);
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
}
