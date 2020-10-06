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

import java.util.Collections;
import java.util.Map;

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
        public String subtype() {
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
        public String subtype() {
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
        public String subtype() {
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
        public String subtype() {
            return "cbor";
        }

        @Override
        public XContent xContent() {
            return CborXContent.cborXContent;
        }
    };

    private static final String COMPATIBLE_WITH_PARAMETER_NAME = "compatible-with";
    private static final String VERSION_PATTERN = "\\d+";
    public static final MediaTypeParser<XContentType> mediaTypeParser = new MediaTypeParser.Builder<XContentType>()
        .withMediaTypeAndParams("application/smile", SMILE, Collections.emptyMap())
        .withMediaTypeAndParams("application/cbor", CBOR, Collections.emptyMap())
        .withMediaTypeAndParams("application/json", JSON, Map.of("charset", "UTF-8"))
        .withMediaTypeAndParams("application/yaml", YAML, Map.of("charset", "UTF-8"))
        .withMediaTypeAndParams("application/*", JSON, Map.of("charset", "UTF-8"))
        .withMediaTypeAndParams("application/x-ndjson", JSON, Map.of("charset", "UTF-8"))
        .withMediaTypeAndParams("application/vnd.elasticsearch+json", JSON,
            Map.of(COMPATIBLE_WITH_PARAMETER_NAME, VERSION_PATTERN, "charset", "UTF-8"))
        .withMediaTypeAndParams("application/vnd.elasticsearch+smile", SMILE,
            Map.of(COMPATIBLE_WITH_PARAMETER_NAME, VERSION_PATTERN, "charset", "UTF-8"))
        .withMediaTypeAndParams("application/vnd.elasticsearch+yaml", YAML,
            Map.of(COMPATIBLE_WITH_PARAMETER_NAME, VERSION_PATTERN, "charset", "UTF-8"))
        .withMediaTypeAndParams("application/vnd.elasticsearch+cbor", CBOR,
            Map.of(COMPATIBLE_WITH_PARAMETER_NAME, VERSION_PATTERN, "charset", "UTF-8"))
        .withMediaTypeAndParams("application/vnd.elasticsearch+x-ndjson", JSON,
            Map.of(COMPATIBLE_WITH_PARAMETER_NAME, VERSION_PATTERN, "charset", "UTF-8"))
        .build();

    /**
     * Accepts a format string, which is most of the time is equivalent to {@link XContentType#subtype()}
     * and attempts to match the value to an {@link XContentType}.
     * The comparisons are done in lower case format.
     * This method will return {@code null} if no match is found
     */
    public static XContentType fromFormat(String mediaType) {
        return mediaTypeParser.fromFormat(mediaType);
    }

    /**
     * Attempts to match the given media type with the known {@link XContentType} values. This match is done in a case-insensitive manner.
     * The provided media type can optionally has parameters.
     * This method is suitable for parsing of the {@code Content-Type} and {@code Accept} HTTP headers.
     * This method will return {@code null} if no match is found
     */
    public static XContentType fromMediaType(String mediaTypeHeaderValue) {
        return mediaTypeParser.fromMediaType(mediaTypeHeaderValue);
    }

    private int index;

    XContentType(int index) {
        this.index = index;
    }

    public static Byte parseVersion(String mediaType) {
        MediaTypeParser<XContentType>.ParsedMediaType parsedMediaType = mediaTypeParser.parseMediaType(mediaType);
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


    @Override
    public String type() {
        return "application";
    }

    @Override
    public String format() {
        return subtype();
    }
}
