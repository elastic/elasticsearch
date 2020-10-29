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

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.cbor.CborXContent;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.smile.SmileXContent;
import org.elasticsearch.common.xcontent.yaml.YamlXContent;

import java.util.Collections;
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
        public String formatPathParameter() {
            return "json";
        }

        @Override
        public XContent xContent() {
            return JsonXContent.jsonXContent;
        }

        @Override
        public Set<Tuple<String, Map<String, String>>> mediaTypeMappings() {
            return Set.of(
                Tuple.tuple("application/json", Map.of("charset", "UTF-8")),
                Tuple.tuple("application/x-ndjson", Map.of("charset", "UTF-8")),
                Tuple.tuple("application/*", Collections.emptyMap()),
                Tuple.tuple("application/vnd.elasticsearch+json",
                    Map.of(COMPATIBLE_WITH_PARAMETER_NAME, VERSION_PATTERN, "charset", "UTF-8")),
                Tuple.tuple("application/vnd.elasticsearch+x-ndjson",
                    Map.of(COMPATIBLE_WITH_PARAMETER_NAME, VERSION_PATTERN, "charset", "UTF-8")));
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
        public String formatPathParameter() {
            return "smile";
        }

        @Override
        public XContent xContent() {
            return SmileXContent.smileXContent;
        }

        @Override
        public Set<Tuple<String, Map<String, String>>> mediaTypeMappings() {
            return Set.of(
                Tuple.tuple("application/smile", Collections.emptyMap()),
                Tuple.tuple("application/vnd.elasticsearch+smile",
                    Map.of(COMPATIBLE_WITH_PARAMETER_NAME, VERSION_PATTERN, "charset", "UTF-8")));
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
        public String formatPathParameter() {
            return "yaml";
        }

        @Override
        public XContent xContent() {
            return YamlXContent.yamlXContent;
        }

        @Override
        public Set<Tuple<String, Map<String, String>>> mediaTypeMappings() {
            return Set.of(
                Tuple.tuple("application/yaml", Collections.emptyMap()),
                Tuple.tuple("application/vnd.elasticsearch+yaml",
                    Map.of(COMPATIBLE_WITH_PARAMETER_NAME, VERSION_PATTERN, "charset", "UTF-8")));
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
        public String formatPathParameter() {
            return "cbor";
        }

        @Override
        public XContent xContent() {
            return CborXContent.cborXContent;
        }

        @Override
        public Set<Tuple<String, Map<String, String>>> mediaTypeMappings() {
            return Set.of(
                Tuple.tuple("application/cbor", Collections.emptyMap()),
                Tuple.tuple("application/vnd.elasticsearch+cbor",
                    Map.of(COMPATIBLE_WITH_PARAMETER_NAME, VERSION_PATTERN, "charset", "UTF-8")));
        }
    };

    public static final MediaTypeRegistry<XContentType> mediaTypeRegistry = new MediaTypeRegistry<XContentType>()
        .register(XContentType.values());

    /**
     * Accepts a format string, which is most of the time is equivalent to MediaType's subtype i.e. <code>application/<b>json</b></code>
     * and attempts to match the value to an {@link XContentType}.
     * The comparisons are done in lower case format.
     * This method will return {@code null} if no match is found
     */
    public static XContentType fromFormat(String format) {
        return mediaTypeRegistry.formatToMediaType(format);
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
                .toMediaType(mediaTypeRegistry);
        }
        return null;
        //throw new IllegalArgumentException("invalid media type [" + mediaTypeHeaderValue + "]");
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
