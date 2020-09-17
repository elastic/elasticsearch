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

import java.util.Locale;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
/*
/**
     * A regexp to allow parsing media types. It covers two use cases.
     * 1. Media type with a version - requires a custom vnd.elasticsearch subtype and a compatible-with parameter
     * i.e. application/vnd.elasticsearch+json;compatible-with
     * 2. Media type without a version - for users not using compatible API i.e. application/json
     */
//private static final Pattern COMPATIBLE_API_HEADER_PATTERN = Pattern.compile(
//    //type
//    "^(application|text)/" +
//        // custom subtype and a version: vnd.elasticsearch+json;compatible-with=7
//        "((vnd\\.elasticsearch\\+([^;\\s]+)(\\s*;\\s*compatible-with=(\\d+)))" +
//        "|([^;\\s]+))" + //subtype: json,yaml,etc some of these are defined in x-pack so can't be enumerated
//        "(?:\\s*;\\s*(charset=UTF-8)?)?$",
//    Pattern.CASE_INSENSITIVE);
// */
    public static final MediaTypeParser<XContentType> mediaTypeParser = new MediaTypeParser<>(XContentType.values(),
        Map.of("application/*", JSON, "application/x-ndjson", JSON,
            "application/vnd.elasticsearch+json", JSON,
            "application/vnd.elasticsearch+smile", SMILE,
            "application/vnd.elasticsearch+yaml", YAML,
            "application/vnd.elasticsearch+cbor", CBOR));


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
