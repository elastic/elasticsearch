/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Abstracts a <a href="http://en.wikipedia.org/wiki/Internet_media_type">Media Type</a> and a query parameter <code>format</code>.
 * Media types are used as values on Content-Type and Accept headers
 * format is an URL parameter, specifies response media type.
 */
public interface MediaType {
    String type();

    /**
     * Returns a subtype part of a MediaType.
     * i.e. json for application/json
     */
    String subtype();

    /**
     * Returns a corresponding format for a MediaType. i.e. json for application/json media type
     * Can differ from the MediaType's subtype i.e plain/text has a subtype of text but format is txt
     */
    String format();

    /**
     * returns a string representation of a media type.
     */
    default String typeWithSubtype() {
        return type() + "/" + subtype();
    }


    @SuppressWarnings("checkstyle:RedundantModifier")
    boolean detectedXContent(final byte[] bytes, int offset, int length);

    @SuppressWarnings("checkstyle:RedundantModifier")
    boolean detectedXContent(final CharSequence content, final int length);

    default String mediaType() {
        return mediaTypeWithoutParameters();
    }

    String mediaTypeWithoutParameters();

    XContentBuilder contentBuilder() throws IOException;

    @SuppressWarnings("checkstyle:RedundantModifier")
    XContentBuilder contentBuilder(final OutputStream os) throws IOException;

    /**
     * Accepts a format string, which is most of the time is equivalent to {@link MediaType#subtype()}
     * and attempts to match the value to an {@link MediaType}.
     * The comparisons are done in lower case format.
     * This method will return {@code null} if no match is found
     */
    static MediaType fromFormat(String mediaType) {
        return MediaTypeRegistry.fromFormat(mediaType);
    }

    /**
     * Attempts to match the given media type with the known {@link MediaType} values. This match is done in a case-insensitive manner.
     * The provided media type can optionally has parameters.
     * This method is suitable for parsing of the {@code Content-Type} and {@code Accept} HTTP headers.
     * This method will return {@code null} if no match is found
     */
    static MediaType fromMediaType(String mediaTypeHeaderValue) {
        mediaTypeHeaderValue = removeVersionInMediaType(mediaTypeHeaderValue);
        return MediaTypeRegistry.fromMediaType(mediaTypeHeaderValue);
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
    static String removeVersionInMediaType(String mediaType) {
        if (mediaType != null && (mediaType = mediaType.toLowerCase(Locale.ROOT)).contains("vnd.opensearch")) {
            return mediaType.replaceAll("vnd.opensearch\\+", "").replaceAll("\\s*;\\s*compatible-with=\\d+", "");
        }
        return mediaType;
    }
    String COMPATIBLE_WITH_PARAMETER_NAME = "compatible-with";
    String VERSION_PATTERN = "\\d+";

    /**
     * Returns a corresponding format path parameter for a MediaType.
     * i.e. ?format=txt for plain/text media type
     */
    String queryParameter();

    /**
     * Returns a set of HeaderValues - allowed media type values on Accept or Content-Type headers
     * Also defines media type parameters for validation.
     */
    Set<HeaderValue> headerValues();

    XContent xContent();

    /**
     * A class to represent supported mediaType values i.e. application/json and parameters to be validated.
     * Parameters for validation is a map where a key is a parameter name, value is a parameter regex which is used for validation.
     * Regex will be applied with case insensitivity.
     */
    record HeaderValue(String v1, Map<String, String> v2) {

        public HeaderValue(String headerValue) {
            this(headerValue, Collections.emptyMap());
        }
    }
}
