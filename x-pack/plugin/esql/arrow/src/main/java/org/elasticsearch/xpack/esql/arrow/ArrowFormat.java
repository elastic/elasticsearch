/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.arrow;

import org.elasticsearch.xcontent.MediaType;

import java.util.Map;
import java.util.Set;

public class ArrowFormat implements MediaType {
    public static final ArrowFormat INSTANCE = new ArrowFormat();

    private static final String FORMAT = "arrow";
    // See https://www.iana.org/assignments/media-types/application/vnd.apache.arrow.stream
    public static final String CONTENT_TYPE = "application/vnd.apache.arrow.stream";
    private static final String VENDOR_CONTENT_TYPE = "application/vnd.elasticsearch+arrow+stream";

    @Override
    public String queryParameter() {
        return FORMAT;
    }

    @Override
    public Set<HeaderValue> headerValues() {
        return Set.of(
            new HeaderValue(CONTENT_TYPE, Map.of("header", "present|absent")),
            new HeaderValue(VENDOR_CONTENT_TYPE, Map.of("header", "present|absent", COMPATIBLE_WITH_PARAMETER_NAME, VERSION_PATTERN))
        );
    }
}
