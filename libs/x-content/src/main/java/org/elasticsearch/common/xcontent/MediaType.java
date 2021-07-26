/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.xcontent;

import org.elasticsearch.core.Tuple;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Abstracts a <a href="http://en.wikipedia.org/wiki/Internet_media_type">Media Type</a> and a query parameter <code>format</code>.
 * Media types are used as values on Content-Type and Accept headers
 * format is an URL parameter, specifies response media type.
 */
public interface MediaType {
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

    /**
     * A class to represent supported mediaType values i.e. application/json and parameters to be validated.
     * Parameters for validation is a map where a key is a parameter name, value is a parameter regex which is used for validation.
     * Regex will be applied with case insensitivity.
     */
    class HeaderValue extends Tuple<String, Map<String, String>> {
        public HeaderValue(String headerValue, Map<String, String> parametersForValidation) {
            super(headerValue, parametersForValidation);
        }

        public HeaderValue(String headerValue) {
            this(headerValue, Collections.emptyMap());
        }
    }
}
