/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
