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

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * A registry for quick media type lookup.
 * It allows to find media type by a header value - typeWithSubtype aka mediaType without parameters.
 * I.e. application/json will return XContentType.JSON
 * Also allows to find media type by a path parameter <code>format</code>.
 * I.e. txt used in path _sql?format=txt will return TextFormat.PLAIN_TEXT
 *
 * Multiple header representations may map to a single {@link MediaType} for example, "application/json"
 * and "application/x-ndjson" both represent a JSON MediaType.
 * A MediaType can have only one query parameter representation.
 * For example "json" (case insensitive) maps back to a JSON media type.
 *
 * Additionally, a http header may optionally have parameters. For example "application/vnd.elasticsearch+json; compatible-with=7".
 * This class also allows to define a regular expression for valid values of charset.
 */
public class MediaTypeRegistry<T extends MediaType> {

    private Map<String, T> queryParamToMediaType = new HashMap<>();
    private Map<String, T> typeWithSubtypeToMediaType = new HashMap<>();
    private Map<String, Map<String, Pattern>> parametersMap = new HashMap<>();

    public T queryParamToMediaType(String format) {
        if (format == null) {
            return null;
        }
        return queryParamToMediaType.get(format.toLowerCase(Locale.ROOT));
    }

    public T typeWithSubtypeToMediaType(String typeWithSubtype) {
        return typeWithSubtypeToMediaType.get(typeWithSubtype.toLowerCase(Locale.ROOT));
    }

    public Map<String, Pattern> parametersFor(String typeWithSubtype) {
        return parametersMap.get(typeWithSubtype);
    }

    public MediaTypeRegistry<T> register(T[] mediaTypes ) {
        for (T mediaType : mediaTypes) {
            Set<MediaType.HeaderValue> tuples = mediaType.headerValues();
            for (MediaType.HeaderValue headerValue : tuples) {
                queryParamToMediaType.put(mediaType.queryParameter(), mediaType);
                typeWithSubtypeToMediaType.put(headerValue.v1(), mediaType);
                parametersMap.put(headerValue.v1(), convertPatterns(headerValue.v2()));
            }
        }
        return this;
    }

    private Map<String,Pattern> convertPatterns(Map<String, String> paramNameAndValueRegex) {
        Map<String, Pattern> parametersForMediaType = new HashMap<>(paramNameAndValueRegex.size());
        for (Map.Entry<String, String> params : paramNameAndValueRegex.entrySet()) {
            String parameterName = params.getKey().toLowerCase(Locale.ROOT);
            String parameterRegex = params.getValue();
            Pattern pattern = Pattern.compile(parameterRegex, Pattern.CASE_INSENSITIVE);
            parametersForMediaType.put(parameterName, pattern);
        }
        return parametersForMediaType;
    }
}
