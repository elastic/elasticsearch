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

public class MediaTypeParser<T extends MediaType> {
    private final Map<String, T> formatToMediaType;
    private final Map<String, T> typeWithSubtypeToMediaType;

    public MediaTypeParser(T[] acceptedMediaTypes) {
        this(acceptedMediaTypes, Map.of());
    }

    public MediaTypeParser(T[] acceptedMediaTypes, Map<String, T> additionalMediaTypes) {
        final int size = acceptedMediaTypes.length + additionalMediaTypes.size();
        Map<String, T> formatMap = new HashMap<>(size);
        Map<String, T> typeMap = new HashMap<>(size);
        for (T mediaType : acceptedMediaTypes) {
            typeMap.put(mediaType.typeWithSubtype(), mediaType);
            formatMap.put(mediaType.format(), mediaType);
        }
        for (Map.Entry<String, T> entry : additionalMediaTypes.entrySet()) {
            String typeWithSubtype = entry.getKey();
            T mediaType = entry.getValue();

            typeMap.put(typeWithSubtype.toLowerCase(Locale.ROOT), mediaType);
            formatMap.put(mediaType.format(), mediaType);
        }

        this.formatToMediaType = Map.copyOf(formatMap);
        this.typeWithSubtypeToMediaType = Map.copyOf(typeMap);
    }

    public T fromMediaType(String mediaType) {
        ParsedMediaType parsedMediaType = parseMediaType(mediaType);
        return parsedMediaType != null ? parsedMediaType.getMediaType() : null;
    }

    public T fromFormat(String format) {
        if (format == null) {
            return null;
        }
        return formatToMediaType.get(format.toLowerCase(Locale.ROOT));
    }

    /**
     * parsing media type that follows https://tools.ietf.org/html/rfc7231#section-3.1.1.1
     * @param headerValue a header value from Accept or Content-Type
     * @return a parsed media-type
     */
    public ParsedMediaType parseMediaType(String headerValue) {
        if (headerValue != null) {
            String[] split = headerValue.toLowerCase(Locale.ROOT).split(";");

            String[] typeSubtype = split[0].trim().toLowerCase(Locale.ROOT)
                .split("/");
            if (typeSubtype.length == 2) {
                String type = typeSubtype[0];
                String subtype = typeSubtype[1];
                T xContentType = typeWithSubtypeToMediaType.get(type + "/" + subtype);
                if (xContentType != null) {
                    Map<String, String> parameters = new HashMap<>();
                    for (int i = 1; i < split.length; i++) {
                        //spaces are allowed between parameters, but not between '=' sign
                        String[] keyValueParam = split[i].trim().split("=");
                        if (keyValueParam.length != 2 || hasSpaces(keyValueParam[0]) || hasSpaces(keyValueParam[1])) {
                            return null;
                        }
                        parameters.put(keyValueParam[0].toLowerCase(Locale.ROOT), keyValueParam[1].toLowerCase(Locale.ROOT));
                    }
                    return new ParsedMediaType(xContentType, parameters);
                }
            }

        }
        return null;
    }

    private boolean hasSpaces(String s) {
        return s.trim().equals(s) == false;
    }

    /**
     * A media type object that contains all the information provided on a Content-Type or Accept header
     */
    public class ParsedMediaType {
        private final Map<String, String> parameters;
        private final T mediaType;

        public ParsedMediaType(T mediaType, Map<String, String> parameters) {
            this.parameters = parameters;
            this.mediaType = mediaType;
        }

        public T getMediaType() {
            return mediaType;
        }

        public Map<String, String> getParameters() {
            return parameters;
        }
    }
}
