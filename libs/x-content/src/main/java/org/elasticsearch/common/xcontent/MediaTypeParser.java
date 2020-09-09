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
    private Map<String, T> formatToMediaType = new HashMap<>();
    private Map<String, T> typeWithSubtypeToMediaType = new HashMap<>();

    public MediaTypeParser(T[] acceptedMediaTypes) {
        for (T mediaType : acceptedMediaTypes) {
            typeWithSubtypeToMediaType.put(mediaType.typeWithSubtype(), mediaType);
            formatToMediaType.put(mediaType.format(), mediaType);
        }
    }

    public MediaTypeParser(T[] acceptedMediaTypes, Map<String, T> additionalMediaTypes) {
        this(acceptedMediaTypes);
        for (Map.Entry<String, T> entry : additionalMediaTypes.entrySet()) {
            String typeWithSubtype = entry.getKey();
            T mediaType = entry.getValue();

            typeWithSubtypeToMediaType.put(typeWithSubtype.toLowerCase(Locale.ROOT), mediaType);
            formatToMediaType.put(mediaType.format(), mediaType);
        }
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
     * parsing media type that follows https://tools.ietf.org/html/rfc2616#section-3.7
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
                        String[] keyValueParam = split[i].trim().split("=");
                        // should we validate that there are no spaces between key = value?
                        if (keyValueParam.length != 2) {
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

    /**
     * A media type object that contains all the information provided on a Content-Type or Accept header
     * // TODO PG to be extended with getCompatibleAPIVersion and more
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
