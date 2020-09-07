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
    private Map<String, T> typeSubtypeToMediaType = new HashMap<>();

    public MediaTypeParser(T[] acceptedMediaTypes) {
        for (T mediaType : acceptedMediaTypes) {
            typeSubtypeToMediaType.put(mediaType.typeSubtype(), mediaType);
            formatToMediaType.put(mediaType.format(), mediaType);
        }
    }

    public T fromMediaType(String mediaType) {
        ParsedMediaType parsedMediaType = parseMediaType(mediaType);
        return parsedMediaType != null ? parsedMediaType.getMediaType() : null;
    }

    public T fromFormat(String format) {
        if(format == null) {
            return null;
        }
        return formatToMediaType.get(format.toLowerCase(Locale.ROOT));
    }

    public MediaTypeParser<T> withAdditionalMediaType(String typeSubtype, T xContentType) {
        typeSubtypeToMediaType.put(typeSubtype.toLowerCase(Locale.ROOT), xContentType);
        formatToMediaType.put(xContentType.format(), xContentType);
        return this;
    }

    public ParsedMediaType parseMediaType(String mediaType) {
        if (mediaType != null) {
            String headerValue = mediaType.toLowerCase(Locale.ROOT);
            // split string on semicolon
            // validate media type is accepted (IIRC whitespace is ok so trim it) //TODO PG whitespace only ok in params
            // rest of strings are params. validate per RFC 7230 and use ones that we care about
            // or use a regex and we can change if necessary
            String[] split = headerValue.split(";");

            String[] typeSubtype =  split[0].toLowerCase(Locale.ROOT)
                                .split("/");
            if (typeSubtype.length == 2) {
                String type = typeSubtype[0];
                String subtype = typeSubtype[1];
                T xContentType = typeSubtypeToMediaType.get(type + "/" + subtype);
                if (xContentType != null) {
                    Map<String, String> parameters = new HashMap<>();
                    for (int i = 1; i < split.length; i++) {
                        String[] keyValueParam = split[i].trim().split("=");
                        parameters.put(keyValueParam[0].toLowerCase(Locale.ROOT), keyValueParam[1].toLowerCase(Locale.ROOT));
                    }
                    return new ParsedMediaType(type, subtype, parameters, xContentType);
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
        private final String type;
        private final String subtype;
        private final Map<String, String> parameters;
        private final T mediaType;

        public ParsedMediaType(String type, String subtype, Map<String, String> parameters, T mediaType) {
            this.type = type;
            this.subtype = subtype;
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
