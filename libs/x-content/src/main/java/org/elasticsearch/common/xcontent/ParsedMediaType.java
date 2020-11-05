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

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * A raw result of parsing media types from Accept or Content-Type headers.
 * It follow parsing and validates as per  rules defined in https://tools.ietf.org/html/rfc7231#section-3.1.1.1
 * Can be resolved to <code>MediaType</code>
 * @see MediaType
 * @see MediaTypeRegistry
 */
public class ParsedMediaType {
    // TODO this should be removed once strict parsing is implemented https://github.com/elastic/elasticsearch/issues/63080
    // sun.net.www.protocol.http.HttpURLConnection sets a default Accept header if it was not provided on a request.
    // For this value Parsing returns null.
    public static final String DEFAULT_ACCEPT_STRING = "text/html, image/gif, image/jpeg, *; q=.2, */*; q=.2";

    private final String type;
    private final String subType;
    private final Map<String, String> parameters;
    // tchar pattern as defined by RFC7230 section 3.2.6
    private static final Pattern TCHAR_PATTERN = Pattern.compile("[a-zA-z0-9!#$%&'*+\\-.\\^_`|~]+");

    private ParsedMediaType(String type, String subType, Map<String, String> parameters) {
        this.type = type;
        this.subType = subType;
        this.parameters = Collections.unmodifiableMap(parameters);
    }

    /**
     * The parsed mime type without the associated parameters. Will always return lowercase.
     */
    public String mediaTypeWithoutParameters() {
        return type + "/" + subType;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    /**
     * Parses a header value into it's parts.
     * Note: parsing can return null, but it will throw exceptions once https://github.com/elastic/elasticsearch/issues/63080 is done
     * Do not rely on nulls
     *
     * @return a {@link ParsedMediaType} if the header could be parsed.
     * @throws IllegalArgumentException if the header is malformed
     */
    public static ParsedMediaType parseMediaType(String headerValue) {
        if (DEFAULT_ACCEPT_STRING.equals(headerValue) || "*/*".equals(headerValue)) {
            return null;
        }
        if (headerValue != null) {
            final String[] elements = headerValue.toLowerCase(Locale.ROOT).split("[\\s\\t]*;");

            final String[] splitMediaType = elements[0].split("/");
            if ((splitMediaType.length == 2 && TCHAR_PATTERN.matcher(splitMediaType[0].trim()).matches()
                && TCHAR_PATTERN.matcher(splitMediaType[1].trim()).matches()) == false) {
                throw new IllegalArgumentException("invalid media type [" + headerValue + "]");
            }
            if (elements.length == 1) {
                return new ParsedMediaType(splitMediaType[0].trim(), splitMediaType[1].trim(), Collections.emptyMap());
            } else {
                Map<String, String> parameters = new HashMap<>();
                for (int i = 1; i < elements.length; i++) {
                    String paramsAsString = elements[i].trim();
                    if (paramsAsString.isEmpty()) {
                        continue;
                    }
                    // intentionally allowing to have spaces around `=`
                    // https://tools.ietf.org/html/rfc7231#section-3.1.1.1 disallows this
                    String[] keyValueParam = elements[i].trim().split("=");
                    if (keyValueParam.length == 2) {
                        String parameterName = keyValueParam[0].toLowerCase(Locale.ROOT).trim();
                        String parameterValue = keyValueParam[1].toLowerCase(Locale.ROOT).trim();
                        parameters.put(parameterName, parameterValue);
                    } else {
                        throw new IllegalArgumentException("invalid parameters for header [" + headerValue + "]");
                    }
                }
                return new ParsedMediaType(splitMediaType[0].trim().toLowerCase(Locale.ROOT),
                    splitMediaType[1].trim().toLowerCase(Locale.ROOT), parameters);
            }
        }
        return null;
    }

    /**
     * Resolves this instance to a MediaType instance defined in given MediaTypeRegistry.
     * Performs validation against parameters.
     * @param mediaTypeRegistry a registry where a mapping between a raw media type to an instance MediaType is defined
     * @return a MediaType instance or null if no media type could be found or if a known parameter do not passes validation
     */
    public  <T extends MediaType> T toMediaType(MediaTypeRegistry<T> mediaTypeRegistry) {
        T type = mediaTypeRegistry.typeWithSubtypeToMediaType(mediaTypeWithoutParameters());
        if (type != null) {

            Map<String, Pattern> registeredParams = mediaTypeRegistry.parametersFor(mediaTypeWithoutParameters());
            for (Map.Entry<String, String> givenParamEntry : parameters.entrySet()) {
                if (isValidParameter(givenParamEntry.getKey(), givenParamEntry.getValue(), registeredParams) == false) {
                    return null;
                }
            }
            return type;
        }
        return null;
    }

    private boolean isValidParameter(String paramName, String value, Map<String, Pattern> registeredParams) {
        if (registeredParams.containsKey(paramName)) {
            Pattern regex = registeredParams.get(paramName);
            return regex.matcher(value).matches();
        }
        //TODO undefined parameters are allowed until https://github.com/elastic/elasticsearch/issues/63080
        return true;
    }
}
