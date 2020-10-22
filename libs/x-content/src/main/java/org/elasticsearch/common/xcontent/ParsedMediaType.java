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

public class ParsedMediaType {

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
    public String mimeTypeWithoutParams() {
        return type + "/" + subType;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    /**
     * Parses a header value into it's parts.
     *
     * @return a {@link ParsedMediaType} if the header could be parsed. TODO: don't return null
     * @throws IllegalArgumentException if the header is malformed
     */
    public static ParsedMediaType parseMediaType(String headerValue) {
        if (headerValue != null) {
            final String[] elements = headerValue.split("[\\s\\t]*;");

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
        throw new IllegalArgumentException("invalid media type [" + headerValue + "]");
    }

    public  <T extends MediaType> T toMediaType(MediaTypeRegistry<T> mediaTypeRegistry) {
        T type = mediaTypeRegistry.typeWithSubtypeToMediaType(mimeTypeWithoutParams());
        if (type != null) {

            Map<String, Pattern> registeredParams = mediaTypeRegistry.parametersFor(mimeTypeWithoutParams());
            for (Map.Entry<String, String> givenParamEntry : parameters.entrySet()) {
                if (isValidParameter(givenParamEntry.getKey(), givenParamEntry.getValue(), registeredParams) == false) {
                    throw new IllegalArgumentException("Invalid param "+ givenParamEntry.getKey()+"="+givenParamEntry.getValue()
                        +"for media type " + mimeTypeWithoutParams());
                }
            }
            return type;
        }

        throw new IllegalArgumentException("Unknown media type "+mimeTypeWithoutParams());
    }

    private boolean isValidParameter(String paramName, String value, Map<String, Pattern> registeredParams) {
        if(registeredParams.containsKey(paramName)){
            Pattern regex = registeredParams.get(paramName);
            return regex.matcher(value).matches();
        }
        //we don't validate undefined parameters
        return true;
    }
}
