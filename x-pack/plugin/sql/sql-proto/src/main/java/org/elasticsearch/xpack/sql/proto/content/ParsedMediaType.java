/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.proto.content;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * NB: cloned from the class in ES XContent.
 */
class ParsedMediaType {
    // tchar pattern as defined by RFC7230 section 3.2.6
    private static final Pattern TCHAR_PATTERN = Pattern.compile("[a-zA-Z0-9!#$%&'*+\\-.\\^_`|~]+");

    private final String originalHeaderValue;
    private final String type;
    private final String subType;
    private final Map<String, String> parameters;

    private ParsedMediaType(String originalHeaderValue, String type, String subType, Map<String, String> parameters) {
        this.originalHeaderValue = originalHeaderValue;
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
     * follows https://tools.ietf.org/html/rfc7231#section-3.1.1.1
     * but allows only single media type. Media ranges will be ignored (treated as not provided)
     * Note: parsing can return null, but it will throw exceptions once https://github.com/elastic/elasticsearch/issues/63080 is done
     * TODO Do not rely on nulls
     *
     * @return a {@link ParsedMediaType} if the header could be parsed.
     * @throws IllegalArgumentException if the header is malformed
     */
    public static ParsedMediaType parseMediaType(String headerValue) {
        if (headerValue != null) {
            if (isMediaRange(headerValue) || "*/*".equals(headerValue)) {
                return null;
            }
            final String[] elements = headerValue.toLowerCase(Locale.ROOT).split(";");

            final String[] splitMediaType = elements[0].split("/");
            if ((splitMediaType.length == 2
                && TCHAR_PATTERN.matcher(splitMediaType[0].trim()).matches()
                && TCHAR_PATTERN.matcher(splitMediaType[1].trim()).matches()) == false) {
                throw new IllegalArgumentException("invalid media-type [" + headerValue + "]");
            }
            if (elements.length == 1) {
                return new ParsedMediaType(headerValue, splitMediaType[0].trim(), splitMediaType[1].trim(), new HashMap<>());
            } else {
                Map<String, String> parameters = new HashMap<>();
                for (int i = 1; i < elements.length; i++) {
                    String paramsAsString = elements[i].trim();
                    if (paramsAsString.isEmpty()) {
                        continue;
                    }
                    // spaces are allowed between parameters, but not between '=' sign
                    String[] keyValueParam = paramsAsString.split("=");
                    if (keyValueParam.length != 2 || hasTrailingSpace(keyValueParam[0]) || hasLeadingSpace(keyValueParam[1])) {
                        throw new IllegalArgumentException("invalid parameters for header [" + headerValue + "]");
                    }
                    String parameterName = keyValueParam[0].toLowerCase(Locale.ROOT).trim();
                    String parameterValue = keyValueParam[1].toLowerCase(Locale.ROOT).trim();
                    parameters.put(parameterName, parameterValue);
                }
                return new ParsedMediaType(
                    headerValue,
                    splitMediaType[0].trim().toLowerCase(Locale.ROOT),
                    splitMediaType[1].trim().toLowerCase(Locale.ROOT),
                    parameters
                );
            }
        }
        return null;
    }

    // simplistic check for media ranges. do not validate if this is a correct header
    private static boolean isMediaRange(String headerValue) {
        return headerValue.contains(",");
    }

    private static boolean hasTrailingSpace(String s) {
        return s.length() == 0 || Character.isWhitespace(s.charAt(s.length() - 1));
    }

    private static boolean hasLeadingSpace(String s) {
        return s.length() == 0 || Character.isWhitespace(s.charAt(0));
    }

    @Override
    public String toString() {
        return originalHeaderValue;
    }

    public String responseContentTypeHeader() {
        return mediaTypeWithoutParameters() + formatParameters(parameters);
    }

    private static String formatParameters(Map<String, String> params) {
        String joined = params.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.joining(";"));
        return joined.isEmpty() ? "" : ";" + joined;
    }
}
