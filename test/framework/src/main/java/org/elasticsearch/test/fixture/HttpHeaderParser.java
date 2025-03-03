/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.fixture;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public enum HttpHeaderParser {
    ;

    private static final Pattern RANGE_HEADER_PATTERN = Pattern.compile("bytes=([0-9]+)-([0-9]+)");

    /**
     * Parse a "Range" header
     *
     * Note: only a single bounded range is supported (e.g. <code>Range: bytes={range_start}-{range_end}</code>)
     *
     * @see <a href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Range">MDN: Range header</a>
     * @param rangeHeaderValue The header value as a string
     * @return a {@link Range} instance representing the parsed value, or null if the header is malformed
     */
    public static Range parseRangeHeader(String rangeHeaderValue) {
        final Matcher matcher = RANGE_HEADER_PATTERN.matcher(rangeHeaderValue);
        if (matcher.matches()) {
            try {
                return new Range(Long.parseLong(matcher.group(1)), Long.parseLong(matcher.group(2)));
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

    public record Range(long start, long end) {}
}
