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
    private static final Pattern CONTENT_RANGE_HEADER_PATTERN = Pattern.compile("bytes (?:(\\d+)-(\\d+)|\\*)/(?:(\\d+)|\\*)");

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

    public record Range(long start, long end) {

        public String headerString() {
            return "bytes=" + start + "-" + end;
        }
    }

    /**
     * Parse a "Content-Range" header
     *
     * @see <a href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Range">MDN: Content-Range header</a>
     *
     * @param contentRangeHeaderValue The header value as a string
     * @return a {@link ContentRange} instance representing the parsed value, or null if the header is malformed
     */
    public static ContentRange parseContentRangeHeader(String contentRangeHeaderValue) {
        final Matcher matcher = CONTENT_RANGE_HEADER_PATTERN.matcher(contentRangeHeaderValue);
        if (matcher.matches()) {
            try {
                if (matcher.groupCount() == 3) {
                    final Long start = parseOptionalLongValue(matcher.group(1));
                    final Long end = parseOptionalLongValue(matcher.group(2));
                    final Long size = parseOptionalLongValue(matcher.group(3));
                    return new ContentRange(start, end, size);
                }
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

    private static Long parseOptionalLongValue(String value) {
        return value == null ? null : Long.parseLong(value);
    }

    /**
     * A HTTP "Content Range"
     * <p>
     * This will always contain one of the following combinations of values:
     * <ul>
     *     <li>start, end, size</li>
     *     <li>start, end</li>
     *     <li>size</li>
     *     <li>nothing</li>
     * </ul>
     *
     * @param start The start of the range
     * @param end The end of the range
     * @param size The total size
     */
    public record ContentRange(Long start, Long end, Long size) {

        public ContentRange {
            assert (start == null) == (end == null) : "Must have either start and end or neither";
        }

        public boolean hasRange() {
            return start != null && end != null;
        }

        public boolean hasSize() {
            return size != null;
        }

        public String headerString() {
            final String rangeString = hasRange() ? start + "-" + end : "*";
            final String sizeString = hasSize() ? String.valueOf(size) : "*";
            return "bytes " + rangeString + "/" + sizeString;
        }
    }
}
