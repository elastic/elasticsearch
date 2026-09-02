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

    private static final Pattern RANGE_HEADER_PATTERN = Pattern.compile("bytes=([0-9]+)-([0-9]*)");
    private static final Pattern SUFFIX_RANGE_PATTERN = Pattern.compile("bytes=-([0-9]+)");
    private static final Pattern CONTENT_RANGE_HEADER_PATTERN = Pattern.compile("bytes (?:(\\d+)-(\\d+)|\\*)/(?:(\\d+)|\\*)");

    /**
     * Parse a "Range" header per RFC 7233.
     *
     * Supports bounded, open-ended, and suffix ranges:
     * <ul>
     *   <li>Bounded: <code>Range: bytes={range_start}-{range_end}</code></li>
     *   <li>Open-ended: <code>Range: bytes={range_start}-</code> (end is null, meaning "to end of file")</li>
     *   <li>Suffix: <code>Range: bytes=-{suffix_length}</code> (last N bytes; start is negative, end is null)</li>
     * </ul>
     *
     * @see <a href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Range">MDN: Range header</a>
     * @param rangeHeaderValue The header value as a string
     * @return a {@link Range} instance representing the parsed value, or null if the header is malformed
     */
    public static Range parseRangeHeader(String rangeHeaderValue) {
        final Matcher matcher = RANGE_HEADER_PATTERN.matcher(rangeHeaderValue);
        if (matcher.matches()) {
            try {
                long start = Long.parseLong(matcher.group(1));
                String endGroup = matcher.group(2);
                Long end = (endGroup == null || endGroup.isEmpty()) ? null : Long.parseLong(endGroup);
                return new Range(start, end);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        final Matcher suffixMatcher = SUFFIX_RANGE_PATTERN.matcher(rangeHeaderValue);
        if (suffixMatcher.matches()) {
            try {
                long suffixLength = Long.parseLong(suffixMatcher.group(1));
                return new Range(-suffixLength, null);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

    /**
     * A HTTP "Range" from a Range header.
     *
     * @param start The start of the range. Negative values indicate a suffix range (last N bytes from end).
     * @param end The end of the range, or null for open-ended/suffix ranges.
     */
    public record Range(long start, Long end) {

        public Range(long start, long end) {
            this(start, (Long) end);
        }

        /**
         * Returns true if this is a suffix range (bytes=-N, meaning the last N bytes).
         */
        public boolean isSuffixRange() {
            return start < 0;
        }

        /**
         * Returns true if this is an open-ended range (no end specified, not a suffix range).
         */
        public boolean isOpenEnded() {
            return end == null && start >= 0;
        }

        /**
         * Resolves this (possibly relative) range against a known content length,
         * producing absolute byte offsets clamped to [0, contentLength).
         *
         * @return resolved absolute range, or null if the range is unsatisfiable (start &gt;= contentLength)
         */
        public ResolvedRange resolveAgainst(long contentLength) {
            long resolvedStart;
            long resolvedEnd;
            if (isSuffixRange()) {
                long suffixLength = -start;
                resolvedStart = Math.max(0, contentLength - suffixLength);
                resolvedEnd = contentLength - 1;
            } else {
                resolvedStart = start;
                resolvedEnd = end != null ? end : contentLength - 1;
            }
            if (resolvedStart >= contentLength) {
                return null;
            }
            resolvedEnd = Math.min(resolvedEnd, contentLength - 1);
            return new ResolvedRange(resolvedStart, resolvedEnd);
        }

        public String headerString() {
            if (start < 0) {
                return "bytes=-" + (-start);
            }
            return end != null ? "bytes=" + start + "-" + end : "bytes=" + start + "-";
        }
    }

    /**
     * An absolute byte range with both start and end resolved against a content length.
     */
    public record ResolvedRange(long start, long end) {
        public long length() {
            return end - start + 1;
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
