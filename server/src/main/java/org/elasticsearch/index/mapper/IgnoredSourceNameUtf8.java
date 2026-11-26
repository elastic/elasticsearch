/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.util.ByteUtils;

/**
 * Helper for locating the end of the field name in an {@code _ignored_source} entry without decoding the value that follows it.
 */
final class IgnoredSourceNameUtf8 {

    private static final long NON_ASCII_MASK = 0x8080808080808080L;

    private IgnoredSourceNameUtf8() {}

    /**
     * Returns the number of UTF-8 bytes, starting at {@code start}, that encode the first {@code charCount} UTF-16 chars.
     * The header stores the name length in chars, but the name is followed by the value in the blob so its length in bytes has to be
     * derived by walking the lead bytes. A supplementary code point is four bytes and counts as two chars.
     * <p>
     * The walk trusts the name to be well-formed UTF-8, as written by {@link IgnoredSourceFieldMapper.SingularIgnoredSourceEncoding#encode}
     * , and only guards against running past {@code end} and against a continuation byte where a lead byte is expected, so a corrupt or
     * misframed entry fails here instead of being read as a different one. As a result the returned length equals {@code charCount} if
     * and only if the name is pure ASCII.
     */
    static int byteLength(byte[] bytes, int start, int end, int charCount) {
        int pos = start;
        int chars = 0;
        // Names are mostly ASCII, so skip 8 bytes at a time while all of them are, which is also 8 chars.
        while (chars + Long.BYTES <= charCount
            && pos + Long.BYTES <= end
            && ((long) ByteUtils.LITTLE_ENDIAN_LONG.get(bytes, pos) & NON_ASCII_MASK) == 0) {
            pos += Long.BYTES;
            chars += Long.BYTES;
        }
        for (; chars < charCount; chars++) {
            if (pos >= end) {
                throw new IllegalStateException("Failed to decode _ignored_source, name is longer than the entry");
            }
            int lead = bytes[pos] & 0xFF;
            if (lead < 0x80) {
                pos += 1;
            } else if (lead < 0xC0) {
                throw new IllegalStateException("Failed to decode _ignored_source, name has an invalid UTF-8 lead byte");
            } else if (lead < 0xE0) {
                pos += 2;
            } else if (lead < 0xF0) {
                pos += 3;
            } else {
                pos += 4;
                // a supplementary code point is a surrogate pair in UTF-16
                chars++;
            }
        }
        if (pos > end) {
            throw new IllegalStateException("Failed to decode _ignored_source, name is longer than the entry");
        }
        return pos - start;
    }
}
