/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common;

import java.util.Collection;
import java.util.Map;

/**
 * Appends a value to a {@link StringBuilder} in the format of its {@code toString()}, but stops after a given number of characters.
 */
public final class TruncatedAppender {

    private TruncatedAppender() {}

    /**
     * Appends at most {@code maxLength} characters of {@code value}. Maps, map entries and collections are rendered as
     * {@link java.util.AbstractMap#toString()} and {@link java.util.AbstractCollection#toString()} render them, and character sequences
     * are copied, without building the full string first. Any other value is rendered with {@link String#valueOf(Object)} and then cut.
     * A cut never leaves the first half of a surrogate pair at the end.
     *
     * @return {@code false} if some of the value was left out because of the limit
     */
    public static boolean append(StringBuilder b, Object value, int maxLength) {
        if (maxLength < 0) {
            throw new IllegalArgumentException("maxLength must not be negative but was [" + maxLength + "]");
        }
        return appendBounded(b, value, (int) Math.min((long) b.length() + maxLength, Integer.MAX_VALUE));
    }

    private static boolean appendBounded(StringBuilder b, Object value, int limit) {
        if (value instanceof Map<?, ?> map) {
            return appendAllBounded(b, map.entrySet(), "{", "}", limit);
        }
        if (value instanceof Map.Entry<?, ?> entry) {
            return appendBounded(b, entry.getKey(), limit) && appendBounded(b, "=", limit) && appendBounded(b, entry.getValue(), limit);
        }
        if (value instanceof Collection<?> collection) {
            return appendAllBounded(b, collection, "[", "]", limit);
        }
        CharSequence text = value instanceof CharSequence chars ? chars : String.valueOf(value);
        int room = limit - b.length();
        if (text.length() <= room) {
            b.append(text);
            return true;
        }
        // do not leave half of a surrogate pair at the end
        b.append(text, 0, room > 0 && Character.isHighSurrogate(text.charAt(room - 1)) ? room - 1 : room);
        return false;
    }

    private static boolean appendAllBounded(StringBuilder b, Collection<?> items, String open, String close, int limit) {
        if (appendBounded(b, open, limit) == false) {
            return false;
        }
        String separator = "";
        for (Object item : items) {
            if (appendBounded(b, separator, limit) == false || appendBounded(b, item, limit) == false) {
                return false;
            }
            separator = ", ";
        }
        return appendBounded(b, close, limit);
    }
}
