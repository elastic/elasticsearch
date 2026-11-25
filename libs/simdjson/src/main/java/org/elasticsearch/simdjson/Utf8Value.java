/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson;

import org.elasticsearch.simdjson.internal.parsers.StringParser;

/**
 * The UTF-8 bytes of a string value, written out with {@link #copyTo}.
 *
 * <p>The bytes a value refers to are borrowed: they must not change until the value has been copied.
 */
public sealed interface Utf8Value permits MaterializedUtf8Value {

    /** Slack that {@link #copyTo} may write past the end of the value. */
    int PADDING = StringParser.DESTINATION_PADDING;

    /** A value whose UTF-8 bytes are {@code bytes[offset, offset + length)}. */
    static Utf8Value of(byte[] bytes, int offset, int length) {
        return new MaterializedUtf8Value(bytes, offset, length);
    }

    /** Upper bound on the number of bytes {@link #copyTo} writes, excluding {@link #PADDING}. */
    int maxLength();

    /**
     * Writes the UTF-8 bytes of the value to {@code dst} starting at {@code dstOffset}, and returns how many
     * bytes form the value. {@code dst} must have room for {@link #maxLength()} plus {@link #PADDING} bytes
     * from {@code dstOffset}; bytes past the returned length are scratch.
     */
    int copyTo(byte[] dst, int dstOffset);
}
