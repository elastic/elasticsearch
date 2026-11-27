/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson;

/** A {@link Utf8Value} whose bytes are already UTF-8 in a slice of an array. */
final class MaterializedUtf8Value implements Utf8Value {

    private final byte[] bytes;
    private final int offset;
    private final int length;

    MaterializedUtf8Value(byte[] bytes, int offset, int length) {
        this.bytes = bytes;
        this.offset = offset;
        this.length = length;
    }

    @Override
    public int maxLength() {
        return length;
    }

    @Override
    public int copyTo(byte[] dst, int dstOffset) {
        System.arraycopy(bytes, offset, dst, dstOffset, length);
        return length;
    }
}
