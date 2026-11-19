/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.tokenattributes.PackedTokenAttributeImpl;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

/**
 * The attribute of {@link AsciiStandardTokenizer}: the same as Lucene's default {@link PackedTokenAttributeImpl}, with a cheaper
 * {@link #getBytesRef()} for ASCII terms. The indexing chain calls {@code getBytesRef()} for every token, and the default
 * implementation runs the general UTF-16 to UTF-8 encoder, which branches for every char. For an ASCII term the UTF-8 bytes are the
 * chars, so we narrow them in a loop without branches that the JIT can vectorize.
 * <p>
 * We look at the chars when asked and do not remember that the term was ASCII, so this stays correct if something else changes the
 * term buffer. Any non-ASCII term gets the bytes of the default implementation.
 */
final class AsciiTermAttributeImpl extends PackedTokenAttributeImpl {

    private BytesRefBuilder asciiBytes = new BytesRefBuilder();

    @Override
    public BytesRef getBytesRef() {
        final int length = length();
        asciiBytes.grow(length);
        final byte[] bytes = asciiBytes.bytes();
        final char[] chars = buffer();
        int bits = 0;
        for (int i = 0; i < length; i++) {
            final char c = chars[i];
            bits |= c;
            bytes[i] = (byte) c;
        }
        if (bits >= 128) {
            return super.getBytesRef();
        }
        asciiBytes.setLength(length);
        return asciiBytes.get();
    }

    @Override
    public AsciiTermAttributeImpl clone() {
        final AsciiTermAttributeImpl clone = (AsciiTermAttributeImpl) super.clone();
        // A clone must not share the scratch bytes, the caller may keep a BytesRef from each one.
        clone.asciiBytes = new BytesRefBuilder();
        return clone;
    }
}
