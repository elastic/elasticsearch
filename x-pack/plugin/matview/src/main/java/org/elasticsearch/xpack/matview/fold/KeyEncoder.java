/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.matview.fold;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

/**
 * Encodes the components of a composite group key into one byte string, so keys read from different segments,
 * where keyword ordinals differ, compare by value. Longs take eight bytes, byte strings are length prefixed, so
 * the encoding is unambiguous for any mix of components.
 */
public final class KeyEncoder {
    private final BytesRefBuilder out = new BytesRefBuilder();

    public KeyEncoder reset() {
        out.clear();
        return this;
    }

    public KeyEncoder addLong(long value) {
        for (int shift = 56; shift >= 0; shift -= 8) {
            out.append((byte) (value >>> shift));
        }
        return this;
    }

    public KeyEncoder addBytes(BytesRef value) {
        for (int shift = 24; shift >= 0; shift -= 8) {
            out.append((byte) (value.length >>> shift));
        }
        out.append(value);
        return this;
    }

    /** The encoded key; valid until the next {@link #reset()}. */
    public BytesRef get() {
        return out.get();
    }
}
