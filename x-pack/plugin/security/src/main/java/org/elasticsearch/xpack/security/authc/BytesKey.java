/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;

import java.util.Arrays;

/**
 * Simple wrapper around bytes so that it can be used as a cache key. The hashCode is computed
 * once upon creation and cached.
 */
public final class BytesKey {

    final byte[] bytes;
    private final int hashCode;

    public BytesKey(byte[] bytes) {
        this.bytes = bytes;
        this.hashCode = StringHelper.murmurhash3_x86_32(bytes, 0, bytes.length, StringHelper.GOOD_FAST_HASH_SEED);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public boolean equals(Object other) {
        if (other == null) {
            return false;
        }
        if (other instanceof BytesKey == false) {
            return false;
        }

        BytesKey otherBytes = (BytesKey) other;
        return Arrays.equals(otherBytes.bytes, bytes);
    }

    @Override
    public String toString() {
        return new BytesRef(bytes).toString();
    }
}
