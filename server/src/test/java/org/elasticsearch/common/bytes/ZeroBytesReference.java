/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.bytes;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.common.util.PageCacheRecycler;

/**
 * A {@link BytesReference} of the given length which contains all zeroes.
 */
public class ZeroBytesReference extends AbstractBytesReference {

    public ZeroBytesReference(int length) {
        super(length);
        assert 0 <= length : length;
    }

    @Override
    public int indexOf(byte marker, int from) {
        assert 0 <= from && from <= length : from + " vs " + length;
        if (marker == 0 && from < length) {
            return from;
        } else {
            return -1;
        }
    }

    @Override
    public byte get(int index) {
        assert 0 <= index && index < length : index + " vs " + length;
        return 0;
    }

    @Override
    public BytesReference slice(int from, int length) {
        assert from + length <= this.length : from + " and " + length + " vs " + this.length;
        return new ZeroBytesReference(length);
    }

    @Override
    public long ramBytesUsed() {
        return 0;
    }

    @Override
    public BytesRef toBytesRef() {
        return new BytesRef(new byte[length], 0, length);
    }

    @Override
    public BytesRefIterator iterator() {
        if (length <= PageCacheRecycler.BYTE_PAGE_SIZE) {
            return super.iterator();
        }
        final byte[] buffer = new byte[PageCacheRecycler.BYTE_PAGE_SIZE];
        return new BytesRefIterator() {

            int remaining = length;

            @Override
            public BytesRef next() {
                if (remaining > 0) {
                    final int nextLength = Math.min(remaining, buffer.length);
                    remaining -= nextLength;
                    return new BytesRef(buffer, 0, nextLength);
                } else {
                    return null;
                }
            }
        };
    }
}
