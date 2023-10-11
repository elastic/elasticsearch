/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.common.util.BigArrays.indexIsInt;

public class ReleasableByteArray implements ByteArray {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(ReleasableByteArray.class);

    private final ReleasableBytesReference ref;

    ReleasableByteArray(StreamInput in) throws IOException {
        this.ref = in.readReleasableBytesReference();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBytesReference(ref);
    }

    @Override
    public long size() {
        return ref.length() / Byte.BYTES;
    }

    @Override
    public byte get(long index) {
        assert indexIsInt(index);
        return ref.get((int) index);
    }

    @Override
    public boolean get(long index, int len, BytesRef ref) {
        assert indexIsInt(index);
        BytesReference sliced = this.ref.slice((int) index, len);
        if (sliced.length() != 0) {
            ref.offset = sliced.arrayOffset();
            ref.length = sliced.length();
            ref.bytes = sliced.array();
            return true;
        } else {
            return false;
        }
    }

    @Override
    public byte set(long index, byte value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void set(long index, byte[] buf, int offset, int len) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void fill(long fromIndex, long toIndex, byte value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasArray() {
        return ref.hasArray();
    }

    @Override
    public byte[] array() {
        // The assumption of this method is that the returned array has valid entries starting from slot 0 and
        // this isn't case when just returning the array from ReleasableBytesReference#array().
        // The interface that this class implements should have something like an arrayOffset() method,
        // so that callers know from what array offset the first actual byte starts.
        throw new UnsupportedOperationException();
    }

    @Override
    public long ramBytesUsed() {
        /*
         * If we return the size of the buffer that we've sliced
         * we're likely to double count things.
         */
        return SHALLOW_SIZE;
    }

    @Override
    public void close() {
        ref.decRef();
    }

}
