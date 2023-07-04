/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class ReleasableLongArray implements LongArray {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(ReleasableLongArray.class);

    private final ReleasableBytesReference ref;

    ReleasableLongArray(StreamInput in) throws IOException {
        this.ref = in.readReleasableBytesReference();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBytesReference(ref);
    }

    @Override
    public long size() {
        return ref.length() / Long.BYTES;
    }

    @Override
    public long get(long index) {
        if (index > Integer.MAX_VALUE / Long.BYTES) {
            // We can't serialize messages longer than 2gb anyway
            throw new ArrayIndexOutOfBoundsException();
        }
        return ref.getLongLE((int) index * Long.BYTES);
    }

    @Override
    public long set(long index, long value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long increment(long index, long inc) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void fill(long fromIndex, long toIndex, long value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void set(long index, byte[] buf, int offset, int len) {
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
