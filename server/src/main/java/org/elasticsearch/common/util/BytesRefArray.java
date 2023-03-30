/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.io.IOException;

/**
 * Compact serializable container for ByteRefs
 */
public class BytesRefArray implements Accountable, Releasable, Writeable {

    // base size of the bytes ref array
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(BytesRefArray.class);

    private final BigArrays bigArrays;
    private LongArray startOffsets;
    private ByteArray bytes;
    private long size;

    public BytesRefArray(long capacity, BigArrays bigArrays) {
        this.bigArrays = bigArrays;
        boolean success = false;
        try {
            startOffsets = bigArrays.newLongArray(capacity + 1, false);
            startOffsets.set(0, 0);
            bytes = bigArrays.newByteArray(capacity * 3, false);
            success = true;
        } finally {
            if (false == success) {
                close();
            }
        }
        size = 0;
    }

    public BytesRefArray(StreamInput in, BigArrays bigArrays) throws IOException {
        this.bigArrays = bigArrays;
        // we allocate big arrays so we have to `close` if we fail here or we'll leak them.
        boolean success = false;
        try {
            // startOffsets
            size = in.readVLong();
            long sizeOfStartOffsets = size + 1;
            startOffsets = bigArrays.newLongArray(sizeOfStartOffsets, false);
            for (long i = 0; i < sizeOfStartOffsets; ++i) {
                startOffsets.set(i, in.readVLong());
            }

            // bytes
            long sizeOfBytes = in.readVLong();
            bytes = bigArrays.newByteArray(sizeOfBytes, false);

            for (long i = 0; i < sizeOfBytes; ++i) {
                bytes.set(i, in.readByte());
            }

            success = true;
        } finally {
            if (success == false) {
                close();
            }
        }
    }

    private BytesRefArray(LongArray startOffsets, ByteArray bytes, long size, BigArrays bigArrays) {
        this.bytes = bytes;
        this.startOffsets = startOffsets;
        this.size = size;
        this.bigArrays = bigArrays;
    }

    public void append(BytesRef key) {
        assert startOffsets != null : "using BytesRefArray after ownership taken";
        final long startOffset = startOffsets.get(size);
        startOffsets = bigArrays.grow(startOffsets, size + 2);
        startOffsets.set(size + 1, startOffset + key.length);
        ++size;
        if (key.length > 0) {
            bytes = bigArrays.grow(bytes, startOffset + key.length);
            bytes.set(startOffset, key.bytes, key.offset, key.length);
        }
    }

    /**
     * Return the key at <code>0 &lt;= index &lt;= capacity()</code>. The result is undefined if the slot is unused.
     * <p>Beware that the content of the {@link BytesRef} may become invalid as soon as {@link #close()} is called</p>
     */
    public BytesRef get(long id, BytesRef dest) {
        assert startOffsets != null : "using BytesRefArray after ownership taken";
        final long startOffset = startOffsets.get(id);
        final int length = (int) (startOffsets.get(id + 1) - startOffset);
        bytes.get(startOffset, length, dest);
        return dest;
    }

    public long size() {
        return size;
    }

    @Override
    public void close() {
        Releasables.close(bytes, startOffsets);
    }

    /**
     * Create new instance and pass ownership of this array to the new one.
     *
     * Note, this closes this array. Don't use it after passing ownership.
     *
     * @param other BytesRefArray to claim ownership from
     * @return a new BytesRefArray instance with the payload of other
     */
    public static BytesRefArray takeOwnershipOf(BytesRefArray other) {
        BytesRefArray b = new BytesRefArray(other.startOffsets, other.bytes, other.size, other.bigArrays);

        // don't leave a broken array behind, although it isn't used any longer
        // on append both arrays get re-allocated
        other.startOffsets = null;
        other.bytes = null;
        other.size = 0;

        return b;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        assert startOffsets != null : "using BytesRefArray after ownership taken";
        out.writeVLong(size);
        long sizeOfStartOffsets = size + 1;

        // start offsets have 1 extra bucket
        for (long i = 0; i < sizeOfStartOffsets; ++i) {
            out.writeVLong(startOffsets.get(i));
        }

        // bytes might be overallocated, the last bucket of startOffsets contains the real size
        long sizeOfBytes = startOffsets.get(size);
        out.writeVLong(sizeOfBytes);
        for (long i = 0; i < sizeOfBytes; ++i) {
            out.writeByte(bytes.get(i));
        }
    }

    @Override
    public long ramBytesUsed() {
        return BASE_RAM_BYTES_USED + startOffsets.ramBytesUsed() + bytes.ramBytesUsed();
    }

}
