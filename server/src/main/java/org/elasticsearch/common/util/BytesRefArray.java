/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.io.IOException;

/**
 * Compact serializable container for ByteRefs
 */
public final class BytesRefArray extends AbstractRefCounted implements Accountable, Releasable, Writeable {

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
            bytes.fillWith(in);

            success = true;
        } finally {
            if (success == false) {
                close();
            }
        }
    }

    public BytesRefArray(LongArray startOffsets, ByteArray bytes, long size, BigArrays bigArrays) {
        this.bytes = bytes;
        this.startOffsets = startOffsets;
        this.size = size;
        this.bigArrays = bigArrays;
    }

    public void append(BytesRef key) {
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
        decRef();
    }

    @Override
    protected void closeInternal() {
        Releasables.close(bytes, startOffsets);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(size);
        long sizeOfStartOffsets = size + 1;

        // start offsets have 1 extra bucket
        for (long i = 0; i < sizeOfStartOffsets; ++i) {
            out.writeVLong(startOffsets.get(i));
        }

        // bytes might be overallocated, the last bucket of startOffsets contains the real size
        final long sizeOfBytes = startOffsets.get(size);
        out.writeVLong(sizeOfBytes);
        final BytesRefIterator bytesIt = bytes.iterator();
        BytesRef bytesRef;
        long remained = sizeOfBytes;
        while (remained > 0 && (bytesRef = bytesIt.next()) != null) {
            int length = Math.toIntExact(Math.min(remained, bytesRef.length));
            remained -= length;
            out.writeBytes(bytesRef.bytes, bytesRef.offset, length);
        }
        assert remained == 0 : remained;
    }

    @Override
    public long ramBytesUsed() {
        return BASE_RAM_BYTES_USED + bigArraysRamBytesUsed();
    }

    /**
     * Memory used by the {@link BigArrays} portion of this {@link BytesRefArray}.
     */
    public long bigArraysRamBytesUsed() {
        return startOffsets.ramBytesUsed() + bytes.ramBytesUsed();
    }

}
