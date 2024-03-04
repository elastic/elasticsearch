/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.io.IOException;

/**
 * Compact serializable container for ByteRefs
 */
public final class BytesRefArray implements Accountable, Releasable, Writeable {

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

    public static BytesRefArray readFrom(StreamInput in, BigArrays bigArrays, boolean readOnly) throws IOException {
        final long size = in.readVLong();
        final long sizeOfStartOffsets = size + 1;
        final LongArray startOffsets = bigArrays.newLongArray(sizeOfStartOffsets, false);
        boolean success = false;
        try {
            // startOffsets
            for (long i = 0; i < sizeOfStartOffsets; ++i) {
                startOffsets.set(i, in.readVLong());
            }
            final ByteArray bytes = readByteArray(in, bigArrays, readOnly);
            success = true;
            return new BytesRefArray(startOffsets, bytes, size, bigArrays);
        } finally {
            if (success == false) {
                Releasables.close(startOffsets);
            }
        }
    }

    private static ByteArray readByteArray(StreamInput in, BigArrays bigArrays, boolean readOnly) throws IOException {
        final long numBytes = in.readVLong();
        if (readOnly && in.supportReadAllToReleasableBytesReference() && Math.min(numBytes, ArrayUtil.MAX_ARRAY_LENGTH) == numBytes) {
            bigArrays.adjustBreaker(numBytes, true);
            Releasable releasable = () -> bigArrays.adjustBreaker(-numBytes, true);
            try {
                final ByteArray bytes = new ReleasableByteArray(in.readReleasableBytesReference(Math.toIntExact(numBytes)), releasable);
                releasable = null;
                return bytes;
            } finally {
                Releasables.close(releasable);
            }
        } else {
            final ByteArray bytes = bigArrays.newByteArray(numBytes, false);
            boolean success = false;
            try {
                if (bytes.hasArray()) {
                    in.readBytes(bytes.array(), 0, Math.toIntExact(numBytes));
                } else {
                    for (long i = 0; i < numBytes; ++i) {
                        bytes.set(i, in.readByte());
                    }
                }
                success = true;
                return bytes;
            } finally {
                if (success == false) {
                    Releasables.close(bytes);
                }
            }
        }
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
        if (bytes.hasArray()) {
            out.writeBytes(bytes.array(), 0, Math.toIntExact(sizeOfBytes));
        } else {
            final long maxReferenceSize = Math.min(sizeOfBytes, ArrayUtil.MAX_ARRAY_LENGTH);
            BytesReference.fromByteArray(bytes, Math.toIntExact(maxReferenceSize)).writeTo(out);
            for (long i = maxReferenceSize; i < sizeOfBytes; ++i) {
                out.writeByte(bytes.get(i));
            }
        }
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
