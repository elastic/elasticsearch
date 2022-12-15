/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefArray;

import java.util.BitSet;

/**
 * Block implementation that stores an array of {@link org.apache.lucene.util.BytesRef}.
 */
public final class BytesRefArrayBlock extends NullsAwareBlock {

    private static final BytesRef NULL_VALUE = new BytesRef();
    private final BytesRefArray bytes;

    public BytesRefArrayBlock(int positionCount, BytesRefArray bytes) {
        this(positionCount, bytes, null);
    }

    public BytesRefArrayBlock(int positionCount, BytesRefArray bytes, BitSet nullsMask) {
        super(positionCount, nullsMask);
        assert bytes.size() == positionCount : bytes.size() + " != " + positionCount;
        this.bytes = bytes;
    }

    @Override
    public BytesRef getBytesRef(int position, BytesRef spare) {
        return bytes.get(position, spare);
    }

    @Override
    public Object getObject(int position) {
        return getBytesRef(position, new BytesRef());
    }

    @Override
    public String toString() {
        return "BytesRefArrayBlock{positions=" + getPositionCount() + '}';
    }

    public static Builder builder(int positionCount) {
        return new Builder(positionCount);
    }

    public static final class Builder {
        private final int positionCount;
        private final BytesRefArray bytes;

        private final BitSet nullsMask;

        public Builder(int positionCount) {
            this.positionCount = positionCount;
            this.bytes = new BytesRefArray(positionCount, BigArrays.NON_RECYCLING_INSTANCE);
            this.nullsMask = new BitSet(positionCount);
        }

        /**
         * Appends a {@link BytesRef} to the Block Builder.
         */
        public void append(BytesRef value) {
            if (bytes.size() >= positionCount) {
                throw new IllegalStateException("Block is full; expected " + positionCount + " values; got " + bytes.size());
            }
            bytes.append(value);
        }

        public void appendNull() {
            // Retrieve the size of the BytesRefArray so that we infer the current position
            // Then use the position to set the bit in the nullsMask
            int position = (int) bytes.size();
            nullsMask.set(position);
            append(NULL_VALUE);
        }

        public BytesRefArrayBlock build() {
            if (bytes.size() != positionCount) {
                throw new IllegalStateException("Incomplete block; expected " + positionCount + " values; got " + bytes.size());
            }
            // If nullsMask has no bit set, we pass null as the nulls mask, so that mayHaveNull() returns false
            return new BytesRefArrayBlock(positionCount, bytes, nullsMask);
        }

        // Method provided for testing only
        protected BytesRefArray getBytes() {
            return bytes;
        }
    }
}
