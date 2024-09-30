/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.io.IOException;

/**
 * A {@link BytesRefVector} consists of a pair: an {@link IntVector} for ordinals and a {@link BytesRefVector} for the dictionary.
 * Compared to the regular {@link BytesRefVector}, this block is slower due to indirect access and consume more memory because of
 * the additional ordinals vector. However, they offer significant speed improvements and reduced memory usage when byte values are
 * frequently repeated
 */
public final class OrdinalBytesRefVector extends AbstractNonThreadSafeRefCounted implements BytesRefVector {
    private final IntVector ordinals;
    private final BytesRefVector bytes;

    public OrdinalBytesRefVector(IntVector ordinals, BytesRefVector bytes) {
        this.ordinals = ordinals;
        this.bytes = bytes;
    }

    static OrdinalBytesRefVector readOrdinalVector(BlockFactory blockFactory, StreamInput in) throws IOException {
        IntVector ordinals = IntVector.readFrom(blockFactory, in);
        BytesRefVector bytes = null;
        OrdinalBytesRefVector result = null;
        try {
            bytes = BytesRefVector.readFrom(blockFactory, in);
            result = new OrdinalBytesRefVector(ordinals, bytes);
        } finally {
            if (result == null) {
                Releasables.close(ordinals, bytes);
            }
        }
        return result;
    }

    void writeOrdinalVector(StreamOutput out) throws IOException {
        ordinals.writeTo(out);
        bytes.writeTo(out);
    }

    /**
     * Returns true if this ordinal vector is dense enough to enable optimizations using its ordinals
     */
    public boolean isDense() {
        return ordinals.getPositionCount() * 2 / 3 >= bytes.getPositionCount();
    }

    @Override
    public int getPositionCount() {
        return ordinals.getPositionCount();
    }

    @Override
    public BlockFactory blockFactory() {
        return ordinals.blockFactory();
    }

    @Override
    public void allowPassingToDifferentDriver() {
        ordinals.allowPassingToDifferentDriver();
        bytes.allowPassingToDifferentDriver();
    }

    @Override
    public BytesRef getBytesRef(int position, BytesRef dest) {
        return bytes.getBytesRef(ordinals.getInt(position), dest);
    }

    @Override
    public OrdinalBytesRefBlock asBlock() {
        return new OrdinalBytesRefBlock(ordinals.asBlock(), bytes);
    }

    @Override
    public OrdinalBytesRefVector asOrdinals() {
        return this;
    }

    public IntVector getOrdinalsVector() {
        return ordinals;
    }

    public BytesRefVector getDictionaryVector() {
        return bytes;
    }

    @Override
    public BytesRefVector filter(int... positions) {
        if (positions.length >= ordinals.getPositionCount()) {
            OrdinalBytesRefVector result = null;
            IntVector filteredOrdinals = ordinals.filter(positions);
            try {
                result = new OrdinalBytesRefVector(filteredOrdinals, bytes);
                bytes.incRef();
            } finally {
                if (result == null) {
                    filteredOrdinals.close();
                }
            }
            return result;
        } else {
            final BytesRef scratch = new BytesRef();
            try (BytesRefVector.Builder builder = blockFactory().newBytesRefVectorBuilder(positions.length)) {
                for (int p : positions) {
                    builder.appendBytesRef(getBytesRef(p, scratch));
                }
                return builder.build();
            }
        }
    }

    @Override
    public BytesRefBlock keepMask(BooleanVector mask) {
        /*
         * The implementation in OrdinalBytesRefBlock is quite fast and
         * amounts to the same thing so we can just reuse it.
         */
        return asBlock().keepMask(mask);
    }

    @Override
    public ReleasableIterator<? extends BytesRefBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        return new BytesRefLookup(asBlock(), positions, targetBlockSize);
    }

    @Override
    public ElementType elementType() {
        return bytes.elementType();
    }

    @Override
    public boolean isConstant() {
        return bytes.isConstant() || ordinals.isConstant();
    }

    @Override
    public long ramBytesUsed() {
        return ordinals.ramBytesUsed() + bytes.ramBytesUsed();
    }

    @Override
    protected void closeInternal() {
        Releasables.close(ordinals, bytes);
    }
}
