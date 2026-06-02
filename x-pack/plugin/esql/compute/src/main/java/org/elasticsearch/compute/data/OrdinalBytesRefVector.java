/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.PagedBytesCursor;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.util.Arrays;

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
        return OrdinalBytesRefBlock.isDense(ordinals.getPositionCount(), bytes.getPositionCount());
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
    public PagedBytesCursor get(int position, PagedBytesCursor scratch) {
        return bytes.get(ordinals.getInt(position), scratch);
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
    public BytesRefVector filter(boolean mayContainDuplicates, int... positions) {
        // Preserve the ordinal encoding through filter so downstream consumers (BytesRefBlockHash, etc.)
        // keep the dictionary fast path. Dictionary entries that are no longer referenced are dropped,
        // because every consumer of OrdinalBytesRefVector hashes the entire dictionary unconditionally.
        final int dictSize = bytes.getPositionCount();
        final int[] remap = new int[dictSize];
        Arrays.fill(remap, -1);
        // keptOrds[i] is the original dictionary index that compacted to position i.
        final int[] keptOrds = new int[dictSize];
        int kept = 0;
        for (int p : positions) {
            int ord = ordinals.getInt(p);
            if (remap[ord] == -1) {
                remap[ord] = kept;
                keptOrds[kept] = ord;
                kept++;
            }
        }
        if (OrdinalBytesRefBlock.isDense(positions.length, kept) == false) {
            // Compacted dictionary would not be dense enough to pay for the per-row indirection
            // downstream (and would not serialize via SERIALIZE_BLOCK_ORDINAL either). Materialize
            // a plain BytesRefVector instead so consumers take their fastest non-ordinal path.
            return materializeFiltered(positions);
        }
        if (kept == dictSize) {
            // Fast path: every dictionary entry is still referenced. Share the dictionary
            // and only filter the ordinals; no remap needed.
            IntVector filteredOrds = ordinals.filter(mayContainDuplicates, positions);
            OrdinalBytesRefVector result = null;
            try {
                result = new OrdinalBytesRefVector(filteredOrds, bytes);
                bytes.incRef();
                return result;
            } finally {
                if (result == null) {
                    filteredOrds.close();
                }
            }
        }
        BytesRefVector newDict = null;
        IntVector newOrds = null;
        OrdinalBytesRefVector result = null;
        try {
            newDict = OrdinalBytesRefBlock.compactDictionary(bytes, keptOrds, kept, blockFactory());
            try (IntVector.Builder ordsBuilder = blockFactory().newIntVectorFixedBuilder(positions.length)) {
                for (int p : positions) {
                    ordsBuilder.appendInt(remap[ordinals.getInt(p)]);
                }
                newOrds = ordsBuilder.build();
            }
            result = new OrdinalBytesRefVector(newOrds, newDict);
            return result;
        } finally {
            if (result == null) {
                Releasables.close(newDict, newOrds);
            }
        }
    }

    private BytesRefVector materializeFiltered(int[] positions) {
        BytesRef scratch = new BytesRef();
        try (BytesRefVector.Builder builder = blockFactory().newBytesRefVectorBuilder(positions.length)) {
            for (int p : positions) {
                builder.appendBytesRef(getBytesRef(p, scratch));
            }
            return builder.build();
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
    public BytesRefVector slice(int beginInclusive, int endExclusive) {
        if (beginInclusive == 0 && endExclusive == getPositionCount()) {
            incRef();
            return this;
        }
        IntVector slicedOrdinals = ordinals.slice(beginInclusive, endExclusive);
        bytes.incRef();
        return new OrdinalBytesRefVector(slicedOrdinals, bytes);
    }

    @Override
    public OrdinalBytesRefVector deepCopy(BlockFactory blockFactory) {
        IntVector copiedOrdinals = null;
        BytesRefVector copiedBytes = null;
        try {
            copiedOrdinals = ordinals.deepCopy(blockFactory);
            copiedBytes = bytes.deepCopy(blockFactory);
            OrdinalBytesRefVector result = new OrdinalBytesRefVector(copiedOrdinals, copiedBytes);
            copiedOrdinals = null;
            copiedBytes = null;
            return result;
        } finally {
            Releasables.closeExpectNoException(copiedOrdinals, copiedBytes);
        }
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
    public int valueMaxByteSize() {
        return bytes.valueMaxByteSize();
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

    @Override
    public boolean equals(Object o) {
        if (o instanceof BytesRefVector other) {
            return BytesRefVector.equals(this, other);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return BytesRefVector.hash(this);
    }
}
