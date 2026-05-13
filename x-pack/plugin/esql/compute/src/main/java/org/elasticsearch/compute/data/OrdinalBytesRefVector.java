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

/**
 * A {@link BytesRefVector} consists of a pair: an {@link IntVector} for ordinals and a {@link BytesRefVector} for the dictionary.
 * Compared to the regular {@link BytesRefVector}, this block is slower due to indirect access and consume more memory because of
 * the additional ordinals vector. However, they offer significant speed improvements and reduced memory usage when byte values are
 * frequently repeated
 */
public final class OrdinalBytesRefVector extends AbstractNonThreadSafeRefCounted implements BytesRefVector {
    private final IntVector ordinals;
    private final BytesRefVector bytes;
    /**
     * See {@link OrdinalBytesRefBlock#needsCompaction()}.
     */
    private final boolean needsCompaction;

    public OrdinalBytesRefVector(IntVector ordinals, BytesRefVector bytes) {
        this(ordinals, bytes, false);
    }

    public OrdinalBytesRefVector(IntVector ordinals, BytesRefVector bytes, boolean needsCompaction) {
        this.ordinals = ordinals;
        this.bytes = bytes;
        this.needsCompaction = needsCompaction;
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
        // Always serialize a phantom-free dictionary; see OrdinalBytesRefBlock#writeOrdinalBlock.
        try (OrdinalBytesRefVector c = compact()) {
            c.ordinals.writeTo(out);
            c.bytes.writeTo(out);
        }
    }

    /**
     * See {@link OrdinalBytesRefBlock#needsCompaction()}.
     */
    public boolean needsCompaction() {
        return needsCompaction;
    }

    /**
     * Vector counterpart of {@link OrdinalBytesRefBlock#referencedDictionaryEntries()}.
     */
    public boolean[] referencedDictionaryEntries() {
        boolean[] referenced = new boolean[bytes.getPositionCount()];
        int positionCount = ordinals.getPositionCount();
        for (int p = 0; p < positionCount; p++) {
            referenced[ordinals.getInt(p)] = true;
        }
        return referenced;
    }

    /**
     * Vector counterpart of {@link OrdinalBytesRefBlock#compact()}. Returns {@code this} (with an extra
     * ref) when no compaction is needed; otherwise returns a new vector with a phantom-free dictionary
     * and remapped ordinals. The returned vector must be closed by the caller.
     */
    public OrdinalBytesRefVector compact() {
        if (needsCompaction == false) {
            incRef();
            return this;
        }
        int dictSize = bytes.getPositionCount();
        boolean[] referenced = referencedDictionaryEntries();
        int referencedCount = 0;
        for (int oldOrd = 0; oldOrd < dictSize; oldOrd++) {
            if (referenced[oldOrd]) {
                referencedCount++;
            }
        }
        if (referencedCount == dictSize) {
            ordinals.incRef();
            bytes.incRef();
            return new OrdinalBytesRefVector(ordinals, bytes, false);
        }
        int positionCount = ordinals.getPositionCount();
        int[] remap = new int[dictSize];
        BytesRefVector compactBytes = null;
        IntVector remappedOrds = null;
        try (BytesRefVector.Builder dictBuilder = blockFactory().newBytesRefVectorBuilder(referencedCount)) {
            BytesRef scratch = new BytesRef();
            int newOrd = 0;
            for (int oldOrd = 0; oldOrd < dictSize; oldOrd++) {
                if (referenced[oldOrd]) {
                    dictBuilder.appendBytesRef(bytes.getBytesRef(oldOrd, scratch));
                    remap[oldOrd] = newOrd++;
                } else {
                    remap[oldOrd] = -1;
                }
            }
            compactBytes = dictBuilder.build();
            try (IntVector.Builder ordsBuilder = blockFactory().newIntVectorFixedBuilder(positionCount)) {
                for (int p = 0; p < positionCount; p++) {
                    ordsBuilder.appendInt(remap[ordinals.getInt(p)]);
                }
                remappedOrds = ordsBuilder.build();
            }
            OrdinalBytesRefVector result = new OrdinalBytesRefVector(remappedOrds, compactBytes, false);
            remappedOrds = null;
            compactBytes = null;
            return result;
        } finally {
            Releasables.closeExpectNoException(remappedOrds, compactBytes);
        }
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
        return new OrdinalBytesRefBlock(ordinals.asBlock(), bytes, needsCompaction);
    }

    @Override
    public OrdinalBytesRefVector asOrdinals() {
        return this;
    }

    public IntVector getOrdinalsVector() {
        return ordinals;
    }

    /**
     * See {@link OrdinalBytesRefBlock#getDictionaryVector()} for the phantom-entries contract.
     */
    public BytesRefVector getDictionaryVector() {
        return bytes;
    }

    @Override
    public BytesRefVector filter(boolean mayContainDuplicates, int... positions) {
        // Share the dictionary with the filtered vector; flagged needsCompaction so phantoms are stripped
        // at the wire boundary or by consumers that walk the dictionary directly.
        OrdinalBytesRefVector result = null;
        IntVector filteredOrdinals = ordinals.filter(mayContainDuplicates, positions);
        try {
            result = new OrdinalBytesRefVector(filteredOrdinals, bytes, true);
            bytes.incRef();
        } finally {
            if (result == null) {
                filteredOrdinals.close();
            }
        }
        return result;
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
        return new OrdinalBytesRefVector(slicedOrdinals, bytes, true);
    }

    @Override
    public OrdinalBytesRefVector deepCopy(BlockFactory blockFactory) {
        IntVector copiedOrdinals = null;
        BytesRefVector copiedBytes = null;
        try {
            copiedOrdinals = ordinals.deepCopy(blockFactory);
            copiedBytes = bytes.deepCopy(blockFactory);
            OrdinalBytesRefVector result = new OrdinalBytesRefVector(copiedOrdinals, copiedBytes, needsCompaction);
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
