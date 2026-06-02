/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.PagedBytesCursor;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.util.Arrays;

/**
 * A {@link BytesRefBlock} consists of a pair: an {@link IntBlock} for ordinals and a {@link BytesRefVector} for the dictionary.
 * Compared to the regular {@link BytesRefBlock}, this block is slower due to indirect access and consume more memory because of
 * the additional ordinals block. However, they offer significant speed improvements and reduced memory usage when byte values are
 * frequently repeated
 */
public final class OrdinalBytesRefBlock extends AbstractNonThreadSafeRefCounted implements BytesRefBlock {
    private final IntBlock ordinals;
    private final BytesRefVector bytes;

    public OrdinalBytesRefBlock(IntBlock ordinals, BytesRefVector bytes) {
        this.ordinals = ordinals;
        this.bytes = bytes;
    }

    static OrdinalBytesRefBlock readOrdinalBlock(BlockFactory blockFactory, BlockStreamInput in) throws IOException {
        BytesRefVector bytes = null;
        OrdinalBytesRefBlock result = null;
        IntBlock ordinals = IntBlock.readFrom(in);
        try {
            bytes = BytesRefVector.readFrom(blockFactory, in);
            result = new OrdinalBytesRefBlock(ordinals, bytes);
        } finally {
            if (result == null) {
                Releasables.close(ordinals, bytes);
            }
        }
        return result;
    }

    void writeOrdinalBlock(StreamOutput out) throws IOException {
        ordinals.writeTo(out);
        bytes.writeTo(out);
    }

    /**
     * Returns true if this ordinal block is dense enough to enable optimizations using its ordinals
     */
    public boolean isDense() {
        return isDense(ordinals.getTotalValueCount(), bytes.getPositionCount());
    }

    public static boolean isDense(long totalPositions, long dictionarySize) {
        return totalPositions >= 10 && totalPositions >= dictionarySize * 2L;
    }

    public IntBlock getOrdinalsBlock() {
        return ordinals;
    }

    public BytesRefVector getDictionaryVector() {
        return bytes;
    }

    @Override
    public BytesRef getBytesRef(int valueIndex, BytesRef dest) {
        return bytes.getBytesRef(ordinals.getInt(valueIndex), dest);
    }

    @Override
    public PagedBytesCursor get(int valueIndex, PagedBytesCursor scratch) {
        return bytes.get(ordinals.getInt(valueIndex), scratch);
    }

    @Override
    public OrdinalBytesRefVector asVector() {
        IntVector vector = ordinals.asVector();
        if (vector != null) {
            return new OrdinalBytesRefVector(vector, bytes);
        } else {
            return null;
        }
    }

    @Override
    public OrdinalBytesRefBlock asOrdinals() {
        return this;
    }

    @Override
    public BytesRefBlock slice(int beginInclusive, int endExclusive) {
        if (beginInclusive == 0 && endExclusive == getPositionCount()) {
            incRef();
            return this;
        }
        IntBlock slicedOrdinals = ordinals.slice(beginInclusive, endExclusive);
        bytes.incRef();
        return new OrdinalBytesRefBlock(slicedOrdinals, bytes);
    }

    @Override
    public BytesRefBlock filter(boolean mayContainDuplicates, int... positions) {
        final OrdinalBytesRefVector vector = asVector();
        if (vector != null) {
            return vector.filter(mayContainDuplicates, positions).asBlock();
        }
        // Preserve the ordinal encoding through filter so downstream consumers (BytesRefBlockHash, etc.)
        // keep the dictionary fast path. Dictionary entries that are no longer referenced are dropped,
        // because every consumer of OrdinalBytesRefBlock hashes the entire dictionary unconditionally.
        final int dictSize = bytes.getPositionCount();
        final int[] remap = new int[dictSize];
        Arrays.fill(remap, -1);
        // keptOrds[i] is the original dictionary index that compacted to position i.
        final int[] keptOrds = new int[dictSize];
        int kept = 0;
        // totalValues sums the (non-null) value count across the filtered positions so we can
        // measure dictionary density on the same scale isDense() uses elsewhere (which counts
        // total values, not positions, to handle multivalues correctly).
        int totalValues = 0;
        for (int p : positions) {
            if (ordinals.isNull(p)) {
                continue;
            }
            int valueCount = ordinals.getValueCount(p);
            int first = ordinals.getFirstValueIndex(p);
            totalValues += valueCount;
            for (int c = 0; c < valueCount; c++) {
                int ord = ordinals.getInt(first + c);
                if (remap[ord] == -1) {
                    remap[ord] = kept;
                    keptOrds[kept] = ord;
                    kept++;
                }
            }
        }
        if (isDense(totalValues, kept) == false) {
            // Compacted dictionary would not be dense enough to pay for the per-row indirection
            // downstream. Materialize a plain BytesRefBlock instead.
            return materializeFiltered(positions);
        }
        if (kept == dictSize) {
            // Fast path: every dictionary entry is still referenced. Share the dictionary
            // and only filter the ordinals; no remap needed.
            IntBlock filteredOrds = ordinals.filter(mayContainDuplicates, positions);
            OrdinalBytesRefBlock result = null;
            try {
                result = new OrdinalBytesRefBlock(filteredOrds, bytes);
                bytes.incRef();
                return result;
            } finally {
                if (result == null) {
                    filteredOrds.close();
                }
            }
        }
        BytesRefVector newDict = null;
        IntBlock newOrds = null;
        OrdinalBytesRefBlock result = null;
        try {
            newDict = compactDictionary(bytes, keptOrds, kept, blockFactory());
            try (IntBlock.Builder ordsBuilder = blockFactory().newIntBlockBuilder(positions.length)) {
                for (int p : positions) {
                    if (ordinals.isNull(p)) {
                        ordsBuilder.appendNull();
                        continue;
                    }
                    int valueCount = ordinals.getValueCount(p);
                    int first = ordinals.getFirstValueIndex(p);
                    if (valueCount == 1) {
                        ordsBuilder.appendInt(remap[ordinals.getInt(first)]);
                    } else {
                        ordsBuilder.beginPositionEntry();
                        for (int c = 0; c < valueCount; c++) {
                            ordsBuilder.appendInt(remap[ordinals.getInt(first + c)]);
                        }
                        ordsBuilder.endPositionEntry();
                    }
                }
                newOrds = ordsBuilder.mvOrdering(mvOrdering()).build();
            }
            result = new OrdinalBytesRefBlock(newOrds, newDict);
            return result;
        } finally {
            if (result == null) {
                Releasables.close(newDict, newOrds);
            }
        }
    }

    @Override
    public BytesRefBlock keepMask(BooleanVector mask) {
        if (getPositionCount() == 0) {
            incRef();
            return this;
        }
        if (mask.isConstant()) {
            if (mask.getBoolean(0)) {
                incRef();
                return this;
            }
            return (BytesRefBlock) blockFactory().newConstantNullBlock(getPositionCount());
        }
        OrdinalBytesRefBlock result = null;
        IntBlock filteredOrdinals = ordinals.keepMask(mask);
        try {
            result = new OrdinalBytesRefBlock(filteredOrdinals, bytes);
            bytes.incRef();
        } finally {
            if (result == null) {
                filteredOrdinals.close();
            }
        }
        return result;
    }

    private BytesRefBlock materializeFiltered(int[] positions) {
        BytesRef scratch = new BytesRef();
        try (BytesRefBlock.Builder builder = blockFactory().newBytesRefBlockBuilder(positions.length)) {
            for (int pos : positions) {
                if (isNull(pos)) {
                    builder.appendNull();
                    continue;
                }
                int valueCount = getValueCount(pos);
                int first = getFirstValueIndex(pos);
                if (valueCount == 1) {
                    builder.appendBytesRef(getBytesRef(first, scratch));
                } else {
                    builder.beginPositionEntry();
                    for (int c = 0; c < valueCount; c++) {
                        builder.appendBytesRef(getBytesRef(first + c, scratch));
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.mvOrdering(mvOrdering()).build();
        }
    }

    /**
     * Builds a new dictionary {@link BytesRefVector} containing only the entries listed in {@code keptOrds[0..kept)},
     * preserving their order. Shared between {@link OrdinalBytesRefBlock#filter} and {@link OrdinalBytesRefVector#filter}.
     */
    static BytesRefVector compactDictionary(BytesRefVector source, int[] keptOrds, int kept, BlockFactory blockFactory) {
        BytesRef scratch = new BytesRef();
        try (BytesRefVector.Builder builder = blockFactory.newBytesRefVectorBuilder(kept)) {
            for (int k = 0; k < kept; k++) {
                builder.appendBytesRef(source.getBytesRef(keptOrds[k], scratch));
            }
            return builder.build();
        }
    }

    @Override
    public ReleasableIterator<BytesRefBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        return new BytesRefLookup(this, positions, targetBlockSize);
    }

    @Override
    public OrdinalBytesRefBlock deepCopy(BlockFactory blockFactory) {
        IntBlock copiedOrdinals = null;
        BytesRefVector copiedBytes = null;
        try {
            copiedOrdinals = ordinals.deepCopy(blockFactory);
            copiedBytes = bytes.deepCopy(blockFactory);
            OrdinalBytesRefBlock result = new OrdinalBytesRefBlock(copiedOrdinals, copiedBytes);
            copiedOrdinals = null;
            copiedBytes = null;
            return result;
        } finally {
            Releasables.closeExpectNoException(copiedOrdinals, copiedBytes);
        }
    }

    @Override
    protected void closeInternal() {
        Releasables.close(ordinals, bytes);
    }

    @Override
    public int getTotalValueCount() {
        return ordinals.getTotalValueCount();
    }

    @Override
    public int getPositionCount() {
        return ordinals.getPositionCount();
    }

    @Override
    public int getFirstValueIndex(int position) {
        return ordinals.getFirstValueIndex(position);
    }

    @Override
    public int getValueCount(int position) {
        return ordinals.getValueCount(position);
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
    public BlockFactory blockFactory() {
        return ordinals.blockFactory();
    }

    @Override
    public void allowPassingToDifferentDriver() {
        ordinals.allowPassingToDifferentDriver();
        bytes.allowPassingToDifferentDriver();
    }

    @Override
    public boolean isNull(int position) {
        return ordinals.isNull(position);
    }

    @Override
    public boolean mayHaveNulls() {
        return ordinals.mayHaveNulls();
    }

    @Override
    public boolean areAllValuesNull() {
        return ordinals.areAllValuesNull();
    }

    @Override
    public boolean mayHaveMultivaluedFields() {
        return ordinals.mayHaveMultivaluedFields();
    }

    @Override
    public boolean doesHaveMultivaluedFields() {
        return ordinals.mayHaveMultivaluedFields();
    }

    @Override
    public MvOrdering mvOrdering() {
        return ordinals.mvOrdering();
    }

    @Override
    public OrdinalBytesRefBlock expand() {
        OrdinalBytesRefBlock result = null;
        IntBlock expandedOrdinals = ordinals.expand();
        try {
            result = new OrdinalBytesRefBlock(expandedOrdinals, bytes);
            bytes.incRef();
        } finally {
            if (result == null) {
                expandedOrdinals.close();
            }
        }
        return result;
    }

    @Override
    public long ramBytesUsed() {
        return ordinals.ramBytesUsed() + bytes.ramBytesUsed();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof BytesRefBlock b) {
            return BytesRefBlock.equals(this, b);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return BytesRefBlock.hash(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[ordinals=" + ordinals + ", bytes=" + bytes + "]";
    }
}
