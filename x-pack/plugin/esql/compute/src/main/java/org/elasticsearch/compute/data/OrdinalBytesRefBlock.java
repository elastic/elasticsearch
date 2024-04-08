/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Releasables;

import java.io.IOException;

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
        return ordinals.getTotalValueCount() * 2 / 3 >= bytes.getPositionCount();
    }

    @Override
    public BytesRef getBytesRef(int valueIndex, BytesRef dest) {
        return bytes.getBytesRef(ordinals.getInt(valueIndex), dest);
    }

    @Override
    public BytesRefVector asVector() {
        IntVector vector = ordinals.asVector();
        if (vector != null) {
            return new OrdinalBytesRefVector(vector, bytes);
        } else {
            return null;
        }
    }

    @Override
    public BytesRefBlock filter(int... positions) {
        if (positions.length * ordinals.getTotalValueCount() >= bytes.getPositionCount() * ordinals.getPositionCount()) {
            OrdinalBytesRefBlock result = null;
            IntBlock filteredOrdinals = ordinals.filter(positions);
            try {
                result = new OrdinalBytesRefBlock(filteredOrdinals, bytes);
                bytes.incRef();
            } finally {
                if (result == null) {
                    filteredOrdinals.close();
                }
            }
            return result;
        } else {
            // TODO: merge this BytesRefArrayBlock#filter
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
                        builder.appendBytesRef(getBytesRef(getFirstValueIndex(pos), scratch));
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
    public int nullValuesCount() {
        return ordinals.nullValuesCount();
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
}
