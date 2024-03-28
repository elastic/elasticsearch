/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Releasables;

final class OrdinalBytesRefBlock extends AbstractNonThreadSafeRefCounted implements BytesRefBlock {
    private final IntBlock ordinals;
    private final BytesRefVector bytes;

    OrdinalBytesRefBlock(IntBlock ordinals, BytesRefVector bytes) {
        this.ordinals = ordinals;
        this.bytes = bytes;
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
        IntBlock filteredOrdinals = ordinals.filter(positions);
        OrdinalBytesRefBlock result = null;
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
        IntBlock expandedOrdinals = ordinals.expand();
        OrdinalBytesRefBlock result = null;
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
