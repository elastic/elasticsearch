/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Releasables;

final class OrdinalBytesRefVector extends AbstractNonThreadSafeRefCounted implements BytesRefVector {
    private final IntVector ordinals;
    private final BytesRefVector bytes;

    OrdinalBytesRefVector(IntVector ordinals, BytesRefVector bytes) {
        this.ordinals = ordinals;
        this.bytes = bytes;
    }

    @Override
    public int getPositionCount() {
        return ordinals.getPositionCount();
    }

    @Override
    public Vector getRow(int position) {
        return filter(position);
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
    public BytesRefBlock asBlock() {
        return new BytesRefVectorBlock(this);
    }

    @Override
    public BytesRefVector filter(int... positions) {
        IntVector filteredOrdinals = ordinals.filter(positions);
        OrdinalBytesRefVector result = null;
        try {
            result = new OrdinalBytesRefVector(filteredOrdinals, bytes);
            bytes.incRef();
        } finally {
            if (result == null) {
                filteredOrdinals.close();
            }
        }
        return result;
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
