/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.ReleasableIterator;

import java.io.IOException;

/**
 * This vector is never instantiated. This class serves as a type holder for {@link ConstantNullBlock#asVector()}.
 */
public final class ConstantNullVector extends AbstractVector
    implements
        BooleanVector,
        BytesRefVector,
        DoubleVector,
        FloatVector,
        IntVector,
        LongVector {

    private ConstantNullVector(int positionCount, BlockFactory blockFactory) {
        super(positionCount, blockFactory);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        assert false : "null vector";
        throw new UnsupportedOperationException("null vector");
    }

    @Override
    public ConstantNullBlock asBlock() {
        assert false : "null vector";
        throw new UnsupportedOperationException("null vector");
    }

    @Override
    public OrdinalBytesRefVector asOrdinals() {
        assert false : "null vector";
        throw new UnsupportedOperationException("null vector");
    }

    @Override
    public ConstantNullVector filter(int... positions) {
        assert false : "null vector";
        throw new UnsupportedOperationException("null vector");
    }

    @Override
    public ConstantNullBlock keepMask(BooleanVector mask) {
        assert false : "null vector";
        throw new UnsupportedOperationException("null vector");
    }

    @Override
    public ReleasableIterator<ConstantNullBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        assert false : "null vector";
        throw new UnsupportedOperationException("null vector");
    }

    @Override
    public boolean getBoolean(int position) {
        assert false : "null vector";
        throw new UnsupportedOperationException("null vector");
    }

    @Override
    public BytesRef getBytesRef(int position, BytesRef dest) {
        assert false : "null vector";
        throw new UnsupportedOperationException("null vector");
    }

    @Override
    public float getFloat(int position) {
        assert false : "null vector";
        throw new UnsupportedOperationException("null vector");
    }

    @Override
    public double getDouble(int position) {
        assert false : "null vector";
        throw new UnsupportedOperationException("null vector");
    }

    @Override
    public int getInt(int position) {
        assert false : "null vector";
        throw new UnsupportedOperationException("null vector");
    }

    @Override
    public long getLong(int position) {
        assert false : "null vector";
        throw new UnsupportedOperationException("null vector");
    }

    @Override
    public int min() {
        assert false : "null vector";
        throw new UnsupportedOperationException("null vector");
    }

    @Override
    public int max() {
        assert false : "null vector";
        throw new UnsupportedOperationException("null vector");
    }

    @Override
    public boolean allTrue() {
        assert false : "null vector";
        throw new UnsupportedOperationException("null vector");
    }

    @Override
    public boolean allFalse() {
        assert false : "null vector";
        throw new UnsupportedOperationException("null vector");
    }

    @Override
    public ElementType elementType() {
        return ElementType.NULL;
    }

    @Override
    public boolean isConstant() {
        return true;
    }

    @Override
    public long ramBytesUsed() {
        return 0;
    }
}
