/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;

public final class BytesRefVectorBlock extends AbstractVectorBlock implements BytesRefBlock {

    private final BytesRefVector vector;

    BytesRefVectorBlock(BytesRefVector vector) {
        super(vector.getPositionCount());
        this.vector = vector;
    }

    @Override
    public BytesRefVector asVector() {
        return vector;
    }

    @Override
    public BytesRef getBytesRef(int valueIndex, BytesRef dest) {  // this is diff, share
        return vector.getBytesRef(valueIndex, dest);
    }

    @Override
    public Object getObject(int position) {
        return getBytesRef(position, new BytesRef());
    }

    @Override
    public int getTotalValueCount() {
        return vector.getPositionCount();
    }

    @Override
    public ElementType elementType() {
        return vector.elementType();
    }

    @Override
    public BytesRefBlock getRow(int position) {
        return filter(position);
    }

    @Override
    public BytesRefBlock filter(int... positions) {
        return new FilterBytesRefVector(vector, positions).asBlock();
    }
}
