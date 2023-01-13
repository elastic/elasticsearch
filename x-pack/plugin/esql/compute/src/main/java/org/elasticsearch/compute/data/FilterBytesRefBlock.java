/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;

final class FilterBytesRefBlock extends AbstractFilterBlock implements BytesRefBlock {

    private final BytesRefBlock bytesRefBlock;

    FilterBytesRefBlock(BytesRefBlock block, int... positions) {
        super(block, positions);
        this.bytesRefBlock = block;
    }

    @Override
    public BytesRefVector asVector() {
        return null;
    }

    @Override
    public BytesRef getBytesRef(int valueIndex, BytesRef spare) {
        return bytesRefBlock.getBytesRef(mapPosition(valueIndex), spare);
    }

    @Override
    public Object getObject(int position) {
        return getBytesRef(position, new BytesRef());
    }

    @Override
    public ElementType elementType() {
        return ElementType.BYTES_REF;
    }

    @Override
    public BytesRefBlock getRow(int position) {
        return filter(position);
    }

    @Override
    public BytesRefBlock filter(int... positions) {
        return new FilterBytesRefBlock(this, positions);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[block=" + bytesRefBlock + "]";
    }
}
