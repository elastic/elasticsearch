/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;

/**
 * Filter block for BytesRefBlocks.
 * This class is generated. Do not edit it.
 */
final class FilterBytesRefBlock extends AbstractFilterBlock implements BytesRefBlock {

    private final BytesRefBlock block;

    FilterBytesRefBlock(BytesRefBlock block, int... positions) {
        super(block, positions);
        this.block = block;
    }

    @Override
    public BytesRefVector asVector() {
        return null;
    }

    @Override
    public BytesRef getBytesRef(int valueIndex, BytesRef dest) {
        return block.getBytesRef(mapPosition(valueIndex), dest);
    }

    @Override
    public ElementType elementType() {
        return ElementType.BYTES_REF;
    }

    @Override
    public BytesRefBlock filter(int... positions) {
        return new FilterBytesRefBlock(this, positions);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof BytesRefBlock that) {
            return BytesRefBlock.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return BytesRefBlock.hash(this);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName());
        sb.append("[positions=" + getPositionCount() + ", values=[");
        appendValues(sb);
        sb.append("]]");
        return sb.toString();
    }

    private void appendValues(StringBuilder sb) {
        final int positionsIndex = getPositionCount() - 1;
        for (int i = 0;; i++) {
            sb.append(getBytesRef(i, new BytesRef()));
            if (i == positionsIndex) {
                return;
            }
            sb.append(", ");
        }
    }
}
