/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

/**
 * Filter block for LongBlocks.
 * This class is generated. Do not edit it.
 */
final class FilterLongBlock extends AbstractFilterBlock implements LongBlock {

    private final LongBlock block;

    FilterLongBlock(LongBlock block, int... positions) {
        super(block, positions);
        this.block = block;
    }

    @Override
    public LongVector asVector() {
        return null;
    }

    @Override
    public long getLong(int valueIndex) {
        return block.getLong(mapPosition(valueIndex));
    }

    @Override
    public ElementType elementType() {
        return ElementType.LONG;
    }

    @Override
    public LongBlock filter(int... positions) {
        return new FilterLongBlock(this, positions);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof LongBlock that) {
            return LongBlock.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return LongBlock.hash(this);
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
            sb.append(getLong(i));
            if (i == positionsIndex) {
                return;
            }
            sb.append(", ");
        }
    }
}
