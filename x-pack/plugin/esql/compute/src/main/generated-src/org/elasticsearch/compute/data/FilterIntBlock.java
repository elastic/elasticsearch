/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

/**
 * Filter block for IntBlocks.
 * This class is generated. Do not edit it.
 */
final class FilterIntBlock extends AbstractFilterBlock implements IntBlock {

    private final IntBlock block;

    FilterIntBlock(IntBlock block, int... positions) {
        super(block, positions);
        this.block = block;
    }

    @Override
    public IntVector asVector() {
        return null;
    }

    @Override
    public int getInt(int valueIndex) {
        return block.getInt(mapPosition(valueIndex));
    }

    @Override
    public ElementType elementType() {
        return ElementType.INT;
    }

    @Override
    public IntBlock filter(int... positions) {
        return new FilterIntBlock(this, positions);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof IntBlock that) {
            return IntBlock.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return IntBlock.hash(this);
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
            sb.append(getInt(i));
            if (i == positionsIndex) {
                return;
            }
            sb.append(", ");
        }
    }
}
