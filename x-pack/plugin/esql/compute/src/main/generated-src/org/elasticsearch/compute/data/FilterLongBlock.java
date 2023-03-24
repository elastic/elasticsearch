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
        return block.getLong(valueIndex);
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
        final int positions = getPositionCount();
        for (int p = 0; p < positions; p++) {
            if (p > 0) {
                sb.append(", ");
            }
            int start = getFirstValueIndex(p);
            int count = getValueCount(p);
            if (count == 1) {
                sb.append(getLong(start));
                continue;
            }
            sb.append('[');
            int end = start + count;
            for (int i = start; i < end; i++) {
                if (i > start) {
                    sb.append(", ");
                }
                sb.append(getLong(i));
            }
            sb.append(']');
        }
    }
}
