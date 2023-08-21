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
        return block.getBytesRef(valueIndex, dest);
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
    public BytesRefBlock expand() {
        if (false == block.mayHaveMultivaluedFields()) {
            return this;
        }
        /*
         * Build a copy of the target block, selecting only the positions
         * we've been assigned and expanding all multivalued fields
         * into single valued fields.
         */
        BytesRefBlock.Builder builder = BytesRefBlock.newBlockBuilder(positions.length);
        BytesRef scratch = new BytesRef();
        for (int p : positions) {
            if (block.isNull(p)) {
                builder.appendNull();
                continue;
            }
            int start = block.getFirstValueIndex(p);
            int end = start + block.getValueCount(p);
            for (int i = start; i < end; i++) {
                BytesRef v = block.getBytesRef(i, scratch);
                builder.appendBytesRef(v);
            }
        }
        return builder.build();
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
        final int positions = getPositionCount();
        for (int p = 0; p < positions; p++) {
            if (p > 0) {
                sb.append(", ");
            }
            int start = getFirstValueIndex(p);
            int count = getValueCount(p);
            if (count == 1) {
                sb.append(getBytesRef(start, new BytesRef()));
                continue;
            }
            sb.append('[');
            int end = start + count;
            for (int i = start; i < end; i++) {
                if (i > start) {
                    sb.append(", ");
                }
                sb.append(getBytesRef(i, new BytesRef()));
            }
            sb.append(']');
        }
    }
}
