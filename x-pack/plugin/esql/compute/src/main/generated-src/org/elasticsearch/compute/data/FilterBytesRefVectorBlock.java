/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;

import java.util.Arrays;

/**
 * Filter block for BytesRefBlocks.
 * This class is generated. Do not edit it.
 */
// TODO: check if javadoc needs updating, both here, in the interfaces and in the abstract filter block
final class FilterBytesRefVectorBlock extends AbstractFilterBlock implements BytesRefBlock {

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(FilterBytesRefBlock.class);
    private final BytesRefVectorBlock block;

    FilterBytesRefVectorBlock(BytesRefVectorBlock block, int... positions) {
        super(positions, block.blockFactory());
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
    public boolean isNull(int position) {
        return false;
    }

    @Override
    public boolean mayHaveNulls() {
        return false;
    }

    @Override
    public boolean areAllValuesNull() {
        return false;
    }

    @Override
    public boolean mayHaveMultivaluedFields() {
        return false;
    }

    @Override
    public final int getTotalValueCount() {
        return positions.length;
    }

    @Override
    public final int getValueCount(int position) {
        return 1;
    }

    @Override
    public final int getPositionCount() {
        return positions.length;
    }

    @Override
    public final int getFirstValueIndex(int position) {
        return mapPosition(position);
    }

    @Override
    public MvOrdering mvOrdering() {
        return MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING;
    }

    @Override
    public BytesRefBlock filter(int... positions) {
        int[] mappedPositions = Arrays.stream(positions).map(this::mapPosition).toArray();
        BytesRefBlock filtered = new FilterBytesRefVectorBlock(block, mappedPositions);
        block.incRef();
        return filtered;
    }

    @Override
    public BytesRefBlock expand() {
        if (false == block.mayHaveMultivaluedFields()) {
            return this;
        }
        // TODO: use the underlying block's expand
        /*
         * Build a copy of the target block, selecting only the positions
         * we've been assigned and expanding all multivalued fields
         * into single valued fields.
         */
        try (BytesRefBlock.Builder builder = new BytesRefBlockBuilder(positions.length * Integer.BYTES, blockFactory())) {
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
    }

    @Override
    public long ramBytesUsed() {
        // TODO: check this
        // from a usage and resource point of view filter blocks encapsulate
        // their inner block, rather than listing it as a child resource
        return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(block) + RamUsageEstimator.sizeOf(positions);
    }

    @Override
    public void allowPassingToDifferentDriver() {
        super.allowPassingToDifferentDriver();
        block.allowPassingToDifferentDriver();
    }

    @Override
    protected void closeInternal() {
        block.close();
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
        sb.append("[positions=" + getPositionCount());
        if (isReleased() == false) {
            sb.append(", values=[");
            appendValues(sb);
            sb.append("]");
        }
        sb.append("]");
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
