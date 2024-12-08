/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.BitSet;

abstract class AbstractArrayBlock extends AbstractNonThreadSafeRefCounted implements Block {
    private final MvOrdering mvOrdering;
    protected final int positionCount;

    @Nullable
    protected final int[] firstValueIndexes;

    @Nullable
    protected final BitSet nullsMask;

    /**
     * @param positionCount the number of values in this block
     */
    protected AbstractArrayBlock(int positionCount, @Nullable int[] firstValueIndexes, @Nullable BitSet nullsMask, MvOrdering mvOrdering) {
        this.positionCount = positionCount;
        this.firstValueIndexes = firstValueIndexes;
        this.mvOrdering = mvOrdering;
        this.nullsMask = nullsMask == null || nullsMask.isEmpty() ? null : nullsMask;
        assert nullsMask != null || firstValueIndexes != null : "Create VectorBlock instead";
        assert assertInvariants();
    }

    @Override
    public final boolean mayHaveMultivaluedFields() {
        /*
         * This could return a false positive if all the indices are one away from
         * each other. But we will try to avoid that.
         */
        return firstValueIndexes != null;
    }

    @Override
    public boolean doesHaveMultivaluedFields() {
        if (false == mayHaveMultivaluedFields()) {
            return false;
        }
        for (int p = 0; p < getPositionCount(); p++) {
            if (getValueCount(p) > 1) {
                return true;
            }
        }
        return false;
    }

    @Override
    public final MvOrdering mvOrdering() {
        return mvOrdering;
    }

    protected final BitSet shiftNullsToExpandedPositions() {
        BitSet expanded = new BitSet(nullsMask.size());
        int next = -1;
        while ((next = nullsMask.nextSetBit(next + 1)) != -1) {
            expanded.set(getFirstValueIndex(next));
        }
        return expanded;
    }

    private boolean assertInvariants() {
        if (firstValueIndexes != null) {
            assert firstValueIndexes.length >= getPositionCount() + 1 : firstValueIndexes.length + " < " + positionCount;
            for (int i = 0; i < getPositionCount(); i++) {
                assert firstValueIndexes[i + 1] > firstValueIndexes[i] : firstValueIndexes[i + 1] + " <= " + firstValueIndexes[i];
            }
        }
        if (nullsMask != null) {
            assert nullsMask.nextSetBit(getPositionCount() + 1) == -1;
        }
        if (firstValueIndexes != null && nullsMask != null) {
            for (int i = 0; i < getPositionCount(); i++) {
                // Either we have multi-values or a null but never both.
                assert ((nullsMask.get(i) == false) || (firstValueIndexes[i + 1] - firstValueIndexes[i]) == 1);
            }
        }
        return true;
    }

    @Override
    public final int getTotalValueCount() {
        if (firstValueIndexes == null) {
            return positionCount - nullValuesCount();
        }
        return firstValueIndexes[positionCount] - nullValuesCount();
    }

    @Override
    public final int getPositionCount() {
        return positionCount;
    }

    /** Gets the index of the first value for the given position. */
    public final int getFirstValueIndex(int position) {
        return firstValueIndexes == null ? position : firstValueIndexes[position];
    }

    /** Gets the number of values for the given position, possibly 0. */
    @Override
    public final int getValueCount(int position) {
        return isNull(position) ? 0 : firstValueIndexes == null ? 1 : firstValueIndexes[position + 1] - firstValueIndexes[position];
    }

    @Override
    public final boolean isNull(int position) {
        return mayHaveNulls() && nullsMask.get(position);
    }

    @Override
    public final boolean mayHaveNulls() {
        return nullsMask != null;
    }

    final int nullValuesCount() {
        return mayHaveNulls() ? nullsMask.cardinality() : 0;
    }

    @Override
    public final boolean areAllValuesNull() {
        return nullValuesCount() == getPositionCount();
    }

    static final class SubFields {
        long bytesReserved = 0;
        final int positionCount;
        final int[] firstValueIndexes;
        final BitSet nullsMask;
        final MvOrdering mvOrdering;

        SubFields(BlockFactory blockFactory, StreamInput in) throws IOException {
            this.positionCount = in.readVInt();
            boolean success = false;
            try {
                if (in.readBoolean()) {
                    bytesReserved += blockFactory.preAdjustBreakerForInt(positionCount + 1);
                    final int[] values = new int[positionCount + 1];
                    values[0] = in.readVInt();
                    for (int i = 1; i <= positionCount; i++) {
                        values[i] = values[i - 1] + in.readVInt();
                    }
                    this.firstValueIndexes = values;
                } else {
                    this.firstValueIndexes = null;
                }
                if (in.readBoolean()) {
                    bytesReserved += blockFactory.preAdjustBreakerForLong(positionCount / Long.BYTES);
                    nullsMask = BitSet.valueOf(in.readLongArray());
                } else {
                    nullsMask = null;
                }
                this.mvOrdering = in.readEnum(MvOrdering.class);
                success = true;
            } finally {
                if (success == false) {
                    blockFactory.adjustBreaker(-bytesReserved);
                }
            }
        }

        int vectorPositions() {
            return firstValueIndexes == null ? positionCount : firstValueIndexes[positionCount];
        }
    }

    void writeSubFields(StreamOutput out) throws IOException {
        out.writeVInt(positionCount);
        out.writeBoolean(firstValueIndexes != null);
        if (firstValueIndexes != null) {
            // firstValueIndexes are monotonic increasing
            out.writeVInt(firstValueIndexes[0]);
            for (int i = 1; i <= positionCount; i++) {
                out.writeVInt(firstValueIndexes[i] - firstValueIndexes[i - 1]);
            }
        }
        out.writeBoolean(nullsMask != null);
        if (nullsMask != null) {
            out.writeLongArray(nullsMask.toLongArray());
        }
        if (out.getTransportVersion().before(TransportVersions.V_8_15_0) && mvOrdering == MvOrdering.SORTED_ASCENDING) {
            out.writeEnum(MvOrdering.UNORDERED);
        } else {
            out.writeEnum(mvOrdering);
        }
    }
}
