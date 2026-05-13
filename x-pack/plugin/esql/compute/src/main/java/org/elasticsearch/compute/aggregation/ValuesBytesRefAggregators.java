/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;

import java.util.Arrays;

final class ValuesBytesRefAggregators {
    private static final int UNSEEN = -1;

    static GroupingAggregatorFunction.AddInput wrapAddInput(
        GroupingAggregatorFunction.AddInput delegate,
        ValuesBytesRefAggregator.GroupingState state,
        BytesRefBlock values
    ) {
        OrdinalBytesRefBlock valuesOrdinal = values.asOrdinals();
        if (valuesOrdinal == null) {
            return delegate;
        }
        final BytesRefVector dict = valuesOrdinal.getDictionaryVector();
        // Lazy ord-to-hashId cache. Phantom dictionary entries (left over from filter()/slice()/keepMask())
        // are never visited because no row references them, so they never enter state.bytes.
        final int[] hashIds = newLazyHashIds(dict.getPositionCount());
        final BytesRef scratch = new BytesRef();
        IntBlock ordinalIds = valuesOrdinal.getOrdinalsBlock();
        return new GroupingAggregatorFunction.AddInput() {
            @Override
            public void add(int positionOffset, IntArrayBlock groupIds) {
                for (int groupPosition = 0; groupPosition < groupIds.getPositionCount(); groupPosition++) {
                    if (groupIds.isNull(groupPosition)) {
                        continue;
                    }
                    int groupStart = groupIds.getFirstValueIndex(groupPosition);
                    int groupEnd = groupStart + groupIds.getValueCount(groupPosition);
                    for (int g = groupStart; g < groupEnd; g++) {
                        int groupId = groupIds.getInt(g);
                        if (ordinalIds.isNull(groupPosition + positionOffset)) {
                            continue;
                        }
                        int valuesStart = ordinalIds.getFirstValueIndex(groupPosition + positionOffset);
                        int valuesEnd = valuesStart + ordinalIds.getValueCount(groupPosition + positionOffset);
                        for (int v = valuesStart; v < valuesEnd; v++) {
                            state.addValueOrdinal(groupId, lazyHashId(ordinalIds.getInt(v), dict, hashIds, scratch, state));
                        }
                    }
                }
            }

            @Override
            public void add(int positionOffset, IntBigArrayBlock groupIds) {
                for (int groupPosition = 0; groupPosition < groupIds.getPositionCount(); groupPosition++) {
                    if (groupIds.isNull(groupPosition)) {
                        continue;
                    }
                    int groupStart = groupIds.getFirstValueIndex(groupPosition);
                    int groupEnd = groupStart + groupIds.getValueCount(groupPosition);
                    for (int g = groupStart; g < groupEnd; g++) {
                        int groupId = groupIds.getInt(g);
                        if (ordinalIds.isNull(groupPosition + positionOffset)) {
                            continue;
                        }
                        int valuesStart = ordinalIds.getFirstValueIndex(groupPosition + positionOffset);
                        int valuesEnd = valuesStart + ordinalIds.getValueCount(groupPosition + positionOffset);
                        for (int v = valuesStart; v < valuesEnd; v++) {
                            state.addValueOrdinal(groupId, lazyHashId(ordinalIds.getInt(v), dict, hashIds, scratch, state));
                        }
                    }
                }
            }

            @Override
            public void add(int positionOffset, IntVector groupIds) {
                addOrdinalInputBlock(state, positionOffset, groupIds, ordinalIds, dict, hashIds, scratch);
            }

            @Override
            public void close() {
                delegate.close();
            }
        };
    }

    static GroupingAggregatorFunction.AddInput wrapAddInput(
        GroupingAggregatorFunction.AddInput delegate,
        ValuesBytesRefAggregator.GroupingState state,
        BytesRefVector values
    ) {
        var valuesOrdinal = values.asOrdinals();
        if (valuesOrdinal == null) {
            return delegate;
        }
        final BytesRefVector dict = valuesOrdinal.getDictionaryVector();
        final int[] hashIds = newLazyHashIds(dict.getPositionCount());
        final BytesRef scratch = new BytesRef();
        var ordinalIds = valuesOrdinal.getOrdinalsVector();
        return new GroupingAggregatorFunction.AddInput() {
            @Override
            public void add(int positionOffset, IntArrayBlock groupIds) {
                for (int groupPosition = 0; groupPosition < groupIds.getPositionCount(); groupPosition++) {
                    if (groupIds.isNull(groupPosition)) {
                        continue;
                    }
                    int groupStart = groupIds.getFirstValueIndex(groupPosition);
                    int groupEnd = groupStart + groupIds.getValueCount(groupPosition);
                    for (int g = groupStart; g < groupEnd; g++) {
                        int groupId = groupIds.getInt(g);
                        int ord = ordinalIds.getInt(groupPosition + positionOffset);
                        state.addValueOrdinal(groupId, lazyHashId(ord, dict, hashIds, scratch, state));
                    }
                }
            }

            @Override
            public void add(int positionOffset, IntBigArrayBlock groupIds) {
                for (int groupPosition = 0; groupPosition < groupIds.getPositionCount(); groupPosition++) {
                    if (groupIds.isNull(groupPosition)) {
                        continue;
                    }
                    int groupStart = groupIds.getFirstValueIndex(groupPosition);
                    int groupEnd = groupStart + groupIds.getValueCount(groupPosition);
                    for (int g = groupStart; g < groupEnd; g++) {
                        int groupId = groupIds.getInt(g);
                        int ord = ordinalIds.getInt(groupPosition + positionOffset);
                        state.addValueOrdinal(groupId, lazyHashId(ord, dict, hashIds, scratch, state));
                    }
                }
            }

            @Override
            public void add(int positionOffset, IntVector groupIds) {
                addOrdinalInputVector(state, positionOffset, groupIds, ordinalIds, dict, hashIds, scratch);
            }

            @Override
            public void close() {
                delegate.close();
            }
        };
    }

    /**
     * Returns a per-page lazy ord-to-hashId cache, sized by the dictionary and pre-filled with
     * {@link #UNSEEN}. Entries are populated by {@link #lazyHashId} on first reference, so phantom
     * dictionary entries that no row touches never enter {@code state.bytes}.
     */
    static int[] newLazyHashIds(int dictSize) {
        int[] cache = new int[dictSize];
        Arrays.fill(cache, UNSEEN);
        return cache;
    }

    static int lazyHashId(int ord, BytesRefVector dict, int[] cache, BytesRef scratch, ValuesBytesRefAggregator.GroupingState state) {
        int v = cache[ord];
        if (v == UNSEEN) {
            v = Math.toIntExact(BlockHash.hashOrdToGroup(state.bytes.add(dict.getBytesRef(ord, scratch))));
            cache[ord] = v;
        }
        return v;
    }

    static void addOrdinalInputBlock(
        ValuesBytesRefAggregator.GroupingState state,
        int positionOffset,
        IntVector groupIds,
        IntBlock ordinalIds,
        BytesRefVector dict,
        int[] hashIds,
        BytesRef scratch
    ) {
        for (int p = 0; p < groupIds.getPositionCount(); p++) {
            final int groupId = groupIds.getInt(p);
            final int valuePosition = p + positionOffset;
            final int start = ordinalIds.getFirstValueIndex(valuePosition);
            final int end = start + ordinalIds.getValueCount(valuePosition);
            for (int i = start; i < end; i++) {
                int ord = ordinalIds.getInt(i);
                state.addValueOrdinal(groupId, lazyHashId(ord, dict, hashIds, scratch, state));
            }
        }
    }

    static void addOrdinalInputVector(
        ValuesBytesRefAggregator.GroupingState state,
        int positionOffset,
        IntVector groupIds,
        IntVector ordinalIds,
        BytesRefVector dict,
        int[] hashIds,
        BytesRef scratch
    ) {
        if (groupIds.isConstant() && ordinalIds.isConstant()) {
            int ord = ordinalIds.getInt(0);
            state.addValueOrdinal(groupIds.getInt(0), lazyHashId(ord, dict, hashIds, scratch, state));
            return;
        }
        int lastGroup = groupIds.getInt(0);
        int lastOrd = ordinalIds.getInt(positionOffset);
        state.addValueOrdinal(lastGroup, lazyHashId(lastOrd, dict, hashIds, scratch, state));
        for (int p = 1; p < groupIds.getPositionCount(); p++) {
            final int nextGroup = groupIds.getInt(p);
            final int nextOrd = ordinalIds.getInt(p + positionOffset);
            if (nextGroup != lastGroup || nextOrd != lastOrd) {
                lastGroup = nextGroup;
                lastOrd = nextOrd;
                state.addValueOrdinal(lastGroup, lazyHashId(lastOrd, dict, hashIds, scratch, state));
            }
        }
    }

    static void combineIntermediateInputValues(
        ValuesBytesRefAggregator.GroupingState state,
        int positionOffset,
        IntVector groupIds,
        BytesRefBlock values
    ) {
        BytesRefVector dict = null;
        IntBlock ordinals = null;
        {
            final OrdinalBytesRefBlock asOrdinals = values.asOrdinals();
            if (asOrdinals != null) {
                dict = asOrdinals.getDictionaryVector();
                ordinals = asOrdinals.getOrdinalsBlock();
            }
        }
        if (dict != null && dict.getPositionCount() < groupIds.getPositionCount()) {
            final int[] hashIds = newLazyHashIds(dict.getPositionCount());
            final BytesRef scratch = new BytesRef();
            IntVector ordinalsVector = ordinals.asVector();
            if (ordinalsVector != null) {
                addOrdinalInputVector(state, positionOffset, groupIds, ordinalsVector, dict, hashIds, scratch);
            } else {
                addOrdinalInputBlock(state, positionOffset, groupIds, ordinals, dict, hashIds, scratch);
            }
        } else {
            final BytesRef scratch = new BytesRef();
            for (int p = 0; p < groupIds.getPositionCount(); p++) {
                final int groupId = groupIds.getInt(p);
                final int valuePosition = p + positionOffset;
                final int start = values.getFirstValueIndex(valuePosition);
                final int end = start + values.getValueCount(valuePosition);
                for (int i = start; i < end; i++) {
                    state.addValue(groupId, values.getBytesRef(i, scratch));
                }
            }
        }
    }

    static void combineIntermediateInputValues(
        ValuesBytesRefAggregator.GroupingState state,
        int positionOffset,
        IntBlock groupIds,
        BytesRefBlock values
    ) {
        final BytesRef scratch = new BytesRef();
        for (int groupPosition = 0; groupPosition < groupIds.getPositionCount(); groupPosition++) {
            if (groupIds.isNull(groupPosition)) {
                continue;
            }
            int groupStart = groupIds.getFirstValueIndex(groupPosition);
            int groupEnd = groupStart + groupIds.getValueCount(groupPosition);
            for (int g = groupStart; g < groupEnd; g++) {
                if (values.isNull(groupPosition + positionOffset)) {
                    continue;
                }
                int groupId = groupIds.getInt(g);
                int valuesStart = values.getFirstValueIndex(groupPosition + positionOffset);
                int valuesEnd = valuesStart + values.getValueCount(groupPosition + positionOffset);
                for (int v = valuesStart; v < valuesEnd; v++) {
                    var bytes = values.getBytesRef(v, scratch);
                    state.addValue(groupId, bytes);
                }
            }
        }
    }
}
