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
import org.elasticsearch.core.Releasables;

final class ValuesBytesRefAggregators {
    static GroupingAggregatorFunction.AddInput wrapAddInput(
        GroupingAggregatorFunction.AddInput delegate,
        ValuesBytesRefAggregator.GroupingState state,
        BytesRefBlock values
    ) {
        OrdinalBytesRefBlock valuesOrdinal = values.asOrdinals();
        if (valuesOrdinal == null) {
            return delegate;
        }
        final IntVector hashIds = hashDict(state, valuesOrdinal.getDictionaryVector());
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
                            state.addValueOrdinal(groupId, hashIds.getInt(ordinalIds.getInt(v)));
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
                            state.addValueOrdinal(groupId, hashIds.getInt(ordinalIds.getInt(v)));
                        }
                    }
                }
            }

            @Override
            public void add(int positionOffset, IntVector groupIds) {
                addOrdinalInputBlock(state, positionOffset, groupIds, ordinalIds, hashIds);
            }

            @Override
            public void close() {
                Releasables.close(hashIds, delegate);
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
        final IntVector hashIds = hashDict(state, valuesOrdinal.getDictionaryVector());
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
                        state.addValueOrdinal(groupId, hashIds.getInt(ordinalIds.getInt(groupPosition + positionOffset)));
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
                        state.addValueOrdinal(groupId, hashIds.getInt(ordinalIds.getInt(groupPosition + positionOffset)));
                    }
                }
            }

            @Override
            public void add(int positionOffset, IntVector groupIds) {
                addOrdinalInputVector(state, positionOffset, groupIds, ordinalIds, hashIds);
            }

            @Override
            public void close() {
                Releasables.close(hashIds, delegate);
            }
        };
    }

    static IntVector hashDict(ValuesBytesRefAggregator.GroupingState state, BytesRefVector dict) {
        BytesRef scratch = new BytesRef();
        try (var hashIdsBuilder = dict.blockFactory().newIntVectorFixedBuilder(dict.getPositionCount())) {
            for (int p = 0; p < dict.getPositionCount(); p++) {
                final long hashId = BlockHash.hashOrdToGroup(state.bytes.add(dict.getBytesRef(p, scratch)));
                hashIdsBuilder.appendInt(Math.toIntExact(hashId));
            }
            return hashIdsBuilder.build();
        }
    }

    static void addOrdinalInputBlock(
        ValuesBytesRefAggregator.GroupingState state,
        int positionOffset,
        IntVector groupIds,
        IntBlock ordinalIds,
        IntVector hashIds
    ) {
        for (int p = 0; p < groupIds.getPositionCount(); p++) {
            final int groupId = groupIds.getInt(p);
            final int valuePosition = p + positionOffset;
            final int start = ordinalIds.getFirstValueIndex(valuePosition);
            final int end = start + ordinalIds.getValueCount(valuePosition);
            for (int i = start; i < end; i++) {
                int ord = ordinalIds.getInt(i);
                state.addValueOrdinal(groupId, hashIds.getInt(ord));
            }
        }
    }

    static void addOrdinalInputVector(
        ValuesBytesRefAggregator.GroupingState state,
        int positionOffset,
        IntVector groupIds,
        IntVector ordinalIds,
        IntVector hashIds
    ) {
        if (groupIds.isConstant() && hashIds.isConstant()) {
            state.addValueOrdinal(groupIds.getInt(0), hashIds.getInt(0));
            return;
        }
        int lastGroup = groupIds.getInt(0);
        int lastOrd = ordinalIds.getInt(positionOffset);
        state.addValueOrdinal(lastGroup, hashIds.getInt(lastOrd));
        for (int p = 1; p < groupIds.getPositionCount(); p++) {
            final int nextGroup = groupIds.getInt(p);
            final int nextOrd = ordinalIds.getInt(p + positionOffset);
            if (nextGroup != lastGroup || nextOrd != lastOrd) {
                lastGroup = nextGroup;
                lastOrd = nextOrd;
                state.addValueOrdinal(lastGroup, hashIds.getInt(lastOrd));
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
            try (var hashIds = hashDict(state, dict)) {
                IntVector ordinalsVector = ordinals.asVector();
                if (ordinalsVector != null) {
                    addOrdinalInputVector(state, positionOffset, groupIds, ordinalsVector, hashIds);
                } else {
                    addOrdinalInputBlock(state, positionOffset, groupIds, ordinals, hashIds);
                }
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
