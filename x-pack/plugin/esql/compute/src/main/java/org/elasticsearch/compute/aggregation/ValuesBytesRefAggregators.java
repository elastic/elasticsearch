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
        BytesRefVector dict = valuesOrdinal.getDictionaryVector();
        final IntVector hashIds;
        BytesRef spare = new BytesRef();
        try (var hashIdsBuilder = values.blockFactory().newIntVectorFixedBuilder(dict.getPositionCount())) {
            for (int p = 0; p < dict.getPositionCount(); p++) {
                hashIdsBuilder.appendInt(Math.toIntExact(BlockHash.hashOrdToGroup(state.bytes.add(dict.getBytesRef(p, spare)))));
            }
            hashIds = hashIdsBuilder.build();
        }
        IntBlock ordinalIds = valuesOrdinal.getOrdinalsBlock();
        return new GroupingAggregatorFunction.AddInput() {
            @Override
            public void add(int positionOffset, IntBlock groupIds) {
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
                            state.values.add(groupId, hashIds.getInt(ordinalIds.getInt(v)));
                        }
                    }
                }
            }

            @Override
            public void add(int positionOffset, IntVector groupIds) {
                for (int groupPosition = 0; groupPosition < groupIds.getPositionCount(); groupPosition++) {
                    int groupId = groupIds.getInt(groupPosition);
                    if (ordinalIds.isNull(groupPosition + positionOffset)) {
                        continue;
                    }
                    int valuesStart = ordinalIds.getFirstValueIndex(groupPosition + positionOffset);
                    int valuesEnd = valuesStart + ordinalIds.getValueCount(groupPosition + positionOffset);
                    for (int v = valuesStart; v < valuesEnd; v++) {
                        state.values.add(groupId, hashIds.getInt(ordinalIds.getInt(v)));
                    }
                }
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
        BytesRefVector dict = valuesOrdinal.getDictionaryVector();
        final IntVector hashIds;
        BytesRef spare = new BytesRef();
        try (var hashIdsBuilder = values.blockFactory().newIntVectorFixedBuilder(dict.getPositionCount())) {
            for (int p = 0; p < dict.getPositionCount(); p++) {
                hashIdsBuilder.appendInt(Math.toIntExact(BlockHash.hashOrdToGroup(state.bytes.add(dict.getBytesRef(p, spare)))));
            }
            hashIds = hashIdsBuilder.build();
        }
        var ordinalIds = valuesOrdinal.getOrdinalsVector();
        return new GroupingAggregatorFunction.AddInput() {
            @Override
            public void add(int positionOffset, IntBlock groupIds) {
                for (int groupPosition = 0; groupPosition < groupIds.getPositionCount(); groupPosition++) {
                    if (groupIds.isNull(groupPosition)) {
                        continue;
                    }
                    int groupStart = groupIds.getFirstValueIndex(groupPosition);
                    int groupEnd = groupStart + groupIds.getValueCount(groupPosition);
                    for (int g = groupStart; g < groupEnd; g++) {
                        int groupId = groupIds.getInt(g);
                        state.values.add(groupId, hashIds.getInt(ordinalIds.getInt(groupPosition + positionOffset)));
                    }
                }
            }

            @Override
            public void add(int positionOffset, IntVector groupIds) {
                for (int groupPosition = 0; groupPosition < groupIds.getPositionCount(); groupPosition++) {
                    int groupId = groupIds.getInt(groupPosition);
                    state.values.add(groupId, hashIds.getInt(ordinalIds.getInt(groupPosition + positionOffset)));
                }
            }

            @Override
            public void close() {
                Releasables.close(hashIds, delegate);
            }
        };
    }
}
