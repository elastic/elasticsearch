/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;

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
        final BytesRefVector dict = valuesOrdinal.getDictionaryVector();
        final BlockFactory blockFactory = dict.blockFactory();
        final long acquiredBytes = (long) Integer.BYTES * dict.getPositionCount();
        blockFactory.breaker().addEstimateBytesAndMaybeBreak(acquiredBytes, "ValuesBytesRefAggregator");
        boolean success = false;
        try {
            // Ord-to-hashId cache. Entries store {@code hashId + 1}, so the zero-initialized array
            // doubles as the "unseen" sentinel. When phantoms are possible (needsCompaction()==true)
            // we leave the array zeroed and let lazyHashId fill entries on first reference, so
            // phantom dict entries never enter state.bytes. When no phantoms are possible (the dense
            // Parquet path) we pre-hash the whole dictionary up front, so the per-row zero-check in
            // lazyHashId is always predicted not-taken — restoring the old eager fast path for
            // clickbench-style dense aggregations.
            final int[] hashIds = valuesOrdinal.needsCompaction() ? newLazyHashIds(dict.getPositionCount()) : eagerHashIds(state, dict);
            final BytesRef scratch = new BytesRef();
            final IntBlock ordinalIds = valuesOrdinal.getOrdinalsBlock();
            GroupingAggregatorFunction.AddInput result = new GroupingAggregatorFunction.AddInput() {
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
                    blockFactory.breaker().addWithoutBreaking(-acquiredBytes);
                    delegate.close();
                }
            };
            success = true;
            return result;
        } finally {
            if (success == false) {
                blockFactory.breaker().addWithoutBreaking(-acquiredBytes);
            }
        }
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
        final BlockFactory blockFactory = dict.blockFactory();
        final long acquiredBytes = (long) Integer.BYTES * dict.getPositionCount();
        blockFactory.breaker().addEstimateBytesAndMaybeBreak(acquiredBytes, "ValuesBytesRefAggregator");
        boolean success = false;
        try {
            // See the BytesRefBlock overload for the rationale behind this conditional.
            final int[] hashIds = valuesOrdinal.needsCompaction() ? newLazyHashIds(dict.getPositionCount()) : eagerHashIds(state, dict);
            final BytesRef scratch = new BytesRef();
            final IntVector ordinalIds = valuesOrdinal.getOrdinalsVector();
            GroupingAggregatorFunction.AddInput result = new GroupingAggregatorFunction.AddInput() {
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
                    blockFactory.breaker().addWithoutBreaking(-acquiredBytes);
                    delegate.close();
                }
            };
            success = true;
            return result;
        } finally {
            if (success == false) {
                blockFactory.breaker().addWithoutBreaking(-acquiredBytes);
            }
        }
    }

    /**
     * Returns a per-page lazy ord-to-hashId cache, sized by the dictionary. Entries are populated by
     * {@link #lazyHashId} on first reference, so phantom dictionary entries that no row touches never
     * enter {@code state.bytes}. The cache stores {@code hashId + 1}; the JVM's zero-init makes
     * {@code 0} the implicit "unseen" sentinel without an explicit fill.
     */
    static int[] newLazyHashIds(int dictSize) {
        return new int[dictSize];
    }

    /**
     * Eager counterpart used when the input block guarantees no phantom dictionary entries: every
     * dict entry is hashed up front (storing {@code hashId + 1}), so subsequent {@link #lazyHashId}
     * calls hit on the first cache load and the {@code v == 0} branch becomes a predicted-not-taken
     * no-op.
     */
    static int[] eagerHashIds(ValuesBytesRefAggregator.GroupingState state, BytesRefVector dict) {
        int[] cache = new int[dict.getPositionCount()];
        BytesRef scratch = new BytesRef();
        for (int p = 0; p < dict.getPositionCount(); p++) {
            cache[p] = Math.toIntExact(BlockHash.hashOrdToGroup(state.bytes.add(dict.getBytesRef(p, scratch)))) + 1;
        }
        return cache;
    }

    static int lazyHashId(int ord, BytesRefVector dict, int[] cache, BytesRef scratch, ValuesBytesRefAggregator.GroupingState state) {
        int v = cache[ord];
        if (v == 0) {
            v = Math.toIntExact(BlockHash.hashOrdToGroup(state.bytes.add(dict.getBytesRef(ord, scratch)))) + 1;
            cache[ord] = v;
        }
        return v - 1;
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
        boolean needsCompaction = false;
        {
            final OrdinalBytesRefBlock asOrdinals = values.asOrdinals();
            if (asOrdinals != null) {
                dict = asOrdinals.getDictionaryVector();
                ordinals = asOrdinals.getOrdinalsBlock();
                needsCompaction = asOrdinals.needsCompaction();
            }
        }
        if (dict != null && dict.getPositionCount() < groupIds.getPositionCount()) {
            final BlockFactory blockFactory = dict.blockFactory();
            final long acquiredBytes = (long) Integer.BYTES * dict.getPositionCount();
            blockFactory.breaker().addEstimateBytesAndMaybeBreak(acquiredBytes, "ValuesBytesRefAggregator");
            try {
                // See wrapAddInput for the rationale behind the eager/lazy split.
                final int[] hashIds = needsCompaction ? newLazyHashIds(dict.getPositionCount()) : eagerHashIds(state, dict);
                final BytesRef scratch = new BytesRef();
                IntVector ordinalsVector = ordinals.asVector();
                if (ordinalsVector != null) {
                    addOrdinalInputVector(state, positionOffset, groupIds, ordinalsVector, dict, hashIds, scratch);
                } else {
                    addOrdinalInputBlock(state, positionOffset, groupIds, ordinals, dict, hashIds, scratch);
                }
            } finally {
                blockFactory.breaker().addWithoutBreaking(-acquiredBytes);
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
