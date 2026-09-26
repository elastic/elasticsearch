/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.compute.aggregation.blockhash;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.PackDimBlock;
import org.elasticsearch.compute.data.PackDimValue;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.topn.TopNEncoder;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.List;

/**
 * Interns packed record content into operator-owned IDs, then delegates sample grouping to existing primitive hashes.
 * Local input dictionary IDs are only a per-page cache key. Exact framed bytes resolve hash collisions, and getKeys
 * reconstructs owned packed values rather than leaking IDs across an exchange. Dimension multivalues stay inside one key.
 */
final class PackDimBlockHash extends BlockHash {
    private final List<GroupSpec> groups;
    private final BytesRefHash[] records;
    private final BlockHash delegate;
    private final BreakingBytesRefBuilder key;

    PackDimBlockHash(List<GroupSpec> groups, BlockFactory factory, int emitBatchSize, boolean forcePacked) {
        super(factory);
        this.groups = List.copyOf(groups);
        records = new BytesRefHash[groups.size()];
        BlockHash created = null;
        BreakingBytesRefBuilder scratch = null;
        boolean success = false;
        try {
            var mapped = new ArrayList<GroupSpec>(groups.size());
            for (int i = 0; i < groups.size(); i++) {
                var group = groups.get(i);
                if (group.elementType() == ElementType.PACK_DIM) {
                    if (group.topNDef() != null) throw new IllegalArgumentException("packed dimensions do not have a sort order");
                    records[i] = new BytesRefHash(1, factory.bigArrays());
                    mapped.add(new GroupSpec(i, ElementType.INT));
                } else {
                    mapped.add(new GroupSpec(i, group.elementType(), group.categorizeDef(), group.topNDef()));
                }
            }
            created = forcePacked
                ? BlockHash.buildPackedValuesBlockHash(mapped, factory, emitBatchSize)
                : BlockHash.build(mapped, factory, emitBatchSize, false);
            scratch = new BreakingBytesRefBuilder(factory.breaker(), "packed dimension group key");
            delegate = created;
            key = scratch;
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(records);
                Releasables.close(created, scratch);
            }
        }
    }

    private Page map(Page input, boolean insert) {
        Block[] mapped = new Block[groups.size()];
        boolean success = false;
        try {
            for (int g = 0; g < groups.size(); g++) {
                Block block = input.getBlock(groups.get(g).channel());
                if (records[g] == null) {
                    block.incRef();
                    mapped[g] = block;
                } else {
                    mapped[g] = identities((PackDimBlock) block, records[g], insert);
                }
            }
            Page result = new Page(input.getPositionCount(), mapped);
            success = true;
            return result;
        } finally {
            if (success == false) Releasables.close(mapped);
        }
    }

    private IntBlock identities(PackDimBlock input, BytesRefHash dictionary, boolean insert) {
        var ordinal = input.asOrdinalPackDim();
        int size = ordinal == null ? input.getPositionCount() : ordinal.getDictionarySize();
        try (
            IntArray cache = blockFactory.bigArrays().newIntArray(size, true);
            var output = blockFactory.newIntBlockBuilder(input.getPositionCount())
        ) {
            PackDimValue value = new PackDimValue();
            BytesRef scratch = new BytesRef();
            for (int p = 0; p < input.getPositionCount(); p++) {
                if (input.isNull(p)) {
                    output.appendNull();
                    continue;
                }
                int local = ordinal == null ? p : ordinal.getOrdinalsBlock().getInt(ordinal.getOrdinalsBlock().getFirstValueIndex(p));
                int cached = cache.get(local);
                if (cached == 0) {
                    input.getPackDim(input.getFirstValueIndex(p), value);
                    key.clear();
                    TopNEncoder.DEFAULT_UNSORTABLE.encodeVInt(value.size(), key);
                    for (int f = 0; f < value.size(); f++) {
                        TopNEncoder.DEFAULT_UNSORTABLE.encodeBytesRef(value.nameAt(f, scratch), key);
                        TopNEncoder.DEFAULT_UNSORTABLE.encodeBytesRef(value.valueAt(f, scratch), key);
                    }
                    long id = insert ? hashOrdToGroup(dictionary.add(key.bytesRefView())) : dictionary.find(key.bytesRefView());
                    // Zero means unevaluated, -1 means an absent lookup key, positive values are ID + 1.
                    cached = id < 0 ? -1 : Math.toIntExact(id + 1);
                    cache.set(local, cached);
                }
                output.appendInt(cached < 0 ? -1 : cached - 1);
            }
            return output.build();
        }
    }

    @Override
    public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
        try (Page mapped = map(page, true)) {
            delegate.add(mapped, addInput);
        }
    }

    @Override
    public ReleasableIterator<IntBlock> lookup(Page page, ByteSizeValue targetBlockSize) {
        try (Page mapped = map(page, false)) {
            return delegate.lookup(mapped, targetBlockSize);
        }
    }

    @Override
    public Block[] getKeys(IntVector selected) {
        Block[] keys = delegate.getKeys(selected);
        boolean success = false;
        try {
            for (int g = 0; g < groups.size(); g++) {
                if (records[g] == null) continue;
                IntBlock ids = (IntBlock) keys[g];
                try (var output = blockFactory.newPackDimBlockBuilder(ids.getPositionCount())) {
                    BytesRef encoded = new BytesRef();
                    for (int p = 0; p < ids.getPositionCount(); p++) {
                        if (ids.isNull(p)) output.appendNull();
                        else {
                            records[g].get(ids.getInt(ids.getFirstValueIndex(p)), encoded);
                            int count = TopNEncoder.DEFAULT_UNSORTABLE.decodeVInt(encoded);
                            BytesRef[] names = new BytesRef[count];
                            BytesRef[] values = new BytesRef[count];
                            for (int f = 0; f < count; f++) {
                                names[f] = TopNEncoder.DEFAULT_UNSORTABLE.decodeBytesRef(encoded, new BytesRef());
                                values[f] = TopNEncoder.DEFAULT_UNSORTABLE.decodeBytesRef(encoded, new BytesRef());
                            }
                            output.append(names, values);
                        }
                    }
                    Block result = output.build();
                    keys[g] = result;
                    ids.close();
                }
            }
            success = true;
            return keys;
        } finally {
            if (success == false) Releasables.close(keys);
        }
    }

    @Override
    public IntVector nonEmpty() {
        return delegate.nonEmpty();
    }

    @Override
    public int numKeys() {
        return delegate.numKeys();
    }

    @Override
    public BitArray seenGroupIds(BigArrays bigArrays) {
        return delegate.seenGroupIds(bigArrays);
    }

    @Override
    public void close() {
        Releasables.close(delegate, key);
        Releasables.close(records);
    }
}
