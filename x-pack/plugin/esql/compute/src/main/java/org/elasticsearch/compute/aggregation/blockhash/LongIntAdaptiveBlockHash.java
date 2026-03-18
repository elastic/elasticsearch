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
import org.elasticsearch.common.util.BytesRefHashTable;
import org.elasticsearch.common.util.LongLongHash;
import org.elasticsearch.common.util.LongLongHashTable;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Objects;

/**
 * An {@link AdaptiveBlockHash} between {@link LongLongHash} and {@link PackedValuesBlockHash}.
 * If all inputs are vectors, it uses a {@link LongIntVectorOnlyBlockHash}, otherwise it migrates to a {@link PackedValuesBlockHash}.
 */
public final class LongIntAdaptiveBlockHash extends AdaptiveBlockHash {
    private final int longChannel;
    private final int intChannel;
    private final int emitBatchSize;
    private final boolean reverseOutput;

    public LongIntAdaptiveBlockHash(List<GroupSpec> specs, BlockFactory blockFactory, int emitBatchSize, boolean reverseOutput) {
        super(specs, blockFactory, emitBatchSize);
        this.longChannel = reverseOutput ? specs.get(1).channel() : specs.get(0).channel();
        this.intChannel = reverseOutput ? specs.get(0).channel() : specs.get(1).channel();
        this.emitBatchSize = emitBatchSize;
        this.reverseOutput = reverseOutput;
        this.current = new LongIntVectorOnlyBlockHash(blockFactory);
    }

    @Override
    protected void prepareAddInput(Page page) {
        if (current instanceof LongIntVectorOnlyBlockHash vectorHash) {
            if (longVector(page) == null || intVector(page) == null) {
                var packedHash = vectorHash.migrateToPackedHash();
                Releasables.close(current, () -> current = packedHash);
            }
        }
    }

    @Override
    protected void prepareForLookup(Page page) {
        prepareAddInput(page);
    }

    private LongVector longVector(Page page) {
        LongBlock longBlock = page.getBlock(longChannel);
        return longBlock.asVector();
    }

    private IntVector intVector(Page page) {
        IntBlock intBlock = page.getBlock(intChannel);
        return intBlock.asVector();
    }

    final class LongIntVectorOnlyBlockHash extends BlockHash {
        private final LongLongHashTable longLongHash;

        LongIntVectorOnlyBlockHash(BlockFactory blockFactory) {
            super(blockFactory);
            this.longLongHash = HashImplFactory.newLongLongHash(blockFactory);
        }

        @Override
        public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
            LongVector longVector = Objects.requireNonNull(longVector(page), "required long vector");
            IntVector intVector = Objects.requireNonNull(intVector(page), "required int vector");
            int position = longVector.getPositionCount();
            int offset = 0;

            while (offset < position) {
                final int batchSize = Math.min(emitBatchSize, position - offset);
                try (var groupIdsBuilder = blockFactory.newIntVectorFixedBuilder(batchSize)) {
                    for (int i = 0; i < batchSize; i++) {
                        long longKey = longVector.getLong(offset + i);
                        int intValue = intVector.getInt(offset + i);
                        long ord = hashOrdToGroup(longLongHash.add(longKey, intValue));
                        groupIdsBuilder.appendInt(i, Math.toIntExact(ord));
                    }
                    try (var groupIds = groupIdsBuilder.build()) {
                        addInput.add(offset, groupIds);
                    }
                }
                offset += batchSize;
            }
        }

        @Override
        public int numKeys() {
            return Math.toIntExact(longLongHash.size());
        }

        @Override
        public ReleasableIterator<IntBlock> lookup(Page page, ByteSizeValue targetBlockSize) {
            LongVector longVector = Objects.requireNonNull(longVector(page), "required long vector");
            IntVector intVector = Objects.requireNonNull(intVector(page), "required int vector");
            longVector.mustIncRef();
            intVector.mustIncRef();
            final long emitSize = targetBlockSize.getBytes() / (Integer.BYTES);
            return new ReleasableIterator<>() {
                private int offset = 0;
                final int positionCount = longVector.getPositionCount();

                @Override
                public boolean hasNext() {
                    return offset < positionCount;
                }

                @Override
                public IntBlock next() {
                    int batchSize = (int) Math.min(emitSize, positionCount - offset);
                    try (var groupIdsBuilder = blockFactory.newIntBlockBuilder(batchSize)) {
                        for (int i = 0; i < batchSize; i++) {
                            long longKey = longVector.getLong(offset + i);
                            int intKey = intVector.getInt(offset + i);
                            long ord = longLongHash.find(longKey, intKey);
                            if (ord < 0) {
                                groupIdsBuilder.appendNull();
                            } else {
                                groupIdsBuilder.appendInt(Math.toIntExact(ord));
                            }
                        }
                        offset += batchSize;
                        return groupIdsBuilder.build();
                    }
                }

                @Override
                public void close() {
                    Releasables.close(longVector::decRef, intVector::decRef);
                }
            };
        }

        static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.nativeOrder());
        static final VarHandle INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.nativeOrder());

        PackedValuesBlockHash migrateToPackedHash() {
            // TODO: allow specifying the initial size to avoid re-hashing
            final int entries = numKeys();
            boolean success = false;
            final PackedValuesBlockHash packed = new PackedValuesBlockHash(specs, blockFactory, emitBatchSize);
            final BytesRefHashTable packedHash = packed.bytesRefHash;
            // byte 0 is reserved for the null bits in the packed values hash
            final int intPosition = 1 + (reverseOutput ? 0 : Long.BYTES);
            final int longPosition = 1 + (reverseOutput ? Integer.BYTES : 0);
            try {
                BytesRef packedKey = new BytesRef(new byte[1 + Long.BYTES + Integer.BYTES]);
                for (int i = 0; i < entries; i++) {
                    long longKey = longLongHash.getKey1(i);
                    int intKey = (int) longLongHash.getKey2(i);
                    LONG_HANDLE.set(packedKey.bytes, longPosition, longKey);
                    INT_HANDLE.set(packedKey.bytes, intPosition, intKey);
                    long ord = packedHash.add(packedKey);
                    assert ord >= 0 : "duplicate keys found when migrating to packed hash";
                }
                success = true;
            } finally {
                if (success == false) {
                    Releasables.close(packed);
                }
            }
            return packed;
        }

        @Override
        public Block[] getKeys() {
            Block longKeys = null;
            Block intKeys = null;
            boolean success = false;
            int positionCount = numKeys();
            try (
                var longsBuilder = blockFactory.newLongVectorFixedBuilder(positionCount);
                var intsBuilder = blockFactory.newIntVectorFixedBuilder(positionCount)
            ) {
                for (int i = 0; i < positionCount; i++) {
                    long longKey = longLongHash.getKey1(i);
                    int intKey = (int) longLongHash.getKey2(i);
                    longsBuilder.appendLong(longKey);
                    intsBuilder.appendInt(intKey);
                }
                longKeys = longsBuilder.build().asBlock();
                intKeys = intsBuilder.build().asBlock();
                success = true;
            } finally {
                if (success == false) {
                    Releasables.close(longKeys, intKeys);
                }
            }
            if (reverseOutput) {
                return new Block[] { intKeys, longKeys };
            } else {
                return new Block[] { longKeys, intKeys };
            }
        }

        @Override
        public IntVector nonEmpty() {
            return blockFactory.newIntRangeVector(0, numKeys());
        }

        @Override
        public BitArray seenGroupIds(BigArrays bigArrays) {
            return new SeenGroupIds.Range(0, numKeys()).seenGroupIds(bigArrays);
        }

        @Override
        public void close() {
            Releasables.close(longLongHash);
        }

        @Override
        public String toString() {
            return "LongIntBlockHash{keys=[LongKey[channel="
                + longChannel
                + "], IntKey[channel="
                + intChannel
                + "]], entries="
                + longLongHash.size()
                + ", size="
                + longLongHash.ramBytesUsed()
                + "b}";
        }
    }
}
