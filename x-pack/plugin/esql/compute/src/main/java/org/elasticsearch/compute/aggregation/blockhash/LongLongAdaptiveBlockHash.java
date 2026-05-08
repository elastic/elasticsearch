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
 * If all inputs are vectors, it uses a {@link LongLongVectorOnlyBlockHash}, otherwise it migrates to a {@link PackedValuesBlockHash}.
 */
public final class LongLongAdaptiveBlockHash extends AdaptiveBlockHash {
    private final int channel1;
    private final int channel2;
    /**
     * Batch size for the vector-only path's bulk add arrays and for emitting
     * ords to aggs in the vector-only path. Must be at least 4096 so that
     * bulk-add operations can process full SIMD lanes.
     */
    private final int vectorBatchSize;

    public LongLongAdaptiveBlockHash(List<GroupSpec> specs, BlockFactory blockFactory, int emitBatchSize) {
        super(specs, blockFactory, emitBatchSize);
        this.channel1 = specs.get(0).channel();
        this.channel2 = specs.get(1).channel();
        this.vectorBatchSize = Math.max(emitBatchSize, 4096);
        this.current = new LongLongVectorOnlyBlockHash(blockFactory);
    }

    @Override
    protected void prepareAddInput(Page page) {
        if (current instanceof LongLongVectorOnlyBlockHash vectorHash) {
            if (vector1(page) == null || vector2(page) == null) {
                var packedHash = vectorHash.migrateToPackedHash();
                Releasables.close(current, () -> current = packedHash);
            }
        }
    }

    @Override
    protected void prepareForLookup(Page page) {
        prepareAddInput(page);
    }

    private LongVector vector1(Page page) {
        LongBlock block = page.getBlock(channel1);
        return block.asVector();
    }

    private LongVector vector2(Page page) {
        LongBlock block = page.getBlock(channel2);
        return block.asVector();
    }

    final class LongLongVectorOnlyBlockHash extends BlockHash {
        private final LongLongHashTable longLongHash;
        private final long batchUsedBytes;
        private final long[] batchKeys1;
        private final long[] batchKeys2;
        private final int[] batchIds;

        LongLongVectorOnlyBlockHash(BlockFactory blockFactory) {
            super(blockFactory);
            final long bytes = (Integer.BYTES + Long.BYTES * 2) * (long) vectorBatchSize;
            blockFactory.adjustBreaker(bytes);
            this.batchUsedBytes = bytes;
            boolean success = false;
            try {
                batchKeys1 = new long[vectorBatchSize];
                batchKeys2 = new long[vectorBatchSize];
                batchIds = new int[vectorBatchSize];
                this.longLongHash = HashImplFactory.newLongLongHash(blockFactory);
                success = true;
            } finally {
                if (success == false) {
                    blockFactory.adjustBreaker(-bytes);
                }
            }
        }

        @Override
        public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
            LongVector vector1 = Objects.requireNonNull(vector1(page), () -> "required long vector for channel " + channel1);
            LongVector vector2 = Objects.requireNonNull(vector2(page), () -> "required long vector for channel " + channel2);
            if (longLongHash.supportBulkAdd()) {
                addBatch(vector1, vector2, addInput);
            } else {
                addOneAtTime(vector1, vector2, addInput);
            }
        }

        private void addBatch(LongVector vector1, LongVector vector2, GroupingAggregatorFunction.AddInput addInput) {
            final int position = vector1.getPositionCount();
            int offset = 0;
            while (offset < position) {
                final int batchSize = Math.min(vectorBatchSize, position - offset);
                vector1.copyTo(offset, batchKeys1, 0, batchSize);
                vector2.copyTo(offset, batchKeys2, 0, batchSize);
                longLongHash.bulkAdd(batchKeys1, batchKeys2, batchIds, batchSize);
                try (var groupIds = blockFactory.newIntArrayVector(batchIds, batchSize)) {
                    addInput.add(offset, groupIds);
                }
                offset += batchSize;
            }
        }

        private void addOneAtTime(LongVector vector1, LongVector vector2, GroupingAggregatorFunction.AddInput addInput) {
            int position = vector1.getPositionCount();
            int offset = 0;

            while (offset < position) {
                final int batchSize = Math.min(vectorBatchSize, position - offset);
                try (var groupIdsBuilder = blockFactory.newIntVectorFixedBuilder(batchSize)) {
                    for (int i = 0; i < batchSize; i++) {
                        long key1 = vector1.getLong(offset + i);
                        long key2 = vector2.getLong(offset + i);
                        long ord = hashOrdToGroup(longLongHash.add(key1, key2));
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
            LongVector vector1 = Objects.requireNonNull(vector1(page), () -> "required long vector for channel " + channel1);
            LongVector vector2 = Objects.requireNonNull(vector2(page), () -> "required long vector for channel " + channel2);
            vector1.mustIncRef();
            vector2.mustIncRef();
            final long emitSize = targetBlockSize.getBytes() / (Integer.BYTES);
            return new ReleasableIterator<>() {
                private int offset = 0;
                final int positionCount = vector1.getPositionCount();

                @Override
                public boolean hasNext() {
                    return offset < positionCount;
                }

                @Override
                public IntBlock next() {
                    int batchSize = (int) Math.min(emitSize, positionCount - offset);
                    try (var groupIdsBuilder = blockFactory.newIntBlockBuilder(batchSize)) {
                        for (int i = 0; i < batchSize; i++) {
                            long key1 = vector1.getLong(offset + i);
                            long key2 = vector2.getLong(offset + i);
                            long ord = longLongHash.find(key1, key2);
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
                    Releasables.close(vector1::decRef, vector2::decRef);
                }
            };
        }

        static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.nativeOrder());

        PackedValuesBlockHash migrateToPackedHash() {
            // TODO: allow specifying the initial size to avoid re-hashing
            final int entries = numKeys();
            boolean success = false;
            final PackedValuesBlockHash packed = new PackedValuesBlockHash(specs, blockFactory, emitBatchSize);
            final BytesRefHashTable packedHash = packed.bytesRefHash;
            // byte 0 is reserved for the null bits in the packed values hash
            final int key1Position = 1;
            final int key2Position = 1 + Long.BYTES;
            try {
                BytesRef packedKey = new BytesRef(new byte[1 + Long.BYTES + Long.BYTES]);
                for (int i = 0; i < entries; i++) {
                    long key1 = longLongHash.getKey1(i);
                    long key2 = longLongHash.getKey2(i);
                    LONG_HANDLE.set(packedKey.bytes, key1Position, key1);
                    LONG_HANDLE.set(packedKey.bytes, key2Position, key2);
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
        public Block[] getKeys(IntVector selected) {
            Block keys1 = null;
            Block keys2 = null;
            boolean success = false;
            int positions = selected.getPositionCount();
            try (
                var keys1Builder = blockFactory.newLongVectorFixedBuilder(positions);
                var keys2Builder = blockFactory.newLongVectorFixedBuilder(positions)
            ) {
                for (int i = 0; i < positions; i++) {
                    int groupId = selected.getInt(i);
                    keys1Builder.appendLong(longLongHash.getKey1(groupId));
                    keys2Builder.appendLong(longLongHash.getKey2(groupId));
                }
                keys1 = keys1Builder.build().asBlock();
                keys2 = keys2Builder.build().asBlock();
                success = true;
            } finally {
                if (success == false) {
                    Releasables.close(keys1, keys2);
                }
            }
            return new Block[] { keys1, keys2 };
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
            blockFactory.adjustBreaker(-batchUsedBytes);
            Releasables.close(longLongHash);
        }

        @Override
        public String toString() {
            return "LongLongBlockHash{keys=[LongKey[channel="
                + channel1
                + "], LongKey[channel="
                + channel2
                + "]], entries="
                + longLongHash.size()
                + ", size="
                + longLongHash.ramBytesUsed()
                + "b}";
        }
    }
}
