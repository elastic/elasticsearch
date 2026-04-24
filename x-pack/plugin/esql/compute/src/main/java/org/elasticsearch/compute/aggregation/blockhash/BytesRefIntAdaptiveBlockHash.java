/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.BytesRefHashTable;
import org.elasticsearch.common.util.LongHashTable;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Objects;

/**
 * An {@link AdaptiveBlockHash} for a pair of BytesRef and an integer
 */
public final class BytesRefIntAdaptiveBlockHash extends AdaptiveBlockHash {
    private final int bytesChannel;
    private final int intChannel;
    private final int emitBatchSize;
    private final boolean reverseOutput;

    public BytesRefIntAdaptiveBlockHash(List<GroupSpec> specs, BlockFactory blockFactory, int emitBatchSize, boolean reverseOutput) {
        super(specs, blockFactory, emitBatchSize);
        this.bytesChannel = reverseOutput ? specs.get(1).channel() : specs.get(0).channel();
        this.intChannel = reverseOutput ? specs.get(0).channel() : specs.get(1).channel();
        this.emitBatchSize = Math.max(emitBatchSize, 4096);
        this.reverseOutput = reverseOutput;
        this.current = new BytesIntVectorOnlyBlockHash(blockFactory);
    }

    @Override
    protected void prepareAddInput(Page page) {
        if (current instanceof BytesIntVectorOnlyBlockHash vectorHash) {
            if (vectorHash.shouldMigrateToPackedKeys() || intVector(page) == null || bytesVector(page) == null) {
                var packedHash = vectorHash.migrateToPackedHash();
                Releasables.close(current, () -> current = packedHash);
            }
        }
    }

    @Override
    protected void prepareForLookup(Page page) {
        if (current instanceof BytesIntVectorOnlyBlockHash vectorHash) {
            if (intVector(page) == null || bytesVector(page) == null) {
                var packedHash = vectorHash.migrateToPackedHash();
                Releasables.close(current, () -> current = packedHash);
            }
        }
    }

    private BytesRefVector bytesVector(Page page) {
        BytesRefBlock bytesBlock = page.getBlock(bytesChannel);
        return bytesBlock.asVector();
    }

    private IntVector intVector(Page page) {
        IntBlock intBlock = page.getBlock(intChannel);
        return intBlock.asVector();
    }

    final class BytesIntVectorOnlyBlockHash extends BlockHash {
        private final BytesRefBlockHash bytesHash;
        private final LongHashTable longHash;
        private final int[] batchIds;
        private long batchUsedBytes;

        BytesIntVectorOnlyBlockHash(BlockFactory blockFactory) {
            super(blockFactory);
            BytesRefBlockHash bytesHash = null;
            LongHashTable longHash = null;
            batchUsedBytes = 0;
            boolean success = false;
            try {
                bytesHash = new BytesRefBlockHash(bytesChannel, blockFactory);
                longHash = HashImplFactory.newLongHash(blockFactory);
                final long bytes = Integer.BYTES * (long) emitBatchSize;
                blockFactory.adjustBreaker(bytes);
                batchUsedBytes = bytes;
                batchIds = new int[emitBatchSize];
                this.bytesHash = bytesHash;
                this.longHash = longHash;
                success = true;
            } finally {
                if (success == false) {
                    Releasables.close(bytesHash, longHash);
                    blockFactory.adjustBreaker(-batchUsedBytes);
                }
            }
        }

        /**
         * If the bytes keys aren't repeated often enough, then using this BlockHash can be slower and use more memory.
         */
        boolean shouldMigrateToPackedKeys() {
            final long size = longHash.size();
            final long dictSize = bytesHash.hash.size();
            return size > 1024 && dictSize > size * 0.6;
        }

        @Override
        public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
            final BytesRefVector bytesVector = Objects.requireNonNull(bytesVector(page), "required bytes vector");
            final IntVector intVector = Objects.requireNonNull(intVector(page), "required int vector");
            try (IntVector bytesOrds = bytesHash.add(bytesVector)) {
                int position = intVector.getPositionCount();
                int offset = 0;
                while (offset < position) {
                    final int batchSize = Math.min(emitBatchSize, position - offset);
                    for (int i = 0; i < batchSize; i++) {
                        final int ordValue = bytesOrds.getInt(offset + i);
                        final int intValue = intVector.getInt(offset + i);
                        final long ord = longHash.add((long) ordValue << 32 | Integer.toUnsignedLong(intValue));
                        batchIds[i] = Math.toIntExact(hashOrdToGroup(ord));
                    }
                    try (var groupIds = blockFactory.newIntArrayVector(batchIds, batchSize)) {
                        addInput.add(offset, groupIds);
                    }
                    offset += batchSize;
                }
            }
        }

        @Override
        public int numKeys() {
            return Math.toIntExact(longHash.size());
        }

        @Override
        public ReleasableIterator<IntBlock> lookup(Page page, ByteSizeValue targetBlockSize) {
            final BytesRefVector bytesVector = Objects.requireNonNull(bytesVector(page), "required bytes vector");
            final IntVector intVector = Objects.requireNonNull(intVector(page), "required int vector");
            bytesVector.mustIncRef();
            intVector.mustIncRef();
            final long emitSize = targetBlockSize.getBytes() / (Integer.BYTES);
            return new ReleasableIterator<>() {
                private int offset = 0;
                final int positionCount = intVector.getPositionCount();

                @Override
                public boolean hasNext() {
                    return offset < positionCount;
                }

                @Override
                public IntBlock next() {
                    final int batchSize = (int) Math.min(emitSize, positionCount - offset);
                    final long ordsBytes = (long) batchSize * Integer.BYTES;
                    blockFactory.adjustBreaker(ordsBytes);
                    try {
                        final int[] ords = new int[batchSize];
                        BytesRef scratch = new BytesRef();
                        for (int i = 0; i < batchSize; i++) {
                            final long ord = bytesHash.hash.find(bytesVector.getBytesRef(offset + i, scratch));
                            ords[i] = Math.toIntExact(ord);
                        }
                        try (var groupIdsBuilder = blockFactory.newIntBlockBuilder(batchSize)) {
                            for (int i = 0; i < batchSize; i++) {
                                if (ords[i] < 0) {
                                    groupIdsBuilder.appendNull();
                                    continue;
                                }
                                final long longKey = ords[i] + 1; // zero reserved for null
                                final int intKey = intVector.getInt(offset + i);
                                long ord = longHash.find(longKey << 32 | Integer.toUnsignedLong(intKey));
                                if (ord < 0) {
                                    groupIdsBuilder.appendNull();
                                } else {
                                    groupIdsBuilder.appendInt(Math.toIntExact(ord));
                                }
                            }
                            offset += batchSize;
                            return groupIdsBuilder.build();
                        }
                    } finally {
                        blockFactory.adjustBreaker(-ordsBytes);
                    }
                }

                @Override
                public void close() {
                    Releasables.close(bytesVector::decRef, intVector::decRef);
                }
            };
        }

        static final VarHandle INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.nativeOrder());

        PackedValuesBlockHash migrateToPackedHash() {
            // TODO: allow specifying the initial size to avoid re-hashing
            final int numKeys = numKeys();
            boolean success = false;
            final PackedValuesBlockHash packed = new PackedValuesBlockHash(specs, blockFactory, emitBatchSize);
            final BytesRefHashTable packedHash = packed.bytesRefHash;
            try {
                BytesRefBuilder packedKey = new BytesRefBuilder();
                int offset = 0;
                final long[] longs = new long[128];
                BytesRef scratch = new BytesRef();
                while (offset < numKeys) {
                    int batchSize = Math.min(numKeys - offset, longs.length);
                    for (int i = 0; i < batchSize; i++) {
                        longs[i] = longHash.get(offset + i);
                    }
                    for (int i = 0; i < batchSize; i++) {
                        final int bytesOrd = Math.toIntExact((longs[i] >> 32) - 1);
                        scratch = bytesHash.hash.get(bytesOrd, scratch);
                        final int totalLength = 1 + scratch.length + 4;
                        packedKey.growNoCopy(totalLength);
                        packedKey.append((byte) 0); // byte 0 is reserved for the null bits in the packed values hash
                        if (reverseOutput) {
                            // int key
                            INT_HANDLE.set(packedKey.bytes(), packedKey.length(), (int) longs[i]);
                            packedKey.setLength(packedKey.length() + Integer.BYTES);
                            // bytes key
                            INT_HANDLE.set(packedKey.bytes(), packedKey.length(), scratch.length);
                            packedKey.setLength(packedKey.length() + Integer.BYTES);
                            packedKey.append(scratch);
                        } else {
                            // bytes key
                            INT_HANDLE.set(packedKey.bytes(), packedKey.length(), scratch.length);
                            packedKey.setLength(packedKey.length() + Integer.BYTES);
                            packedKey.append(scratch);
                            // int key
                            INT_HANDLE.set(packedKey.bytes(), packedKey.length(), (int) longs[i]);
                            packedKey.setLength(packedKey.length() + Integer.BYTES);
                        }
                        long ord = packedHash.add(packedKey.get());
                        assert ord == offset + i : "ord= " + ord + " != " + (offset + i);
                        packedKey.setLength(0);
                    }
                    offset += batchSize;
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
            Block bytesKeys = null;
            Block intKeys = null;
            boolean success = false;
            int positions = selected.getPositionCount();
            long usedBytes = Integer.BYTES * (long) selected.getPositionCount();
            blockFactory.adjustBreaker(usedBytes);
            final int[] byteOrds = new int[positions];
            try (var intsBuilder = blockFactory.newIntVectorFixedBuilder(positions)) {
                for (int i = 0; i < positions; i++) {
                    int groupId = selected.getInt(i);
                    long longValue = longHash.get(groupId);
                    byteOrds[i] = Math.toIntExact(longValue >>> 32);
                    intsBuilder.appendInt((int) (longValue));
                }
                intKeys = intsBuilder.build().asBlock();
                try (var bytesSelected = blockFactory.newIntArrayVector(byteOrds, positions)) {
                    bytesKeys = bytesHash.getKeys(bytesSelected)[0];
                }
                success = true;
            } finally {
                blockFactory.adjustBreaker(-usedBytes);
                if (success == false) {
                    Releasables.close(bytesKeys, intKeys);
                }
            }
            if (reverseOutput) {
                return new Block[] { intKeys, bytesKeys };
            } else {
                return new Block[] { bytesKeys, intKeys };
            }
        }

        @Override
        public IntVector nonEmpty() {
            return blockFactory.newIntRangeVector(0, numKeys());
        }

        @Override
        public BitArray seenGroupIds(BigArrays bigArrays) {
            return new Range(0, numKeys()).seenGroupIds(bigArrays);
        }

        @Override
        public void close() {
            blockFactory.adjustBreaker(-batchUsedBytes);
            Releasables.close(bytesHash, longHash);
        }

        @Override
        public String toString() {
            return "BytesIntBlockHash{keys=[BytesRefKey[channel="
                + bytesChannel
                + "], IntKey[channel="
                + intChannel
                + "]], entries="
                + longHash.size()
                + ", size="
                + longHash.ramBytesUsed()
                + "b}";
        }
    }
}
