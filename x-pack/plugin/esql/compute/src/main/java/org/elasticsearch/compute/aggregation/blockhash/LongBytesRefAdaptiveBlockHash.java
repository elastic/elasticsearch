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
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.common.util.BytesRefHashTable;
import org.elasticsearch.common.util.LongLongHashTable;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.List;

/**
 * An {@link AdaptiveBlockHash} for the two-key {@code (LONG, BYTES_REF)} (or {@code (BYTES_REF, LONG)})
 * grouping shape. As long as both inputs arrive as vectors, it routes through a dictionary-aware fast
 * path: {@link BytesRefBlockHash} hashes the bytes column once (so for an {@code OrdinalBytesRefVector}
 * it pays per dictionary entry, not per row), and a {@link LongLongHashTable} combines
 * {@code (bytesRefOrd, longKey)} into a stable group id. As soon as a page brings a non-vector key
 * column (i.e. nulls or multivalues are possible), it migrates to a {@link PackedValuesBlockHash} —
 * which has correct null/MV semantics — and replays existing groups so ordinals stay stable.
 *
 * <p>Modeled after {@link LongIntAdaptiveBlockHash}. The migration encoding is variable-width because
 * the {@code BYTES_REF} column has no fixed size; everything else is the same.
 */
public final class LongBytesRefAdaptiveBlockHash extends AdaptiveBlockHash {
    private final int longChannel;
    private final int bytesChannel;
    /**
     * Batch size for the vector-only path's bulk add arrays and for emitting
     * ords to aggs in the vector-only path. Must be at least 4096 so that
     * bulk-add operations can process full SIMD lanes.
     */
    private final int vectorBatchSize;
    private final boolean reverseOutput;

    public LongBytesRefAdaptiveBlockHash(List<GroupSpec> specs, BlockFactory blockFactory, int emitBatchSize, boolean reverseOutput) {
        super(specs, blockFactory, emitBatchSize);
        this.longChannel = reverseOutput ? specs.get(1).channel() : specs.get(0).channel();
        this.bytesChannel = reverseOutput ? specs.get(0).channel() : specs.get(1).channel();
        this.vectorBatchSize = Math.max(emitBatchSize, 4096);
        this.reverseOutput = reverseOutput;
        this.current = new BytesRefLongVectorOnlyBlockHash(blockFactory);
    }

    @Override
    protected void prepareAddInput(Page page) {
        if (current instanceof BytesRefLongVectorOnlyBlockHash vectorHash) {
            if (longVector(page) == null || bytesRefVector(page) == null) {
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

    private BytesRefVector bytesRefVector(Page page) {
        BytesRefBlock bytesBlock = page.getBlock(bytesChannel);
        return bytesBlock.asVector();
    }

    // for testing
    int effectiveEmitBatchSize() {
        if (current instanceof BytesRefLongVectorOnlyBlockHash) {
            return vectorBatchSize;
        } else {
            return emitBatchSize;
        }
    }

    /**
     * Whether this hash has migrated off the dictionary-aware vector-only fast path onto
     * {@link PackedValuesBlockHash}. Once migrated, the hash never goes back. Intended for
     * tests that need to pin "the fast path actually fired end-to-end" without scraping
     * {@link #toString()}.
     */
    public boolean migratedToPackedHash() {
        return current instanceof BytesRefLongVectorOnlyBlockHash == false;
    }

    final class BytesRefLongVectorOnlyBlockHash extends BlockHash {
        private final BytesRefBlockHash bytesHash;
        private final LongLongHashTable longLongHash;
        private final long batchUsedBytes;
        private final long[] batchKeys1;
        private final long[] batchKeys2;
        private final int[] batchIds;

        BytesRefLongVectorOnlyBlockHash(BlockFactory blockFactory) {
            super(blockFactory);
            final long bytes = (Integer.BYTES + Long.BYTES * 2) * (long) vectorBatchSize;
            blockFactory.adjustBreaker(bytes);
            this.batchUsedBytes = bytes;
            this.batchKeys1 = new long[vectorBatchSize];
            this.batchKeys2 = new long[vectorBatchSize];
            this.batchIds = new int[vectorBatchSize];
            BytesRefBlockHash bh = null;
            LongLongHashTable llh = null;
            boolean success = false;
            try {
                bh = new BytesRefBlockHash(bytesChannel, blockFactory);
                this.bytesHash = bh;
                llh = HashImplFactory.newLongLongHash(blockFactory);
                this.longLongHash = llh;
                success = true;
            } finally {
                if (success == false) {
                    blockFactory.adjustBreaker(-bytes);
                    Releasables.close(bh, llh);
                }
            }
        }

        @Override
        public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
            BytesRefVector bytesVector = bytesRefVector(page);
            LongVector longsVector = longVector(page);
            assert bytesVector != null && longsVector != null : "prepareAddInput must migrate before calling add with non-vector keys";
            try (IntVector bytesHashes = bytesHash.add(bytesVector)) {
                if (longLongHash.supportBulkAdd()) {
                    addBatch(bytesHashes, longsVector, addInput);
                } else {
                    addOneAtTime(bytesHashes, longsVector, addInput);
                }
            }
        }

        private void addBatch(IntVector bytesHashes, LongVector longsVector, GroupingAggregatorFunction.AddInput addInput) {
            final int positions = bytesHashes.getPositionCount();
            int offset = 0;
            while (offset < positions) {
                final int batchSize = Math.min(vectorBatchSize, positions - offset);
                for (int i = 0; i < batchSize; i++) {
                    batchKeys1[i] = bytesHashes.getInt(offset + i);
                    batchKeys2[i] = longsVector.getLong(offset + i);
                }
                longLongHash.bulkAdd(batchKeys1, batchKeys2, batchIds, batchSize);
                try (var groupIds = blockFactory.newIntArrayVector(batchIds, batchSize)) {
                    addInput.add(offset, groupIds);
                }
                offset += batchSize;
            }
        }

        private void addOneAtTime(IntVector bytesHashes, LongVector longsVector, GroupingAggregatorFunction.AddInput addInput) {
            int positions = bytesHashes.getPositionCount();
            int offset = 0;
            while (offset < positions) {
                final int batchSize = Math.min(vectorBatchSize, positions - offset);
                try (var groupIdsBuilder = blockFactory.newIntVectorFixedBuilder(batchSize)) {
                    for (int i = 0; i < batchSize; i++) {
                        int byteHash = bytesHashes.getInt(offset + i);
                        long longKey = longsVector.getLong(offset + i);
                        long ord = hashOrdToGroup(longLongHash.add(byteHash, longKey));
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
            BytesRefVector bytesVector = bytesRefVector(page);
            LongVector longsVector = longVector(page);
            assert bytesVector != null && longsVector != null : "prepareForLookup must migrate before calling lookup with non-vector keys";
            bytesVector.mustIncRef();
            longsVector.mustIncRef();
            // Floor at one ord per emit; otherwise sub-4-byte target sizes would deadlock the iterator.
            final long emitSize = Math.max(1L, targetBlockSize.getBytes() / Integer.BYTES);
            return new ReleasableIterator<>() {
                private int offset = 0;
                final int positionCount = bytesVector.getPositionCount();
                final BytesRef scratch = new BytesRef();

                @Override
                public boolean hasNext() {
                    return offset < positionCount;
                }

                @Override
                public IntBlock next() {
                    int batchSize = (int) Math.min(emitSize, positionCount - offset);
                    try (var builder = blockFactory.newIntBlockBuilder(batchSize)) {
                        for (int i = 0; i < batchSize; i++) {
                            BytesRef v = bytesVector.getBytesRef(offset + i, scratch);
                            long bytesId = bytesHash.hash.find(v);
                            if (bytesId < 0) {
                                builder.appendNull();
                                continue;
                            }
                            // bytesHash uses the null-reserved scheme: the int stored in longLongHash.key1
                            // is hash.add(...) + 1, so to look it up we add 1 here too.
                            long byteHash = bytesId + 1;
                            long longKey = longsVector.getLong(offset + i);
                            long ord = longLongHash.find(byteHash, longKey);
                            if (ord < 0) {
                                builder.appendNull();
                            } else {
                                builder.appendInt(Math.toIntExact(ord));
                            }
                        }
                        offset += batchSize;
                        return builder.build();
                    }
                }

                @Override
                public void close() {
                    Releasables.close(bytesVector::decRef, longsVector::decRef);
                }
            };
        }

        static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.nativeOrder());
        static final VarHandle INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.nativeOrder());

        /**
         * Re-key every existing {@code (bytesRefOrd, longKey)} group into a fresh
         * {@link PackedValuesBlockHash}. The packed hash uses the variable-width key layout
         * {@code [nullbits | col_0 | col_1]} where the {@code BYTES_REF} column contributes
         * a 4-byte length prefix followed by the raw bytes; the columns are written in spec order
         * (i.e. swapped when {@link #reverseOutput} is set). Vector-only inputs never observe nulls,
         * so the single null-tracking byte is always {@code 0} and {@code bytesRefOrd} is always
         * {@code >= 1}; the {@code ord == 0} branch is defensive.
         */
        PackedValuesBlockHash migrateToPackedHash() {
            // TODO: allow specifying the initial size to avoid re-hashing
            final int entries = numKeys();
            boolean success = false;
            final PackedValuesBlockHash packed = new PackedValuesBlockHash(specs, blockFactory, emitBatchSize);
            final BytesRefHashTable packedHash = packed.bytesRefHash;
            // 1 null-tracking byte for 2 columns (matches PackedValuesBlockHash#nullTrackingBytes for size=2).
            final int nullTrackingBytes = 1;
            try {
                BytesRef scratch = new BytesRef();
                BytesRef packedKey = new BytesRef();
                byte[] buf = new byte[nullTrackingBytes + Long.BYTES + Integer.BYTES + 16];
                packedKey.bytes = buf;
                for (int i = 0; i < entries; i++) {
                    long bytesRefOrd = longLongHash.getKey1(i);
                    long longKey = longLongHash.getKey2(i);
                    int bytesLen;
                    BytesRef bytes;
                    if (bytesRefOrd == 0) {
                        // Defensive: not reachable from the vector-only path.
                        bytes = null;
                        bytesLen = 0;
                    } else {
                        bytes = bytesHash.hash.get(bytesRefOrd - 1, scratch);
                        bytesLen = bytes.length;
                    }
                    int needed = nullTrackingBytes + Long.BYTES + Integer.BYTES + bytesLen;
                    if (buf.length < needed) {
                        buf = new byte[needed];
                        packedKey.bytes = buf;
                    }
                    buf[0] = 0;
                    int pos = nullTrackingBytes;
                    if (reverseOutput) {
                        // (BYTES_REF, LONG): [int_len | bytes | long]
                        INT_HANDLE.set(buf, pos, bytesLen);
                        pos += Integer.BYTES;
                        if (bytes != null && bytesLen > 0) {
                            System.arraycopy(bytes.bytes, bytes.offset, buf, pos, bytesLen);
                        }
                        pos += bytesLen;
                        LONG_HANDLE.set(buf, pos, longKey);
                        pos += Long.BYTES;
                    } else {
                        // (LONG, BYTES_REF): [long | int_len | bytes]
                        LONG_HANDLE.set(buf, pos, longKey);
                        pos += Long.BYTES;
                        INT_HANDLE.set(buf, pos, bytesLen);
                        pos += Integer.BYTES;
                        if (bytes != null && bytesLen > 0) {
                            System.arraycopy(bytes.bytes, bytes.offset, buf, pos, bytesLen);
                        }
                        pos += bytesLen;
                    }
                    packedKey.offset = 0;
                    packedKey.length = pos;
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
            BytesRefBlock k1 = null;
            LongVector k2 = null;
            int positions = selected.getPositionCount();
            if (OrdinalBytesRefBlock.isDense(positions, bytesHash.hash.size())) {
                try (var ordinals = blockFactory.newIntBlockBuilder(positions); var longs = blockFactory.newLongVectorBuilder(positions)) {
                    for (int i = 0; i < positions; i++) {
                        int groupId = selected.getInt(i);
                        long h1 = longLongHash.getKey1(groupId);
                        if (h1 == 0) {
                            // Not reachable in the vector-only path (which never inserts a null bytesref).
                            ordinals.appendNull();
                        } else {
                            ordinals.appendInt(Math.toIntExact(h1 - 1));
                        }
                        longs.appendLong(longLongHash.getKey2(groupId));
                    }
                    BytesRefArray bytes = bytesHash.hash.getBytesRefs();
                    bytes.incRef();
                    BytesRefVector dict = null;
                    try {
                        dict = blockFactory.newBytesRefArrayVector(bytes, Math.toIntExact(bytes.size()));
                        bytes = null; // transfer ownership to dict
                        k1 = new OrdinalBytesRefBlock(ordinals.build(), dict);
                        dict = null;  // transfer ownership to k1
                    } finally {
                        Releasables.closeExpectNoException(bytes, dict);
                    }
                    k2 = longs.build();
                } finally {
                    if (k2 == null) {
                        Releasables.closeExpectNoException(k1);
                    }
                }
            } else {
                try (
                    BytesRefBlock.Builder keys1 = blockFactory.newBytesRefBlockBuilder(positions);
                    LongVector.Builder keys2 = blockFactory.newLongVectorBuilder(positions)
                ) {
                    BytesRef scratch = new BytesRef();
                    for (int i = 0; i < positions; i++) {
                        int groupId = selected.getInt(i);
                        long h1 = longLongHash.getKey1(groupId);
                        if (h1 == 0) {
                            keys1.appendNull();
                        } else {
                            keys1.appendBytesRef(bytesHash.hash.get(h1 - 1, scratch));
                        }
                        keys2.appendLong(longLongHash.getKey2(groupId));
                    }
                    k1 = keys1.build();
                    k2 = keys2.build();
                } finally {
                    if (k2 == null) {
                        Releasables.closeExpectNoException(k1);
                    }
                }
            }
            if (reverseOutput) {
                return new Block[] { k1, k2.asBlock() };
            } else {
                return new Block[] { k2.asBlock(), k1 };
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
            blockFactory.adjustBreaker(-batchUsedBytes);
            Releasables.close(bytesHash, longLongHash);
        }

        @Override
        public String toString() {
            return "BytesRefLongBlockHash{keys=[BytesRefKey[channel="
                + bytesChannel
                + "], LongKey[channel="
                + longChannel
                + "]], entries="
                + longLongHash.size()
                + ", size="
                + bytesHash.hash.ramBytesUsed()
                + "b}";
        }
    }
}
