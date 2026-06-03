/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.BytesRefHashTable;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.mvdedupe.BatchEncoder;
import org.elasticsearch.compute.operator.mvdedupe.MultivalueDedupe;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.swisshash.BytesRefSwissHash;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;

/**
 * Maps any number of columns to a group ids with every unique combination resulting
 * in a unique group id. Works by unique-ing the values of each column and concatenating
 * the combinatorial explosion of all values into a byte array and then hashing each
 * byte array. If the values are
 * <pre>{@code
 *     a=(1, 2, 3) b=(2, 3) c=(4, 5, 5)
 * }</pre>
 * Then you get these grouping keys:
 * <pre>{@code
 *     1, 2, 4
 *     1, 2, 5
 *     1, 3, 4
 *     1, 3, 5
 *     2, 2, 4
 *     2, 2, 5
 *     2, 3, 4
 *     2, 3, 5
 *     3, 2, 4
 *     3, 3, 5
 * }</pre>
 * <p>
 *     The iteration order in the above is how we do it - it's as though it's
 *     nested {@code for} loops with the first column being the outer-most loop
 *     and the last column being the inner-most loop. See {@link Group} for more.
 * </p>
 */
final class PackedValuesBlockHash extends BlockHash {
    static final int DEFAULT_BATCH_SIZE = Math.toIntExact(ByteSizeValue.ofKb(10).getBytes());

    private final int emitBatchSize;
    final BytesRefHashTable bytesRefHash;
    private final int nullTrackingBytes;
    private final BreakingBytesRefBuilder bytes;
    private final List<GroupSpec> specs;
    private final BatchWork batchWork;
    private boolean seenNull;

    PackedValuesBlockHash(List<GroupSpec> specs, BlockFactory blockFactory, int emitBatchSize) {
        this(specs, blockFactory, blockFactory.breaker(), emitBatchSize);
    }

    /*
     * This constructor is also used by {@code PackedValuesBlockHashCircuitBreakerTests} to provide different circuit breakers
     *  to bytesRefHash and bytes. Production code should use the primary constructor above and provide same breaker for both.
     */
    PackedValuesBlockHash(List<GroupSpec> specs, BlockFactory blockFactory, CircuitBreaker circuitBreaker, int emitBatchSize) {
        super(blockFactory);
        this.specs = specs;
        this.emitBatchSize = emitBatchSize;
        this.nullTrackingBytes = (specs.size() + 7) / 8;
        final int[] columnOffsets = new int[specs.size()];
        int keyLength = nullTrackingBytes;
        boolean allFixedWidth = true;
        boolean variableWidthSupported = true;
        for (int g = 0; g < specs.size(); g++) {
            ElementType type = specs.get(g).elementType();
            int size = elementSize(type);
            if (size < 0) {
                allFixedWidth = false;
            } else if (allFixedWidth) {
                columnOffsets[g] = keyLength;
                keyLength += size;
            }
            // Variable-width path supports the same fixed-width set + BYTES_REF; everything else (e.g. NULL) excluded.
            if (type != ElementType.BOOLEAN
                && type != ElementType.INT
                && type != ElementType.LONG
                && type != ElementType.DOUBLE
                && type != ElementType.BYTES_REF) {
                variableWidthSupported = false;
            }
        }
        this.bytesRefHash = HashImplFactory.newBytesRefHash(blockFactory);
        boolean success = false;
        try {
            this.bytes = new BreakingBytesRefBuilder(circuitBreaker, "PackedValuesBlockHash", this.nullTrackingBytes);
            if (allFixedWidth && bytesRefHash instanceof BytesRefSwissHash swiss) {
                this.batchWork = new FixedWidthBatchWork(swiss, columnOffsets, keyLength, emitBatchSize, blockFactory, specs);
            } else if (variableWidthSupported && bytesRefHash instanceof BytesRefSwissHash swiss) {
                this.batchWork = new VariableWidthBatchWork(swiss, emitBatchSize, blockFactory, specs, nullTrackingBytes);
            } else {
                this.batchWork = null;
            }
            success = true;
        } finally {
            if (success == false) {
                close();
            }
        }
    }

    @Override
    public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
        if (batchWork != null && allVectors(page)) {
            batchWork.bulkAdd(page, addInput);
            return;
        }
        add(page, addInput, DEFAULT_BATCH_SIZE);
    }

    private boolean allVectors(Page page) {
        for (GroupSpec spec : specs) {
            if (page.getBlock(spec.channel()).asVector() == null) {
                return false;
            }
        }
        return true;
    }

    void add(Page page, GroupingAggregatorFunction.AddInput addInput, int batchSize) {
        try (AddWork work = new AddWork(page, addInput, batchSize)) {
            work.add();
        }
    }

    /**
     * The on-heap representation of a {@code for} loop for each group key.
     */
    private static class Group implements Releasable {
        final GroupSpec spec;
        final BatchEncoder encoder;
        int positionOffset;
        int valueOffset;
        /**
         * The number of values we've written for this group. Think of it as
         * the loop variable in a {@code for} loop.
         */
        int writtenValues;
        /**
         * The number of values of this group at this position. Think of it as
         * the maximum value in a {@code for} loop.
         */
        int valueCount;
        int bytesStart;

        Group(GroupSpec spec, Page page, int batchSize) {
            this.spec = spec;
            this.encoder = MultivalueDedupe.batchEncoder(page.getBlock(spec.channel()), batchSize, true);
        }

        @Override
        public void close() {
            encoder.close();
        }
    }

    class AddWork extends AddPage {
        final Group[] groups;
        final int positionCount;
        int position;

        AddWork(Page page, GroupingAggregatorFunction.AddInput addInput, int batchSize) {
            super(blockFactory, emitBatchSize, addInput);
            this.groups = specs.stream().map(s -> new Group(s, page, batchSize)).toArray(Group[]::new);
            this.positionCount = page.getPositionCount();
        }

        /**
         * Encodes one permutation of the keys at time into {@link #bytes} and adds it
         * to the {@link #bytesRefHash}. The encoding is mostly provided by
         * {@link BatchEncoder} with nulls living in a bit mask at the front of the bytes.
         */
        void add() {
            for (position = 0; position < positionCount; position++) {
                boolean singleEntry = startPosition(groups);
                if (singleEntry) {
                    addSingleEntry();
                } else {
                    addMultipleEntries();
                }
            }
            flushRemaining();
        }

        private void addSingleEntry() {
            fillBytesSv(groups);
            appendOrdSv(position, Math.toIntExact(hashOrdToGroup(bytesRefHash.add(bytes.bytesRefView()))));
        }

        private void addMultipleEntries() {
            int g = 0;
            do {
                fillBytesMv(groups, g);
                appendOrdInMv(position, Math.toIntExact(hashOrdToGroup(bytesRefHash.add(bytes.bytesRefView()))));
                g = rewindKeys(groups);
            } while (g >= 0);
            finishMv();
            for (Group group : groups) {
                group.valueOffset += group.valueCount;
            }
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(super::close, Releasables.wrap(groups));
        }
    }

    @Override
    public ReleasableIterator<IntBlock> lookup(Page page, ByteSizeValue targetBlockSize) {
        return new LookupWork(page, targetBlockSize.getBytes(), DEFAULT_BATCH_SIZE);
    }

    class LookupWork implements ReleasableIterator<IntBlock> {
        private final Group[] groups;
        private final long targetByteSize;
        private final int positionCount;
        private int position;

        LookupWork(Page page, long targetByteSize, int batchSize) {
            this.groups = specs.stream().map(s -> new Group(s, page, batchSize)).toArray(Group[]::new);
            this.positionCount = page.getPositionCount();
            this.targetByteSize = targetByteSize;
        }

        @Override
        public boolean hasNext() {
            return position < positionCount;
        }

        @Override
        public IntBlock next() {
            int size = Math.toIntExact(Math.min(positionCount - position, targetByteSize / Integer.BYTES / 2));
            try (IntBlock.Builder ords = blockFactory.newIntBlockBuilder(size)) {
                if (ords.estimatedBytes() > targetByteSize) {
                    throw new IllegalStateException(
                        "initial builder overshot target [" + ords.estimatedBytes() + "] vs [" + targetByteSize + "]"
                    );
                }
                while (position < positionCount && ords.estimatedBytes() < targetByteSize) {
                    // TODO a test where targetByteSize is very small should still make a few rows.
                    boolean singleEntry = startPosition(groups);
                    if (singleEntry) {
                        lookupSingleEntry(ords);
                    } else {
                        lookupMultipleEntries(ords);
                    }
                    position++;
                }
                return ords.build();
            }
        }

        private void lookupSingleEntry(IntBlock.Builder ords) {
            fillBytesSv(groups);
            long found = bytesRefHash.find(bytes.bytesRefView());
            if (found < 0) {
                ords.appendNull();
            } else {
                ords.appendInt(Math.toIntExact(found));
            }
        }

        private void lookupMultipleEntries(IntBlock.Builder ords) {
            long firstFound = -1;
            boolean began = false;
            int g = 0;
            int count = 0;
            do {
                fillBytesMv(groups, g);

                // emit ords
                long found = bytesRefHash.find(bytes.bytesRefView());
                if (found >= 0) {
                    if (firstFound < 0) {
                        firstFound = found;
                    } else {
                        if (began == false) {
                            began = true;
                            ords.beginPositionEntry();
                            ords.appendInt(Math.toIntExact(firstFound));
                            count++;
                        }
                        ords.appendInt(Math.toIntExact(found));
                        count++;
                        if (count > Block.MAX_LOOKUP) {
                            // TODO replace this with a warning and break and remove the accepted error from GenerativeRestTest
                            throw new IllegalArgumentException("Found a single entry with " + count + " entries");
                        }
                    }
                }
                g = rewindKeys(groups);
            } while (g >= 0);
            if (firstFound < 0) {
                ords.appendNull();
            } else if (began) {
                ords.endPositionEntry();
            } else {
                // Only found one value
                ords.appendInt(Math.toIntExact(hashOrdToGroup(firstFound)));
            }
            for (Group group : groups) {
                group.valueOffset += group.valueCount;
            }
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(groups);
        }
    }

    /**
     * Correctly position all {@code groups}, clear the {@link #bytes},
     * and position it past the null tracking bytes. Call this before
     * encoding a new position.
     * @return true if this position has only a single ordinal
     */
    private boolean startPosition(Group[] groups) {
        boolean singleEntry = true;
        for (Group g : groups) {
            /*
             * Make sure all encoders have encoded the current position and the
             * offsets are queued to its start.
             */
            var encoder = g.encoder;
            g.positionOffset++;
            while (g.positionOffset >= encoder.positionCount()) {
                encoder.encodeNextBatch();
                g.positionOffset = 0;
                g.valueOffset = 0;
            }
            g.valueCount = encoder.valueCount(g.positionOffset);
            singleEntry &= (g.valueCount == 1);
        }
        Arrays.fill(bytes.bytes(), 0, nullTrackingBytes, (byte) 0);
        bytes.setLength(nullTrackingBytes);
        return singleEntry;
    }

    private void fillBytesSv(Group[] groups) {
        for (int g = 0; g < groups.length; g++) {
            Group group = groups[g];
            assert group.writtenValues == 0;
            assert group.valueCount == 1;
            if (group.encoder.read(group.valueOffset++, bytes) == 0) {
                seenNull = true;
                int nullByte = g / 8;
                int nullShift = g % 8;
                bytes.bytes()[nullByte] |= (byte) (1 << nullShift);
            }
        }
        padZerosForFixedLengthKey();
    }

    private void fillBytesMv(Group[] groups, int startingGroup) {
        for (int g = startingGroup; g < groups.length; g++) {
            Group group = groups[g];
            group.bytesStart = bytes.length();
            if (group.encoder.read(group.valueOffset + group.writtenValues, bytes) == 0) {
                assert group.valueCount == 1 : "null value in non-singleton list";
                seenNull = true;
                int nullByte = g / 8;
                int nullShift = g % 8;
                bytes.bytes()[nullByte] |= (byte) (1 << nullShift);
            }
            ++group.writtenValues;
        }
        padZerosForFixedLengthKey();
    }

    private void padZerosForFixedLengthKey() {
        if (batchWork == null) {
            return;
        }
        final int keyLength = batchWork.fixedKeyLength();
        if (keyLength < 0) {
            return;
        }
        final int len = bytes.length();
        if (len < keyLength) {
            bytes.grow(keyLength);
            Arrays.fill(bytes.bytes(), len, keyLength, (byte) 0);
            bytes.setLength(keyLength);
        }
    }

    private int rewindKeys(Group[] groups) {
        int g = groups.length - 1;
        Group group = groups[g];
        bytes.setLength(group.bytesStart);
        while (group.writtenValues == group.valueCount) {
            group.writtenValues = 0;
            if (g == 0) {
                return -1;
            } else {
                group = groups[--g];
                bytes.setLength(group.bytesStart);
            }
        }
        return g;
    }

    @Override
    public Block[] getKeys(IntVector selected) {
        if (batchWork != null && seenNull == false) {
            return batchWork.getNonNullKeys(selected, bytesRefHash, blockFactory);
        }
        return getKeysWithNulls(selected);
    }

    private Block[] getKeysWithNulls(IntVector selected) {
        int positions = selected.getPositionCount();
        BatchEncoder.Decoder[] decoders = new BatchEncoder.Decoder[specs.size()];
        Block.Builder[] builders = new Block.Builder[specs.size()];
        try {
            for (int g = 0; g < builders.length; g++) {
                ElementType elementType = specs.get(g).elementType();
                decoders[g] = BatchEncoder.decoder(elementType);
                builders[g] = elementType.newBlockBuilder(positions, blockFactory);
            }

            int batchSize = Math.min(100, positions);
            BytesRef[] values = new BytesRef[batchSize];
            BytesRef[] nulls = new BytesRef[batchSize];
            for (int b = 0; b < batchSize; b++) {
                values[b] = new BytesRef();
                nulls[b] = new BytesRef();
                nulls[b].length = nullTrackingBytes;
            }
            int offset = 0;
            for (int i = 0; i < positions; i++) {
                int groupId = selected.getInt(i);
                values[offset] = bytesRefHash.get(groupId, values[offset]);

                // Reference the null bytes in the nulls array and values in the values
                nulls[offset].bytes = values[offset].bytes;
                nulls[offset].offset = values[offset].offset;
                values[offset].offset += nullTrackingBytes;
                values[offset].length -= nullTrackingBytes;

                offset++;
                if (offset == batchSize) {
                    readKeys(decoders, builders, nulls, values, offset);
                    offset = 0;
                }
            }
            if (offset > 0) {
                readKeys(decoders, builders, nulls, values, offset);
            }
            return Block.Builder.buildAll(builders);
        } finally {
            Releasables.closeExpectNoException(builders);
        }
    }

    private void readKeys(BatchEncoder.Decoder[] decoders, Block.Builder[] builders, BytesRef[] nulls, BytesRef[] values, int count) {
        for (int g = 0; g < builders.length; g++) {
            int nullByte = g / 8;
            int nullShift = g % 8;
            byte nullTest = (byte) (1 << nullShift);
            BatchEncoder.IsNull isNull = offset -> {
                BytesRef n = nulls[offset];
                return (n.bytes[n.offset + nullByte] & nullTest) != 0;
            };
            decoders[g].decode(builders[g], isNull, values, count);
        }
    }

    @Override
    public IntVector nonEmpty() {
        return blockFactory.newIntRangeVector(0, Math.toIntExact(bytesRefHash.size()));
    }

    @Override
    public int numKeys() {
        return Math.toIntExact(bytesRefHash.size());
    }

    @Override
    public BitArray seenGroupIds(BigArrays bigArrays) {
        return new SeenGroupIds.Range(0, Math.toIntExact(bytesRefHash.size())).seenGroupIds(bigArrays);
    }

    @Override
    public void close() {
        Releasables.close(bytesRefHash, bytes, batchWork);
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("PackedValuesBlockHash{groups=[");
        for (int i = 0; i < specs.size(); i++) {
            if (i > 0) {
                b.append(", ");
            }
            GroupSpec spec = specs.get(i);
            b.append(spec.channel()).append(':').append(spec.elementType());
        }
        b.append("], entries=").append(bytesRefHash.size());
        b.append(", size=").append(ByteSizeValue.ofBytes(bytesRefHash.ramBytesUsed()));
        return b.append("}").toString();
    }

    private static int elementSize(ElementType type) {
        return switch (type) {
            case NULL -> 0;
            case BOOLEAN -> 1;
            case INT -> Integer.BYTES;
            case LONG, DOUBLE -> Long.BYTES;
            default -> -1;
        };
    }

    private static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.nativeOrder());
    private static final VarHandle INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.nativeOrder());
    private static final VarHandle DOUBLE_HANDLE = MethodHandles.byteArrayViewVarHandle(double[].class, ByteOrder.nativeOrder());

    /**
     * Approximate shallow size of a {@link BytesRef} object: object header + {@code byte[]} reference + 2 ints.
     * Used only for breaker accounting; precision matches the rest of this class's manual byte counting.
     */
    private static final long BYTES_REF_SHALLOW_BYTES = RamUsageEstimator.shallowSizeOfInstance(BytesRef.class);

    /** Emit one batch's worth of group ids through {@code addInput}. */
    static void emitBatch(
        BlockFactory blockFactory,
        GroupingAggregatorFunction.AddInput addInput,
        int[] groupIds,
        int positionOffset,
        int emitSize
    ) {
        try (var groupIdsVec = blockFactory.newIntArrayVector(groupIds, emitSize)) {
            addInput.add(positionOffset, groupIdsVec);
        }
    }

    /**
     * Bulk-path worker for all-vector pages. One of two implementations is selected per
     * {@link PackedValuesBlockHash} instance, so calls dispatch monomorphically.
     */
    private sealed interface BatchWork extends Releasable permits FixedWidthBatchWork, VariableWidthBatchWork {
        void bulkAdd(Page page, GroupingAggregatorFunction.AddInput addInput);

        Block[] getNonNullKeys(IntVector selected, BytesRefHashTable hash, BlockFactory blockFactory);

        /** Fixed encoded key length in bytes, or {@code -1} if rows have variable length. */
        int fixedKeyLength();
    }

    private static final class FixedWidthBatchWork implements BatchWork {
        private static final int BATCH_SIZE = 128;
        private final PrefetchBarrier prefetchBarrier = new PrefetchBarrier();
        final int[] offsets;
        final int keyLength;
        final byte[] keys;
        final long[] hashes;
        final int[] groupIds;
        private final int emitBatchSize;
        private final BytesRefSwissHash swiss;

        private final BlockFactory blockFactory;
        private final List<GroupSpec> specs;
        /** Per-page resolved vectors, refreshed once at the top of {@link #bulkAdd} so {@link #fillKeys} doesn't refetch per chunk. */
        private final Vector[] vectors;
        private long usedBytes;

        FixedWidthBatchWork(
            BytesRefSwissHash swiss,
            int[] offsets,
            int keyLength,
            int emitBatchSize,
            BlockFactory blockFactory,
            List<GroupSpec> specs
        ) {
            this.swiss = swiss;
            this.offsets = offsets;
            this.keyLength = keyLength;
            this.blockFactory = blockFactory;
            this.specs = specs;
            this.usedBytes = (long) BATCH_SIZE * keyLength + (long) emitBatchSize * Integer.BYTES + (long) BATCH_SIZE * Long.BYTES
                + (long) specs.size() * RamUsageEstimator.NUM_BYTES_OBJECT_REF;
            blockFactory.adjustBreaker(usedBytes);
            this.keys = new byte[BATCH_SIZE * keyLength];
            this.hashes = new long[BATCH_SIZE];
            this.groupIds = new int[emitBatchSize];
            this.vectors = new Vector[specs.size()];
            this.emitBatchSize = emitBatchSize;
        }

        @Override
        public int fixedKeyLength() {
            return keyLength;
        }

        @Override
        public void bulkAdd(Page page, GroupingAggregatorFunction.AddInput addInput) {
            int positionCount = page.getPositionCount();
            int positionOffset = 0;
            final BytesRef key = new BytesRef();
            key.bytes = keys;
            key.length = keyLength;
            int dummy = 0;
            for (int g = 0; g < specs.size(); g++) {
                vectors[g] = page.getBlock(specs.get(g).channel()).asVector();
            }
            while (positionOffset < positionCount) {
                final int emitSize = Math.min(emitBatchSize, positionCount - positionOffset);
                int batchOffset = 0;
                while (batchOffset < emitSize) {
                    final int chunkSize = Math.min(BATCH_SIZE, emitSize - batchOffset);
                    fillKeys(positionOffset + batchOffset, chunkSize);
                    if (swiss.shouldPrefetch()) {
                        for (int i = 0; i < chunkSize; i++) {
                            hashes[i] = BytesRefSwissHash.hash64(keys, i * keyLength, keyLength);
                            dummy ^= swiss.prefetch(hashes[i]);
                        }
                        for (int i = 0; i < chunkSize; i++) {
                            key.offset = i * keyLength;
                            long id = swiss.addWithHash(key, hashes[i]);
                            groupIds[batchOffset + i] = Math.toIntExact(hashOrdToGroup(id));
                        }
                    } else {
                        for (int i = 0; i < chunkSize; i++) {
                            key.offset = i * keyLength;
                            long id = swiss.add(key);
                            groupIds[batchOffset + i] = Math.toIntExact(hashOrdToGroup(id));
                        }
                    }
                    batchOffset += chunkSize;
                }
                emitBatch(blockFactory, addInput, groupIds, positionOffset, emitSize);
                positionOffset += emitSize;
            }
            prefetchBarrier.consume(dummy);
        }

        private void fillKeys(int positionOffset, int batchSize) {
            for (int g = 0; g < specs.size(); g++) {
                final int offset = offsets[g];
                final Vector vector = vectors[g];
                switch (specs.get(g).elementType()) {
                    case LONG -> {
                        LongVector longVector = (LongVector) vector;
                        for (int i = 0; i < batchSize; i++) {
                            LONG_HANDLE.set(keys, i * keyLength + offset, longVector.getLong(positionOffset + i));
                        }
                    }
                    case INT -> {
                        IntVector intVector = (IntVector) vector;
                        for (int i = 0; i < batchSize; i++) {
                            INT_HANDLE.set(keys, i * keyLength + offset, intVector.getInt(positionOffset + i));
                        }
                    }
                    case DOUBLE -> {
                        DoubleVector doubleVector = (DoubleVector) vector;
                        for (int i = 0; i < batchSize; i++) {
                            DOUBLE_HANDLE.set(keys, i * keyLength + offset, doubleVector.getDouble(positionOffset + i));
                        }
                    }
                    case BOOLEAN -> {
                        BooleanVector booleanVector = (BooleanVector) vector;
                        for (int i = 0; i < batchSize; i++) {
                            keys[i * keyLength + offset] = booleanVector.getBoolean(positionOffset + i) ? (byte) 1 : (byte) 0;
                        }
                    }
                    default -> throw new IllegalStateException("unsupported type: " + specs.get(g).elementType());
                }
            }
        }

        @Override
        public Block[] getNonNullKeys(IntVector selected, BytesRefHashTable hash, BlockFactory blockFactory) {
            final int positions = selected.getPositionCount();
            final Block.Builder[] builders = new Block.Builder[specs.size()];
            final BytesRef[] bytes = new BytesRef[BATCH_SIZE];
            for (int i = 0; i < BATCH_SIZE; i++) {
                bytes[i] = new BytesRef();
            }
            try {
                for (int g = 0; g < builders.length; g++) {
                    builders[g] = specs.get(g).elementType().newBlockBuilder(positions, blockFactory);
                }
                for (int p = 0; p < positions; p += BATCH_SIZE) {
                    final int batchSize = Math.min(BATCH_SIZE, positions - p);
                    for (int r = 0; r < batchSize; r++) {
                        hash.get(selected.getInt(p + r), bytes[r]);
                    }
                    for (int g = 0; g < specs.size(); g++) {
                        final int columnOffset = offsets[g];
                        switch (specs.get(g).elementType()) {
                            case LONG -> {
                                var b = (LongBlock.Builder) builders[g];
                                for (int r = 0; r < batchSize; r++) {
                                    b.appendLong((long) LONG_HANDLE.get(bytes[r].bytes, bytes[r].offset + columnOffset));
                                }
                            }
                            case INT -> {
                                var b = (IntBlock.Builder) builders[g];
                                for (int r = 0; r < batchSize; r++) {
                                    b.appendInt((int) INT_HANDLE.get(bytes[r].bytes, bytes[r].offset + columnOffset));
                                }
                            }
                            case DOUBLE -> {
                                var b = (DoubleBlock.Builder) builders[g];
                                for (int r = 0; r < batchSize; r++) {
                                    b.appendDouble((double) DOUBLE_HANDLE.get(bytes[r].bytes, bytes[r].offset + columnOffset));
                                }
                            }
                            case BOOLEAN -> {
                                var b = (BooleanBlock.Builder) builders[g];
                                for (int r = 0; r < batchSize; r++) {
                                    b.appendBoolean(bytes[r].bytes[bytes[r].offset + columnOffset] != 0);
                                }
                            }
                            default -> throw new IllegalStateException("unsupported type: " + specs.get(g).elementType());
                        }
                    }
                }
                return Block.Builder.buildAll(builders);
            } finally {
                Releasables.closeExpectNoException(builders);
            }
        }

        @Override
        public void close() {
            final long bytes = usedBytes;
            usedBytes = 0;
            blockFactory.adjustBreaker(-bytes);
            prefetchBarrier.flush();
        }
    }

    /**
     * Bulk path for vector pages where at least one column is {@code BYTES_REF}.
     *
     * <p>Per batch: collect per-row encoded sizes, materialize one flat {@code keyBuf}
     * laid out as {@code [nullTrackingBytes | col0 | col1 | ... | colN]} per row,
     * then hash + insert. The encoding mirrors the slow path's {@link BatchEncoder}
     * layout so keys produced here are byte-identical to keys produced by
     * {@link AddWork} — that's what lets {@link #getKeys} mix bulk-built keys with
     * slow-path keys without re-encoding.
     *
     * <p>For fixed-width columns each row contributes a constant number of bytes;
     * for {@code BYTES_REF} columns each row contributes a 4-byte length prefix
     * followed by the value bytes. A soft per-chunk cap of {@value #CHUNK_SOFT_CAP}
     * bytes bounds {@link #keyBuf} on typical workloads while still giving
     * adversarial single oversized rows a chunk of their own.
     */
    private static final class VariableWidthBatchWork implements BatchWork {
        private static final int BATCH_SIZE = 128;
        private static final int INITIAL_KEY_BUF = 8 * 1024;
        private static final int CHUNK_SOFT_CAP = 256 * 1024;

        private final PrefetchBarrier prefetchBarrier = new PrefetchBarrier();
        private final BytesRefSwissHash swiss;
        private final BlockFactory blockFactory;
        private final List<GroupSpec> specs;
        private final int emitBatchSize;
        private final int nullTrackingBytes;

        /**
         * Spec indices of the {@code BYTES_REF} columns, in spec order. Lets the per-row sizing loop
         * iterate only over variable-width columns and index directly into {@link #vectors}.
         */
        private final int[] bytesRefSpecIdx;

        /**
         * Inverse of {@link #bytesRefSpecIdx}: maps each spec index to its position in the
         * {@code BYTES_REF} cache, or {@code -1} for non-{@code BYTES_REF} specs. Lets
         * {@link #serializeColumns} look up the cached {@link BytesRef} for the current spec
         * without a running counter.
         */
        private final int[] gToBytesRefIdx;

        /**
         * Constant per-row contribution: {@link #nullTrackingBytes} + sum of fixed-width column sizes
         * + {@code Integer.BYTES} length prefix per {@code BYTES_REF} column. Only the variable-length
         * value bytes are added per row.
         */
        private final int fixedRowBase;

        /**
         * Per-chunk key slices: {@code rowKeys[i].bytes} points at {@link #keyBuf} and
         * {@code offset}/{@code length} delimit the serialized bytes for row {@code i}.
         * Set by {@link #collectRowSizes} (offset + length) and refreshed after every
         * {@link #ensureKeyBuf} call (bytes pointer, which may change on reallocation).
         */
        private final BytesRef[] rowKeys;
        private final long[] hashes;
        private final int[] groupIds;
        /** Per-page resolved vectors, refreshed once at the top of {@link #bulkAdd}. */
        private final Vector[] vectors;
        /**
         * Same contents as {@link #vectors} restricted to {@code BYTES_REF} columns, in
         * {@link #bytesRefSpecIdx} order. Refreshed at the top of {@link #bulkAdd} so the
         * sizing/serialize loops don't pay a {@code (BytesRefVector)} cast per row.
         */
        private final BytesRefVector[] brVectors;
        /**
         * Per-chunk cache of {@code BYTES_REF} values, indexed as {@code [bytesRefIdx][rowInChunk]}.
         * Populated in {@link #collectRowSizes} (where each value is read once) and consumed in
         * {@link #serializeColumns}, avoiding a second {@code getBytesRef} call per row.
         */
        private final BytesRef[][] bytesRefCache;

        private byte[] keyBuf;
        private long usedBytes;

        VariableWidthBatchWork(
            BytesRefSwissHash swiss,
            int emitBatchSize,
            BlockFactory blockFactory,
            List<GroupSpec> specs,
            int nullTrackingBytes
        ) {
            this.swiss = swiss;
            this.blockFactory = blockFactory;
            this.specs = specs;
            this.emitBatchSize = emitBatchSize;
            this.nullTrackingBytes = nullTrackingBytes;
            this.gToBytesRefIdx = new int[specs.size()];
            int rowBase = nullTrackingBytes;
            int brCount = 0;
            for (int g = 0; g < specs.size(); g++) {
                int cs = elementSize(specs.get(g).elementType());
                if (cs >= 0) {
                    rowBase += cs;
                    this.gToBytesRefIdx[g] = -1;
                } else {
                    rowBase += Integer.BYTES;
                    this.gToBytesRefIdx[g] = brCount++;
                }
            }
            this.fixedRowBase = rowBase;
            this.bytesRefSpecIdx = new int[brCount];
            for (int g = 0, b = 0; g < specs.size(); g++) {
                if (specs.get(g).elementType() == ElementType.BYTES_REF) {
                    bytesRefSpecIdx[b++] = g;
                }
            }
            // Approximate accounting:
            // emitBatchSize + gToBytesRefIdx(specs.size()) ints
            // + hashes(BATCH_SIZE) longs + rowKeys(BATCH_SIZE) BytesRef objects
            // + keyBuf
            // + rowKeys(BATCH_SIZE) + vectors(specs.size()) + brVectors(brCount) refs
            // + bytesRefCache: brCount inner-array refs + brCount * BATCH_SIZE refs + brCount * BATCH_SIZE BytesRef objects
            final long brCacheBytes = (long) brCount * (RamUsageEstimator.NUM_BYTES_OBJECT_REF + (long) BATCH_SIZE
                * (RamUsageEstimator.NUM_BYTES_OBJECT_REF + BYTES_REF_SHALLOW_BYTES));
            final long refBytes = (long) (BATCH_SIZE + specs.size() + brCount) * RamUsageEstimator.NUM_BYTES_OBJECT_REF;
            final long fixedBytes = (long) (emitBatchSize + specs.size()) * Integer.BYTES + (long) BATCH_SIZE * (Long.BYTES
                + BYTES_REF_SHALLOW_BYTES) + INITIAL_KEY_BUF + refBytes + brCacheBytes;
            blockFactory.adjustBreaker(fixedBytes);
            this.usedBytes = fixedBytes;
            boolean success = false;
            try {
                this.rowKeys = new BytesRef[BATCH_SIZE];
                for (int i = 0; i < BATCH_SIZE; i++) {
                    this.rowKeys[i] = new BytesRef();
                }
                this.hashes = new long[BATCH_SIZE];
                this.groupIds = new int[emitBatchSize];
                this.keyBuf = new byte[INITIAL_KEY_BUF];
                this.vectors = new Vector[specs.size()];
                this.brVectors = new BytesRefVector[brCount];
                this.bytesRefCache = new BytesRef[brCount][BATCH_SIZE];
                for (int b = 0; b < brCount; b++) {
                    for (int i = 0; i < BATCH_SIZE; i++) {
                        this.bytesRefCache[b][i] = new BytesRef();
                    }
                }
                success = true;
            } finally {
                if (success == false) {
                    blockFactory.adjustBreaker(-fixedBytes);
                    this.usedBytes = 0;
                }
            }
        }

        @Override
        public int fixedKeyLength() {
            return -1;
        }

        @Override
        public void bulkAdd(Page page, GroupingAggregatorFunction.AddInput addInput) {
            final int positionCount = page.getPositionCount();
            int positionOffset = 0;
            final int[] cursors = new int[BATCH_SIZE];
            int dummy = 0;
            for (int g = 0; g < specs.size(); g++) {
                vectors[g] = page.getBlock(specs.get(g).channel()).asVector();
            }
            for (int b = 0; b < brVectors.length; b++) {
                brVectors[b] = (BytesRefVector) vectors[bytesRefSpecIdx[b]];
            }
            while (positionOffset < positionCount) {
                final int emitSize = Math.min(emitBatchSize, positionCount - positionOffset);
                int batchOffset = 0;
                while (batchOffset < emitSize) {
                    final int requested = Math.min(BATCH_SIZE, emitSize - batchOffset);
                    final int chunkSize = collectRowSizes(positionOffset + batchOffset, requested);
                    ensureKeyBuf(rowKeys[chunkSize - 1].offset + rowKeys[chunkSize - 1].length);
                    for (int i = 0; i < chunkSize; i++) {
                        rowKeys[i].bytes = keyBuf;
                    }
                    serializeColumns(positionOffset + batchOffset, chunkSize, cursors);

                    if (swiss.shouldPrefetch()) {
                        for (int i = 0; i < chunkSize; i++) {
                            hashes[i] = BytesRefSwissHash.hash64(rowKeys[i]);
                            dummy ^= swiss.prefetch(hashes[i]);
                        }
                        for (int i = 0; i < chunkSize; i++) {
                            long id = swiss.addWithHash(rowKeys[i], hashes[i]);
                            groupIds[batchOffset + i] = Math.toIntExact(hashOrdToGroup(id));
                        }
                    } else {
                        for (int i = 0; i < chunkSize; i++) {
                            long id = swiss.add(rowKeys[i]);
                            groupIds[batchOffset + i] = Math.toIntExact(hashOrdToGroup(id));
                        }
                    }
                    batchOffset += chunkSize;
                }
                emitBatch(blockFactory, addInput, groupIds, positionOffset, emitSize);
                positionOffset += emitSize;
            }
            prefetchBarrier.consume(dummy);
        }

        /**
         * Size each row and write its starting byte offset and length into {@link #rowKeys},
         * ready for {@link #serializeColumns} and hashing. Honors the {@link #CHUNK_SOFT_CAP}
         * soft cap and always returns at least 1 row; an oversized single row gets a chunk of
         * its own and forces {@link #keyBuf} to grow when serialized.
         *
         * <p>Per-page page→block→vector lookups are hoisted out of the row loop into
         * {@link #vectors}; the sizing loop iterates only the {@code BYTES_REF} columns
         * (fixed-width contributions are folded into {@link #fixedRowBase}). Each
         * {@code BYTES_REF} value is read once here and stashed into {@link #bytesRefCache}
         * so {@link #serializeColumns} can reuse it without a second {@code getBytesRef} call.
         */
        private int collectRowSizes(int positionOffset, int requested) {
            final int brCount = brVectors.length;
            int offset = 0;
            int rows = 0;
            for (int i = 0; i < requested; i++) {
                int sz = fixedRowBase;
                for (int b = 0; b < brCount; b++) {
                    final BytesRef br = brVectors[b].getBytesRef(positionOffset + i, bytesRefCache[b][i]);
                    bytesRefCache[b][i] = br;
                    sz += br.length;
                }
                if (rows > 0 && offset + sz > CHUNK_SOFT_CAP) {
                    break;
                }
                rowKeys[i].offset = offset;
                rowKeys[i].length = sz;
                offset += sz;
                rows++;
            }
            return rows;
        }

        private void ensureKeyBuf(int size) {
            if (keyBuf.length >= size) {
                return;
            }
            final int newCap = ArrayUtil.oversize(size, Byte.BYTES);
            final long delta = (long) newCap - keyBuf.length;
            blockFactory.adjustBreaker(delta);
            keyBuf = new byte[newCap];
            usedBytes += delta;
        }

        private void serializeColumns(int positionOffset, int rows, int[] cursors) {
            // Vectors carry no nulls, so we only need to clear the per-row null tracking prefix; column bytes are fully overwritten below.
            for (int i = 0; i < rows; i++) {
                final int o = rowKeys[i].offset;
                for (int b = 0; b < nullTrackingBytes; b++) {
                    keyBuf[o + b] = 0;
                }
                cursors[i] = o + nullTrackingBytes;
            }
            for (int g = 0; g < specs.size(); g++) {
                final Vector vector = vectors[g];
                switch (specs.get(g).elementType()) {
                    case LONG -> {
                        LongVector lv = (LongVector) vector;
                        for (int i = 0; i < rows; i++) {
                            LONG_HANDLE.set(keyBuf, cursors[i], lv.getLong(positionOffset + i));
                            cursors[i] += Long.BYTES;
                        }
                    }
                    case INT -> {
                        IntVector iv = (IntVector) vector;
                        for (int i = 0; i < rows; i++) {
                            INT_HANDLE.set(keyBuf, cursors[i], iv.getInt(positionOffset + i));
                            cursors[i] += Integer.BYTES;
                        }
                    }
                    case DOUBLE -> {
                        DoubleVector dv = (DoubleVector) vector;
                        for (int i = 0; i < rows; i++) {
                            DOUBLE_HANDLE.set(keyBuf, cursors[i], dv.getDouble(positionOffset + i));
                            cursors[i] += Double.BYTES;
                        }
                    }
                    case BOOLEAN -> {
                        BooleanVector bv = (BooleanVector) vector;
                        for (int i = 0; i < rows; i++) {
                            keyBuf[cursors[i]] = bv.getBoolean(positionOffset + i) ? (byte) 1 : (byte) 0;
                            cursors[i] += Byte.BYTES;
                        }
                    }
                    case BYTES_REF -> {
                        final BytesRef[] cacheRow = bytesRefCache[gToBytesRefIdx[g]];
                        for (int i = 0; i < rows; i++) {
                            final BytesRef br = cacheRow[i];
                            final int c = cursors[i];
                            INT_HANDLE.set(keyBuf, c, br.length);
                            System.arraycopy(br.bytes, br.offset, keyBuf, c + Integer.BYTES, br.length);
                            cursors[i] = c + Integer.BYTES + br.length;
                        }
                    }
                    default -> throw new IllegalStateException("unsupported type: " + specs.get(g).elementType());
                }
            }
        }

        @Override
        public Block[] getNonNullKeys(IntVector selected, BytesRefHashTable hash, BlockFactory blockFactory) {
            final int positions = selected.getPositionCount();
            final Block.Builder[] builders = new Block.Builder[specs.size()];
            final BytesRef bytesRefScratch = new BytesRef();
            final BytesRef[] decodeKeys = new BytesRef[BATCH_SIZE];
            for (int i = 0; i < BATCH_SIZE; i++) {
                decodeKeys[i] = new BytesRef();
            }
            try {
                for (int g = 0; g < builders.length; g++) {
                    builders[g] = specs.get(g).elementType().newBlockBuilder(positions, blockFactory);
                }
                for (int p = 0; p < positions; p += BATCH_SIZE) {
                    final int batchSize = Math.min(BATCH_SIZE, positions - p);
                    // Use decodeKeys[r].offset as the per-row read cursor: skip the null-tracking prefix here
                    // and advance it as each column is consumed below. The .length field is intentionally not
                    // maintained — only .bytes and .offset are read.
                    for (int r = 0; r < batchSize; r++) {
                        hash.get(selected.getInt(p + r), decodeKeys[r]);
                        decodeKeys[r].offset += nullTrackingBytes;
                    }
                    for (int g = 0; g < specs.size(); g++) {
                        switch (specs.get(g).elementType()) {
                            case LONG -> {
                                var b = (LongBlock.Builder) builders[g];
                                for (int r = 0; r < batchSize; r++) {
                                    final BytesRef k = decodeKeys[r];
                                    b.appendLong((long) LONG_HANDLE.get(k.bytes, k.offset));
                                    k.offset += Long.BYTES;
                                }
                            }
                            case INT -> {
                                var b = (IntBlock.Builder) builders[g];
                                for (int r = 0; r < batchSize; r++) {
                                    final BytesRef k = decodeKeys[r];
                                    b.appendInt((int) INT_HANDLE.get(k.bytes, k.offset));
                                    k.offset += Integer.BYTES;
                                }
                            }
                            case DOUBLE -> {
                                var b = (DoubleBlock.Builder) builders[g];
                                for (int r = 0; r < batchSize; r++) {
                                    final BytesRef k = decodeKeys[r];
                                    b.appendDouble((double) DOUBLE_HANDLE.get(k.bytes, k.offset));
                                    k.offset += Double.BYTES;
                                }
                            }
                            case BOOLEAN -> {
                                var b = (BooleanBlock.Builder) builders[g];
                                for (int r = 0; r < batchSize; r++) {
                                    final BytesRef k = decodeKeys[r];
                                    b.appendBoolean(k.bytes[k.offset] != 0);
                                    k.offset += Byte.BYTES;
                                }
                            }
                            case BYTES_REF -> {
                                var b = (BytesRefBlock.Builder) builders[g];
                                for (int r = 0; r < batchSize; r++) {
                                    final BytesRef k = decodeKeys[r];
                                    final int len = (int) INT_HANDLE.get(k.bytes, k.offset);
                                    bytesRefScratch.bytes = k.bytes;
                                    bytesRefScratch.offset = k.offset + Integer.BYTES;
                                    bytesRefScratch.length = len;
                                    b.appendBytesRef(bytesRefScratch);
                                    k.offset += Integer.BYTES + len;
                                }
                            }
                            default -> throw new IllegalStateException("unsupported type: " + specs.get(g).elementType());
                        }
                    }
                }
                return Block.Builder.buildAll(builders);
            } finally {
                Releasables.closeExpectNoException(builders);
            }
        }

        @Override
        public void close() {
            final long bytes = usedBytes;
            usedBytes = 0;
            blockFactory.adjustBreaker(-bytes);
            prefetchBarrier.flush();
        }
    }
}
