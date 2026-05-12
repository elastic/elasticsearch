/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.apache.lucene.util.BytesRef;
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
        for (int g = 0; g < specs.size(); g++) {
            int size = elementSize(specs.get(g).elementType());
            if (size < 0) {
                keyLength = -1;
                break;
            }
            columnOffsets[g] = keyLength;
            keyLength += size;
        }
        this.bytesRefHash = HashImplFactory.newBytesRefHash(blockFactory);
        boolean success = false;
        try {
            this.bytes = new BreakingBytesRefBuilder(circuitBreaker, "PackedValuesBlockHash", this.nullTrackingBytes);
            if (keyLength != -1 && bytesRefHash instanceof BytesRefSwissHash swiss) {
                this.batchWork = new BatchWork(swiss, columnOffsets, keyLength, emitBatchSize, blockFactory, specs);
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
        if (batchWork != null) {
            final int keyLength = batchWork.keyLength;
            final int len = bytes.length();
            if (len < keyLength) {
                bytes.grow(keyLength);
                Arrays.fill(bytes.bytes(), len, keyLength, (byte) 0);
                bytes.setLength(keyLength);
            }
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

    // TODO: Support BytesRef
    private static final class BatchWork implements Releasable {
        private static final int BATCH_SIZE = 128;
        private static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.nativeOrder());
        private static final VarHandle INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.nativeOrder());
        private static final VarHandle DOUBLE_HANDLE = MethodHandles.byteArrayViewVarHandle(double[].class, ByteOrder.nativeOrder());
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
        private long usedBytes;

        BatchWork(
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
            this.usedBytes = (long) BATCH_SIZE * keyLength + (long) emitBatchSize * Integer.BYTES + (long) BATCH_SIZE * Long.BYTES;
            blockFactory.adjustBreaker(usedBytes);
            this.keys = new byte[BATCH_SIZE * keyLength];
            this.hashes = new long[BATCH_SIZE];
            this.groupIds = new int[emitBatchSize];
            this.emitBatchSize = emitBatchSize;
        }

        void bulkAdd(Page page, GroupingAggregatorFunction.AddInput addInput) {
            int positionCount = page.getPositionCount();
            int positionOffset = 0;
            final BytesRef key = new BytesRef();
            key.bytes = keys;
            key.length = keyLength;
            int dummy = 0;
            while (positionOffset < positionCount) {
                final int emitSize = Math.min(emitBatchSize, positionCount - positionOffset);
                int batchOffset = 0;
                while (batchOffset < emitSize) {
                    final int chunkSize = Math.min(BATCH_SIZE, emitSize - batchOffset);
                    fillKeys(page, positionOffset + batchOffset, chunkSize);
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
                try (var groupIdsVec = blockFactory.newIntArrayVector(groupIds, emitSize)) {
                    addInput.add(positionOffset, groupIdsVec);
                }
                positionOffset += emitSize;
            }
            prefetchBarrier.consume(dummy);
        }

        private void fillKeys(Page page, int positionOffset, int batchSize) {
            for (int g = 0; g < specs.size(); g++) {
                final int offset = offsets[g];
                final Vector vector = page.getBlock(specs.get(g).channel()).asVector();
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

        Block[] getNonNullKeys(IntVector selected, BytesRefHashTable hash, BlockFactory blockFactory) {
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
}
