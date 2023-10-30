/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.BlockMultiValueDeduplicator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.HashAggregationOperator;
import org.elasticsearch.compute.operator.MultivalueDedupeBoolean;
import org.elasticsearch.compute.operator.MultivalueDedupeBytesRef;
import org.elasticsearch.compute.operator.MultivalueDedupeDouble;
import org.elasticsearch.compute.operator.MultivalueDedupeInt;
import org.elasticsearch.compute.operator.MultivalueDedupeLong;
import org.elasticsearch.core.Releasables;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;

/**
 * Maps any number of columns to a group ids with every unique combination resulting
 * in a unique group id. Works by uniqing the values of each column and concatenating
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
 */
final class PackedValuesBlockHash extends BlockHash {

    private final int emitBatchSize;
    private final BytesRefHash bytesRefHash;
    private final int nullTrackingBytes;
    private final BytesRefBuilder bytes = new BytesRefBuilder();
    private final Group[] groups;

    PackedValuesBlockHash(List<HashAggregationOperator.GroupSpec> specs, DriverContext driverContext, int emitBatchSize) {
        super(driverContext);
        this.groups = specs.stream().map(Group::new).toArray(Group[]::new);
        this.emitBatchSize = emitBatchSize;
        this.bytesRefHash = new BytesRefHash(1, bigArrays);
        this.nullTrackingBytes = (groups.length + 7) / 8;
    }

    @Override
    public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
        try (AddWork work = new AddWork(page, addInput)) {
            work.add();
        }
    }

    private static class Group {
        final HashAggregationOperator.GroupSpec spec;
        Encoder encoder;
        int valueOffset;
        int valueCount;
        int bytesStart;

        Group(HashAggregationOperator.GroupSpec spec) {
            this.spec = spec;
        }
    }

    class AddWork extends LongLongBlockHash.AbstractAddBlock {
        final int positionCount;
        int position;

        AddWork(Page page, GroupingAggregatorFunction.AddInput addInput) {
            super(blockFactory, emitBatchSize, addInput);
            for (Group group : groups) {
                group.encoder = newEncoder(page.getBlock(group.spec.channel()));
            }
            bytes.grow(nullTrackingBytes);
            this.positionCount = page.getPositionCount();
        }

        /**
         * Encodes one permutation of the keys at time into {@link #bytes}. The encoding is
         * mostly provided by {@link Encoder} with nulls living in a bit mask at the
         * front of the bytes.
         */
        void add() {
            for (position = 0; position < positionCount; position++) {
                // Make sure all encoders have encoded the current position and the offsets are queued to it's start
                boolean singleEntry = true;
                for (Group g : groups) {
                    var encoder = g.encoder;
                    encoder.moveToPosition(position);
                    g.valueCount = encoder.valueCount();
                    singleEntry &= (g.valueCount == 1);
                }
                Arrays.fill(bytes.bytes(), 0, nullTrackingBytes, (byte) 0);
                bytes.setLength(nullTrackingBytes);
                if (singleEntry) {
                    addSingleEntry();
                } else {
                    addMultipleEntries();
                }
            }
            emitOrds();
        }

        private void addSingleEntry() {
            for (int g = 0; g < groups.length; g++) {
                Group group = groups[g];
                if (group.encoder.read(0, bytes) == 0) {
                    int nullByte = g / 8;
                    int nullShift = g % 8;
                    bytes.bytes()[nullByte] |= (byte) (1 << nullShift);
                }
            }
            int ord = Math.toIntExact(hashOrdToGroup(bytesRefHash.add(bytes.get())));
            ords.appendInt(ord);
            addedValue(position);
        }

        private void addMultipleEntries() {
            ords.beginPositionEntry();
            int g = 0;
            outer: for (;;) {
                for (; g < groups.length; g++) {
                    Group group = groups[g];
                    group.bytesStart = bytes.length();
                    if (group.encoder.read(group.valueOffset, bytes) == 0) {
                        assert group.valueCount == 1 : "null value in non-singleton list";
                        int nullByte = g / 8;
                        int nullShift = g % 8;
                        bytes.bytes()[nullByte] |= (byte) (1 << nullShift);
                    }
                    ++group.valueOffset;
                }
                // emit ords
                int ord = Math.toIntExact(hashOrdToGroup(bytesRefHash.add(bytes.get())));
                ords.appendInt(ord);
                addedValueInMultivaluePosition(position);

                // rewind
                Group group = groups[--g];
                bytes.setLength(group.bytesStart);
                while (group.valueOffset == group.valueCount) {
                    group.valueOffset = 0;
                    if (g == 0) {
                        break outer;
                    } else {
                        group = groups[--g];
                        bytes.setLength(group.bytesStart);
                    }
                }
            }
            ords.endPositionEntry();
        }
    }

    @Override
    public Block[] getKeys() {
        int size = Math.toIntExact(bytesRefHash.size());
        Decoder[] decoders = new Decoder[groups.length];
        Block.Builder[] builders = new Block.Builder[groups.length];
        try {
            for (int g = 0; g < builders.length; g++) {
                ElementType elementType = groups[g].spec.elementType();
                decoders[g] = newDecoder(elementType);
                builders[g] = elementType.newBlockBuilder(size, blockFactory);
            }

            BytesRef[] values = new BytesRef[(int) Math.min(100, bytesRefHash.size())];
            BytesRef[] nulls = new BytesRef[values.length];
            for (int offset = 0; offset < values.length; offset++) {
                values[offset] = new BytesRef();
                nulls[offset] = new BytesRef();
                nulls[offset].length = nullTrackingBytes;
            }
            int offset = 0;
            for (int i = 0; i < bytesRefHash.size(); i++) {
                values[offset] = bytesRefHash.get(i, values[offset]);

                // Reference the null bytes in the nulls array and values in the values
                nulls[offset].bytes = values[offset].bytes;
                nulls[offset].offset = values[offset].offset;
                values[offset].offset += nullTrackingBytes;
                values[offset].length -= nullTrackingBytes;

                offset++;
                if (offset == values.length) {
                    readKeys(decoders, builders, nulls, values, offset);
                    offset = 0;
                }
            }
            if (offset > 0) {
                readKeys(decoders, builders, nulls, values, offset);
            }

            Block[] keyBlocks = new Block[groups.length];
            try {
                for (int g = 0; g < keyBlocks.length; g++) {
                    keyBlocks[g] = builders[g].build();
                }
            } finally {
                if (keyBlocks[keyBlocks.length - 1] == null) {
                    Releasables.closeExpectNoException(keyBlocks);
                }
            }
            return keyBlocks;
        } finally {
            Releasables.closeExpectNoException(builders);
        }
    }

    private void readKeys(Decoder[] decoders, Block.Builder[] builders, BytesRef[] nulls, BytesRef[] values, int count) {
        for (int g = 0; g < builders.length; g++) {
            int nullByte = g / 8;
            int nullShift = g % 8;
            byte nullTest = (byte) (1 << nullShift);
            IsNull isNull = offset -> {
                BytesRef n = nulls[offset];
                return (n.bytes[n.offset + nullByte] & nullTest) != 0;
            };
            decoders[g].decode(builders[g], isNull, values, count);
        }
    }

    @Override
    public IntVector nonEmpty() {
        return IntVector.range(0, Math.toIntExact(bytesRefHash.size()), blockFactory);
    }

    @Override
    public BitArray seenGroupIds(BigArrays bigArrays) {
        return new SeenGroupIds.Range(0, Math.toIntExact(bytesRefHash.size())).seenGroupIds(bigArrays);
    }

    @Override
    public void close() {
        bytesRefHash.close();
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("PackedValuesBlockHash{groups=[");
        for (int i = 0; i < groups.length; i++) {
            if (i > 0) {
                b.append(", ");
            }
            Group group = groups[i];
            b.append(group.spec.channel()).append(':').append(group.spec.elementType());
        }
        b.append("], entries=").append(bytesRefHash.size());
        b.append(", size=").append(ByteSizeValue.ofBytes(bytesRefHash.ramBytesUsed()));
        return b.append("}").toString();
    }

    /**
     * Checks if an offset is {@code null}.
     */
    interface IsNull {
        boolean isNull(int offset);
    }

    /**
     * Decodes values encoded by {@link Encoder}.
     */
    interface Decoder {
        void decode(Block.Builder builder, IsNull isNull, BytesRef[] encoded, int count);
    }

    /**
     * Get a {@link Decoder} for the provided {@link ElementType}.
     */
    static Decoder newDecoder(ElementType elementType) {
        return switch (elementType) {
            case INT -> new IntsDecoder();
            case LONG -> new LongsDecoder();
            case DOUBLE -> new DoublesDecoder();
            case BYTES_REF -> new BytesRefsDecoder();
            case BOOLEAN -> new BooleansDecoder();
            default -> throw new IllegalArgumentException("can't decode " + elementType);
        };
    }

    abstract static class Encoder implements Accountable {
        static final long BASE_RAM_USAGE = RamUsageEstimator.shallowSizeOfInstance(Encoder.class);
        final BlockMultiValueDeduplicator deduplicator;

        Encoder(BlockMultiValueDeduplicator deduplicator) {
            this.deduplicator = deduplicator;
        }

        /**
         * The number of values at the current position after {@link #moveToPosition(int)}
         */
        final int valueCount() {
            return Math.max(deduplicator.valueCount(), 1);
        }

        /**
         * Read the value at the specified offset then append to the {@code dst}.
         *
         * @return the number of bytes has read
         */
        final int read(int offset, BytesRefBuilder dst) {
            if (deduplicator.valueCount() == 0) {
                assert offset == 0 : offset;
                return 0;
            } else {
                return doRead(offset, dst);
            }
        }

        abstract int doRead(int offset, BytesRefBuilder dst);

        /**
         * Advances the encoder to the given position
         */
        final void moveToPosition(int position) {
            deduplicator.moveToPosition(position);
        }

        @Override
        public final long ramBytesUsed() {
            return BASE_RAM_USAGE;
        }
    }

    /**
     * Get a {@link Encoder} for the provided {@link Block}.
     */
    static Encoder newEncoder(Block block) {
        if (block.areAllValuesNull()) {
            return new NullsEncoder(block);
        }
        final ElementType elementType = block.elementType();
        return switch (elementType) {
            case INT -> new IntsEncoder((IntBlock) block);
            case LONG -> new LongsEncoder((LongBlock) block);
            case DOUBLE -> new DoublesEncoder((DoubleBlock) block);
            case BYTES_REF -> new BytesRefsEncoder((BytesRefBlock) block);
            case BOOLEAN -> new BooleansEncoder((BooleanBlock) block);
            default -> throw new IllegalArgumentException("can't encode " + elementType);
        };
    }

    private static final VarHandle intHandle = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.nativeOrder());

    private static class IntsDecoder implements Decoder {
        @Override
        public void decode(Block.Builder builder, IsNull isNull, BytesRef[] encoded, int count) {
            IntBlock.Builder b = (IntBlock.Builder) builder;
            for (int i = 0; i < count; i++) {
                if (isNull.isNull(i)) {
                    b.appendNull();
                } else {
                    BytesRef e = encoded[i];
                    b.appendInt((int) intHandle.get(e.bytes, e.offset));
                    e.offset += Integer.BYTES;
                    e.length -= Integer.BYTES;
                }
            }
        }
    }

    private static class IntsEncoder extends Encoder {
        IntsEncoder(IntBlock block) {
            super(new MultivalueDedupeInt(Block.Ref.floating(block)).getDeduplicator());
        }

        @Override
        protected int doRead(int offset, BytesRefBuilder dst) {
            int before = dst.length();
            int after = before + Integer.BYTES;
            dst.grow(after);
            int v = ((MultivalueDedupeInt.Deduplicator) deduplicator).getInt(offset);
            intHandle.set(dst.bytes(), before, v);
            dst.setLength(after);
            return Integer.BYTES;
        }
    }

    private static final VarHandle longHandle = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.nativeOrder());

    private static class LongsEncoder extends Encoder {
        LongsEncoder(LongBlock block) {
            super(new MultivalueDedupeLong(Block.Ref.floating(block)).getDeduplicator());
        }

        @Override
        protected int doRead(int valueIndex, BytesRefBuilder dst) {
            int before = dst.length();
            int after = before + Long.BYTES;
            dst.grow(after);
            long v = ((MultivalueDedupeLong.Deduplicator) deduplicator).getLong(valueIndex);
            longHandle.set(dst.bytes(), before, v);
            dst.setLength(after);
            return Long.BYTES;
        }
    }

    private static class LongsDecoder implements Decoder {
        @Override
        public void decode(Block.Builder builder, IsNull isNull, BytesRef[] encoded, int count) {
            LongBlock.Builder b = (LongBlock.Builder) builder;
            for (int i = 0; i < count; i++) {
                if (isNull.isNull(i)) {
                    b.appendNull();
                } else {
                    BytesRef e = encoded[i];
                    b.appendLong((long) longHandle.get(e.bytes, e.offset));
                    e.offset += Long.BYTES;
                    e.length -= Long.BYTES;
                }
            }
        }

    }

    private static final VarHandle doubleHandle = MethodHandles.byteArrayViewVarHandle(double[].class, ByteOrder.nativeOrder());

    private static class DoublesEncoder extends Encoder {
        DoublesEncoder(DoubleBlock block) {
            super(new MultivalueDedupeDouble(Block.Ref.floating(block)).getDeduplicator());
        }

        @Override
        protected int doRead(int offset, BytesRefBuilder dst) {
            int before = dst.length();
            int after = before + Double.BYTES;
            dst.grow(after);
            double v = ((MultivalueDedupeDouble.Deduplicator) deduplicator).getDouble(offset);
            doubleHandle.set(dst.bytes(), before, v);
            dst.setLength(after);
            return Double.BYTES;
        }
    }

    private static class DoublesDecoder implements Decoder {
        @Override
        public void decode(Block.Builder builder, IsNull isNull, BytesRef[] encoded, int count) {
            DoubleBlock.Builder b = (DoubleBlock.Builder) builder;
            for (int i = 0; i < count; i++) {
                if (isNull.isNull(i)) {
                    b.appendNull();
                } else {
                    BytesRef e = encoded[i];
                    b.appendDouble((double) doubleHandle.get(e.bytes, e.offset));
                    e.offset += Double.BYTES;
                    e.length -= Double.BYTES;
                }
            }
        }
    }

    private static class BooleansEncoder extends Encoder {
        BooleansEncoder(BooleanBlock block) {
            super(new MultivalueDedupeBoolean(Block.Ref.floating(block)).getDeduplicator());
        }

        @Override
        protected int doRead(int offset, BytesRefBuilder dst) {
            var v = ((MultivalueDedupeBoolean.Deduplicator) deduplicator).getBoolean(offset);
            dst.append((byte) (v ? 1 : 0));
            return 1;
        }
    }

    private static class BooleansDecoder implements Decoder {
        @Override
        public void decode(Block.Builder builder, IsNull isNull, BytesRef[] encoded, int count) {
            BooleanBlock.Builder b = (BooleanBlock.Builder) builder;
            for (int i = 0; i < count; i++) {
                if (isNull.isNull(i)) {
                    b.appendNull();
                } else {
                    BytesRef e = encoded[i];
                    b.appendBoolean(e.bytes[e.offset] == 1);
                    e.offset++;
                    e.length--;
                }
            }
        }
    }

    private static class BytesRefsEncoder extends Encoder {
        private final BytesRef scratch = new BytesRef();

        BytesRefsEncoder(BytesRefBlock block) {
            super(new MultivalueDedupeBytesRef(Block.Ref.floating(block)).getDeduplicator());
        }

        @Override
        protected int doRead(int offset, BytesRefBuilder dst) {
            var v = ((MultivalueDedupeBytesRef.Deduplicator) deduplicator).getBytesRef(offset, scratch);
            int start = dst.length();
            dst.grow(start + Integer.BYTES + v.length);
            intHandle.set(dst.bytes(), start, v.length);
            dst.setLength(start + Integer.BYTES);
            dst.append(v);
            return Integer.BYTES + v.length;
        }
    }

    private static class BytesRefsDecoder implements Decoder {
        @Override
        public void decode(Block.Builder builder, IsNull isNull, BytesRef[] encoded, int count) {
            BytesRef scratch = new BytesRef();
            BytesRefBlock.Builder b = (BytesRefBlock.Builder) builder;
            for (int i = 0; i < count; i++) {
                if (isNull.isNull(i)) {
                    b.appendNull();
                } else {
                    BytesRef e = encoded[i];
                    scratch.bytes = e.bytes;
                    scratch.length = (int) intHandle.get(e.bytes, e.offset);
                    e.offset += Integer.BYTES;
                    e.length -= Integer.BYTES;
                    scratch.offset = e.offset;
                    b.appendBytesRef(scratch);
                    e.offset += scratch.length;
                    e.length -= scratch.length;
                }
            }
        }
    }

    private record NullBlockDeduplicator(Block block) implements BlockMultiValueDeduplicator {
        @Override
        public void moveToPosition(int position) {
            assert position <= block.getPositionCount() : position + " >= " + block.getPositionCount();
        }

        @Override
        public int valueCount() {
            return 1;
        }
    }

    private static final class NullsEncoder extends Encoder {
        NullsEncoder(Block block) {
            super(new NullBlockDeduplicator(block));
            assert block.areAllValuesNull() : block;
        }

        @Override
        protected int doRead(int offset, BytesRefBuilder dst) {
            return 0;
        }
    }
}
