/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.arrow;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;

import java.io.IOException;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

public abstract class BlockConverter {

    private final FieldType fieldType;
    private final String esqlType;

    protected BlockConverter(String esqlType, Types.MinorType minorType) {
        // Add the exact ESQL type as field metadata
        var meta = Map.of("elastic:type", esqlType);
        this.fieldType = new FieldType(true, minorType.getType(), null, meta);
        this.esqlType = esqlType;
    }

    public final String esqlType() {
        return this.esqlType;
    }

    public final FieldType arrowFieldType() {
        return this.fieldType;
    }

    // Block.nullValuesCount was more efficient but was removed in https://github.com/elastic/elasticsearch/pull/108916
    protected int nullValuesCount(Block block) {
        if (block.mayHaveNulls() == false) {
            return 0;
        }

        if (block.areAllValuesNull()) {
            return block.getPositionCount();
        }

        int count = 0;
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (block.isNull(i)) {
                count++;
            }
        }
        return count;
    }

    public interface BufWriter {
        long write(RecyclerBytesStreamOutput out) throws IOException;
    }

    /**
     * Convert a block into Arrow buffers.
     * @param block the ESQL block
     * @param bufs arrow buffers, used to track sizes
     * @param bufWriters buffer writers, that will do the actual work of writing the data
     */
    public abstract void convert(Block block, List<ArrowBuf> bufs, List<BufWriter> bufWriters);

    /**
     * Conversion of Double blocks
     */
    public static class AsFloat64 extends BlockConverter {

        public AsFloat64(String esqlType) {
            super(esqlType, Types.MinorType.FLOAT8);
        }

        @Override
        public void convert(Block b, List<ArrowBuf> bufs, List<BufWriter> bufWriters) {
            DoubleBlock block = (DoubleBlock) b;

            accumulateVectorValidity(bufs, bufWriters, block);

            bufs.add(dummyArrowBuf(vectorLength(block)));
            bufWriters.add(out -> {
                if (block.areAllValuesNull()) {
                    return BlockConverter.writeZeroes(out, vectorLength(block));
                }

                // TODO could we "just" get the memory of the array and dump it?
                int count = block.getPositionCount();
                for (int i = 0; i < count; i++) {
                    out.writeDoubleLE(block.getDouble(i));
                }
                return vectorLength(block);
            });
        }

        private static int vectorLength(DoubleBlock b) {
            return Double.BYTES * b.getPositionCount();
        }
    }

    /**
     * Conversion of Int blocks
     */
    public static class AsInt32 extends BlockConverter {

        public AsInt32(String esqlType) {
            super(esqlType, Types.MinorType.INT);
        }

        @Override
        public void convert(Block b, List<ArrowBuf> bufs, List<BufWriter> bufWriters) {
            IntBlock block = (IntBlock) b;

            accumulateVectorValidity(bufs, bufWriters, block);

            bufs.add(dummyArrowBuf(vectorLength(block)));
            bufWriters.add(out -> {
                if (block.areAllValuesNull()) {
                    return BlockConverter.writeZeroes(out, vectorLength(block));
                }

                // TODO could we "just" get the memory of the array and dump it?
                int count = block.getPositionCount();
                for (int i = 0; i < count; i++) {
                    out.writeIntLE(block.getInt(i));
                }
                return vectorLength(block);
            });
        }

        private static int vectorLength(IntBlock b) {
            return Integer.BYTES * b.getPositionCount();
        }
    }

    /**
     * Conversion of Long blocks
     */
    public static class AsInt64 extends BlockConverter {
        public AsInt64(String esqlType) {
            this(esqlType, Types.MinorType.BIGINT);
        }

        protected AsInt64(String esqlType, Types.MinorType minorType) {
            super(esqlType, minorType);
        }

        @Override
        public void convert(Block b, List<ArrowBuf> bufs, List<BufWriter> bufWriters) {
            LongBlock block = (LongBlock) b;
            accumulateVectorValidity(bufs, bufWriters, block);

            bufs.add(dummyArrowBuf(vectorLength(block)));
            bufWriters.add(out -> {
                if (block.areAllValuesNull()) {
                    return BlockConverter.writeZeroes(out, vectorLength(block));
                }

                // TODO could we "just" get the memory of the array and dump it?
                int count = block.getPositionCount();
                for (int i = 0; i < count; i++) {
                    out.writeLongLE(block.getLong(i));
                }
                return vectorLength(block);
            });
        }

        private static int vectorLength(LongBlock b) {
            return Long.BYTES * b.getPositionCount();
        }
    }

    /**
     * Conversion of Boolean blocks
     */
    public static class AsBoolean extends BlockConverter {
        public AsBoolean(String esqlType) {
            super(esqlType, Types.MinorType.BIT);
        }

        @Override
        public void convert(Block b, List<ArrowBuf> bufs, List<BufWriter> bufWriters) {
            BooleanBlock block = (BooleanBlock) b;
            accumulateVectorValidity(bufs, bufWriters, block);

            bufs.add(dummyArrowBuf(vectorLength(block)));
            bufWriters.add(out -> {
                int count = block.getPositionCount();
                BitSet bits = new BitSet();

                // Only set the bits that are true, writeBitSet will take
                // care of adding zero bytes if needed.
                if (block.areAllValuesNull() == false) {
                    for (int i = 0; i < count; i++) {
                        if (block.getBoolean(i)) {
                            bits.set(i);
                        }
                    }
                }

                return BlockConverter.writeBitSet(out, bits, count);
            });
        }

        private static int vectorLength(BooleanBlock b) {
            return BlockConverter.bitSetLength(b.getPositionCount());
        }
    }

    /**
     * Conversion of ByteRef blocks
     */
    public static class BytesRefConverter extends BlockConverter {

        public BytesRefConverter(String esqlType, Types.MinorType minorType) {
            super(esqlType, minorType);
        }

        @Override
        public void convert(Block b, List<ArrowBuf> bufs, List<BufWriter> bufWriters) {
            BytesRefBlock block = (BytesRefBlock) b;

            BlockConverter.accumulateVectorValidity(bufs, bufWriters, block);

            // Offsets vector
            bufs.add(dummyArrowBuf(offsetVectorLength(block)));

            bufWriters.add(out -> {
                if (block.areAllValuesNull()) {
                    var count = block.getPositionCount() + 1;
                    for (int i = 0; i < count; i++) {
                        out.writeIntLE(0);
                    }
                    return offsetVectorLength(block);
                }

                // TODO could we "just" get the memory of the array and dump it?
                BytesRef scratch = new BytesRef();
                int offset = 0;
                for (int i = 0; i < block.getPositionCount(); i++) {
                    out.writeIntLE(offset);
                    // FIXME: add a ByteRefsVector.getLength(position): there are some cases
                    // where getBytesRef will allocate, which isn't needed here.
                    BytesRef v = block.getBytesRef(i, scratch);

                    offset += v.length;
                }
                out.writeIntLE(offset);
                return offsetVectorLength(block);
            });

            // Data vector
            bufs.add(BlockConverter.dummyArrowBuf(dataVectorLength(block)));

            bufWriters.add(out -> {
                if (block.areAllValuesNull()) {
                    return 0;
                }

                // TODO could we "just" get the memory of the array and dump it?
                BytesRef scratch = new BytesRef();
                long length = 0;
                for (int i = 0; i < block.getPositionCount(); i++) {
                    BytesRef v = block.getBytesRef(i, scratch);

                    out.write(v.bytes, v.offset, v.length);
                    length += v.length;
                }
                return length;
            });
        }

        private static int offsetVectorLength(BytesRefBlock block) {
            return Integer.BYTES * (block.getPositionCount() + 1);
        }

        private int dataVectorLength(BytesRefBlock block) {
            if (block.areAllValuesNull()) {
                return 0;
            }

            // TODO we can probably get the length from the vector without all this sum

            int length = 0;
            BytesRef scratch = new BytesRef();
            for (int i = 0; i < block.getPositionCount(); i++) {
                BytesRef v = block.getBytesRef(i, scratch);
                length += v.length;
            }
            return length;
        }
    }

    /**
     * Conversion of ByteRefs where each value is itself converted to a different format.
     */
    public static class TransformedBytesRef extends BytesRefConverter {

        private final BiFunction<BytesRef, BytesRef, BytesRef> valueConverter;

        /**
         *
         * @param esqlType ESQL type name
         * @param minorType Arrow type
         * @param valueConverter a function that takes (value, scratch) input parameters and returns the transformed value
         */
        public TransformedBytesRef(String esqlType, Types.MinorType minorType, BiFunction<BytesRef, BytesRef, BytesRef> valueConverter) {
            super(esqlType, minorType);
            this.valueConverter = valueConverter;
        }

        @Override
        public void convert(Block b, List<ArrowBuf> bufs, List<BufWriter> bufWriters) {
            BytesRefBlock block = (BytesRefBlock) b;
            try (BytesRefBlock transformed = transformValues(block)) {
                super.convert(transformed, bufs, bufWriters);
            }
        }

        /**
         * Creates a new BytesRefBlock by applying the value converter to each non null and non empty value
         */
        private BytesRefBlock transformValues(BytesRefBlock block) {
            try (BytesRefBlock.Builder builder = block.blockFactory().newBytesRefBlockBuilder(block.getPositionCount())) {
                BytesRef scratch = new BytesRef();
                for (int i = 0; i < block.getPositionCount(); i++) {
                    if (block.isNull(i)) {
                        builder.appendNull();
                    } else {
                        BytesRef bytes = block.getBytesRef(i, scratch);
                        if (bytes.length != 0) {
                            bytes = valueConverter.apply(bytes, scratch);
                        }
                        builder.appendBytesRef(bytes);
                    }
                }
                return builder.build();
            }
        }
    }

    public static class AsVarChar extends BytesRefConverter {
        public AsVarChar(String esqlType) {
            super(esqlType, Types.MinorType.VARCHAR);
        }
    }

    public static class AsVarBinary extends BytesRefConverter {
        public AsVarBinary(String esqlType) {
            super(esqlType, Types.MinorType.VARBINARY);
        }
    }

    public static class AsNull extends BlockConverter {
        public AsNull(String esqlType) {
            super(esqlType, Types.MinorType.NULL);
        }

        @Override
        public void convert(Block block, List<ArrowBuf> bufs, List<BufWriter> bufWriters) {
            // Null vector in arrow has no associated buffers
            // See https://arrow.apache.org/docs/format/Columnar.html#null-layout
        }
    }

    // Create a dummy ArrowBuf used for size accounting purposes.
    private static ArrowBuf dummyArrowBuf(long size) {
        return new ArrowBuf(null, null, 0, 0).writerIndex(size);
    }

    // Length in bytes of a validity buffer
    private static int bitSetLength(int totalValues) {
        return (totalValues + 7) / 8;
    }

    private static void accumulateVectorValidity(List<ArrowBuf> bufs, List<BufWriter> bufWriters, Block b) {
        bufs.add(dummyArrowBuf(bitSetLength(b.getPositionCount())));
        bufWriters.add(out -> {
            if (b.mayHaveNulls() == false) {
                return writeAllTrueValidity(out, b.getPositionCount());
            } else if (b.areAllValuesNull()) {
                return writeAllFalseValidity(out, b.getPositionCount());
            } else {
                return writeValidities(out, b);
            }
        });
    }

    private static long writeAllTrueValidity(RecyclerBytesStreamOutput out, int valueCount) {
        int allOnesCount = valueCount / 8;
        for (int i = 0; i < allOnesCount; i++) {
            out.writeByte((byte) 0xff);
        }
        int remaining = valueCount % 8;
        if (remaining == 0) {
            return allOnesCount;
        }
        out.writeByte((byte) ((1 << remaining) - 1));
        return allOnesCount + 1;
    }

    private static long writeAllFalseValidity(RecyclerBytesStreamOutput out, int valueCount) {
        int count = bitSetLength(valueCount);
        for (int i = 0; i < count; i++) {
            out.writeByte((byte) 0x00);
        }
        return count;
    }

    private static long writeValidities(RecyclerBytesStreamOutput out, Block block) {
        int valueCount = block.getPositionCount();
        BitSet bits = new BitSet(valueCount);
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (block.isNull(i) == false) {
                bits.set(i);
            }
        }
        return writeBitSet(out, bits, valueCount);
    }

    private static long writeBitSet(RecyclerBytesStreamOutput out, BitSet bits, int bitCount) {
        byte[] bytes = bits.toByteArray();
        out.writeBytes(bytes, 0, bytes.length);

        // toByteArray will return bytes up to the last bit set. It may therefore
        // have a length lower than what is needed to actually store bitCount bits.
        int expectedLength = bitSetLength(bitCount);
        writeZeroes(out, expectedLength - bytes.length);

        return expectedLength;
    }

    private static long writeZeroes(RecyclerBytesStreamOutput out, int byteCount) {
        for (int i = 0; i < byteCount; i++) {
            out.writeByte((byte) 0);
        }
        return byteCount;
    }
}
