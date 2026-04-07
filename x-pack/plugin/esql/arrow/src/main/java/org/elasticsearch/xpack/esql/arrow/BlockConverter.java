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
     * @param multivalued is this column multivalued? This block may not, but some blocks in that column are.
     * @param bufs arrow buffers, used to track sizes
     * @param bufWriters buffer writers, that will do the actual work of writing the data
     */
    public abstract void convert(Block block, boolean multivalued, List<ArrowBuf> bufs, List<BufWriter> bufWriters);

    /**
     * Conversion of Double blocks
     */
    public static class AsFloat64 extends BlockConverter {

        public AsFloat64(String esqlType) {
            super(esqlType, Types.MinorType.FLOAT8);
        }

        @Override
        public void convert(Block b, boolean multivalued, List<ArrowBuf> bufs, List<BufWriter> bufWriters) {
            DoubleBlock block = (DoubleBlock) b;

            if (multivalued) {
                addListOffsets(bufs, bufWriters, block);
            }
            accumulateVectorValidity(bufs, bufWriters, block, multivalued);

            bufs.add(dummyArrowBuf(vectorByteSize(block)));
            bufWriters.add(out -> {
                if (block.areAllValuesNull()) {
                    return BlockConverter.writeZeroes(out, vectorByteSize(block));
                }

                // TODO could we "just" get the memory of the array and dump it?
                int count = BlockConverter.valueCount(block);
                for (int i = 0; i < count; i++) {
                    out.writeDoubleLE(block.getDouble(i));
                }
                return (long) count * Double.BYTES;
            });
        }

        private static int vectorByteSize(DoubleBlock b) {
            return Double.BYTES * BlockConverter.valueCount(b);
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
        public void convert(Block b, boolean multivalued, List<ArrowBuf> bufs, List<BufWriter> bufWriters) {
            IntBlock block = (IntBlock) b;

            if (multivalued) {
                addListOffsets(bufs, bufWriters, block);
            }
            accumulateVectorValidity(bufs, bufWriters, block, multivalued);

            bufs.add(dummyArrowBuf(vectorByteSize(block)));
            bufWriters.add(out -> {
                if (block.areAllValuesNull()) {
                    return BlockConverter.writeZeroes(out, vectorByteSize(block));
                }

                // TODO could we "just" get the memory of the array and dump it?
                int count = BlockConverter.valueCount(block);
                for (int i = 0; i < count; i++) {
                    out.writeIntLE(block.getInt(i));
                }
                return (long) count * Integer.BYTES;
            });
        }

        private static int vectorByteSize(Block b) {
            return Integer.BYTES * BlockConverter.valueCount(b);
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
        public void convert(Block b, boolean multivalued, List<ArrowBuf> bufs, List<BufWriter> bufWriters) {
            LongBlock block = (LongBlock) b;

            if (multivalued) {
                addListOffsets(bufs, bufWriters, block);
            }
            accumulateVectorValidity(bufs, bufWriters, block, multivalued);

            bufs.add(dummyArrowBuf(vectorByteSize(block)));
            bufWriters.add(out -> {
                if (block.areAllValuesNull()) {
                    return BlockConverter.writeZeroes(out, vectorByteSize(block));
                }

                // TODO could we "just" get the memory of the array and dump it?
                int count = BlockConverter.valueCount(block);
                for (int i = 0; i < count; i++) {
                    out.writeLongLE(block.getLong(i));
                }
                return (long) count * Long.BYTES;
            });
        }

        private static int vectorByteSize(LongBlock b) {
            return Long.BYTES * BlockConverter.valueCount(b);
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
        public void convert(Block b, boolean multivalued, List<ArrowBuf> bufs, List<BufWriter> bufWriters) {
            BooleanBlock block = (BooleanBlock) b;

            if (multivalued) {
                addListOffsets(bufs, bufWriters, block);
            }
            accumulateVectorValidity(bufs, bufWriters, block, multivalued);

            bufs.add(dummyArrowBuf(vectorByteSize(block)));
            bufWriters.add(out -> {
                int count = BlockConverter.valueCount(block);
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

        private static int vectorByteSize(BooleanBlock b) {
            return BlockConverter.bitSetLength(BlockConverter.valueCount(b));
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
        public void convert(Block b, boolean multivalued, List<ArrowBuf> bufs, List<BufWriter> bufWriters) {
            BytesRefBlock block = (BytesRefBlock) b;

            if (multivalued) {
                addListOffsets(bufs, bufWriters, block);
            }
            accumulateVectorValidity(bufs, bufWriters, block, multivalued);

            // Offsets vector
            bufs.add(dummyArrowBuf(offsetvectorByteSize(block)));

            bufWriters.add(out -> {
                if (block.areAllValuesNull()) {
                    var count = valueCount(block) + 1;
                    for (int i = 0; i < count; i++) {
                        out.writeIntLE(0);
                    }
                    return offsetvectorByteSize(block);
                }

                // TODO could we "just" get the memory of the array and dump it?
                BytesRef scratch = new BytesRef();
                int offset = 0;
                for (int i = 0; i < valueCount(block); i++) {
                    out.writeIntLE(offset);
                    // FIXME: add a ByteRefsVector.getLength(position): there are some cases
                    // where getBytesRef will allocate, which isn't needed here.
                    BytesRef v = block.getBytesRef(i, scratch);

                    offset += v.length;
                }
                out.writeIntLE(offset);
                return offsetvectorByteSize(block);
            });

            // Data vector
            bufs.add(BlockConverter.dummyArrowBuf(dataVectorByteSize(block)));

            bufWriters.add(out -> {
                if (block.areAllValuesNull()) {
                    return 0;
                }

                // TODO could we "just" get the memory of the array and dump it?
                BytesRef scratch = new BytesRef();
                long length = 0;
                for (int i = 0; i < valueCount(block); i++) {
                    BytesRef v = block.getBytesRef(i, scratch);

                    out.write(v.bytes, v.offset, v.length);
                    length += v.length;
                }
                return length;
            });
        }

        private static int offsetvectorByteSize(BytesRefBlock block) {
            return Integer.BYTES * (valueCount(block) + 1);
        }

        private int dataVectorByteSize(BytesRefBlock block) {
            if (block.areAllValuesNull()) {
                return 0;
            }

            // TODO we can probably get the length from the vector without all this sum

            int length = 0;
            BytesRef scratch = new BytesRef();
            for (int i = 0; i < valueCount(block); i++) {
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
        public void convert(Block b, boolean multivalued, List<ArrowBuf> bufs, List<BufWriter> bufWriters) {
            BytesRefBlock block = (BytesRefBlock) b;
            try (BytesRefBlock transformed = transformValues(block)) {
                super.convert(transformed, multivalued, bufs, bufWriters);
            }
        }

        /**
         * Creates a new BytesRefBlock by applying the value converter to each non null and non empty value
         */
        private BytesRefBlock transformValues(BytesRefBlock block) {
            try (BytesRefBlock.Builder builder = block.blockFactory().newBytesRefBlockBuilder(block.getPositionCount())) {
                BytesRef scratch = new BytesRef();
                if (block.mayHaveMultivaluedFields() == false) {
                    for (int pos = 0; pos < valueCount(block); pos++) {
                        if (block.isNull(pos)) {
                            builder.appendNull();
                        } else {
                            convertAndAppend(builder, block, pos, scratch);
                        }
                    }
                } else {
                    for (int pos = 0; pos < block.getPositionCount(); pos++) {
                        if (block.isNull(pos)) {
                            builder.appendNull();
                        } else {
                            builder.beginPositionEntry();
                            int startPos = block.getFirstValueIndex(pos);
                            int lastPos = block.getFirstValueIndex(pos + 1);
                            for (int valuePos = startPos; valuePos < lastPos; valuePos++) {
                                convertAndAppend(builder, block, valuePos, scratch);
                            }
                            builder.endPositionEntry();
                        }
                    }
                }
                return builder.build();
            }
        }

        private void convertAndAppend(BytesRefBlock.Builder builder, BytesRefBlock block, int position, BytesRef scratch) {
            BytesRef bytes = block.getBytesRef(position, scratch);
            if (bytes.length != 0) {
                bytes = valueConverter.apply(bytes, scratch);
            }
            builder.appendBytesRef(bytes);
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
        public void convert(Block block, boolean multivalued, List<ArrowBuf> bufs, List<BufWriter> bufWriters) {
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

    /**
     * Get the value count for a block. For single-valued blocks this is the same as the position count.
     * For multivalued blocks, this is the flattened number of items.
     */
    static int valueCount(Block block) {
        int result = block.getFirstValueIndex(block.getPositionCount());

        // firstValueIndex is always zero for all-null blocks.
        if (result == 0 && block.areAllValuesNull()) {
            result = block.getPositionCount();
        }

        return result;
    }

    private static void accumulateVectorValidity(List<ArrowBuf> bufs, List<BufWriter> bufWriters, Block b, boolean multivalued) {
        // If that block is in a multivalued-column, validities are output in the parent Arrow List buffer (values themselves
        // do not contain nulls per docvalues limitations).
        if (multivalued || b.mayHaveNulls() == false) {
            // Arrow IPC allows a compact form for "all true" validities using an empty buffer.
            bufs.add(dummyArrowBuf(0));
            bufWriters.add(w -> 0);
            return;
        }

        int valueCount = b.getPositionCount();
        bufs.add(dummyArrowBuf(bitSetLength(valueCount)));
        bufWriters.add(out -> {
            if (b.areAllValuesNull()) {
                return writeAllFalseValidity(out, valueCount);
            } else {
                return writeValidities(out, b, valueCount);
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

    private static long writeValidities(RecyclerBytesStreamOutput out, Block block, int valueCount) {
        BitSet bits = new BitSet(valueCount);
        for (int i = 0; i < block.getPositionCount(); i++) {
            // isNull is value indices, not multi-value positions
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

    private static void addListOffsets(List<ArrowBuf> bufs, List<BufWriter> bufWriters, Block block) {
        // Add validity buffer
        accumulateVectorValidity(bufs, bufWriters, block, false);

        // Add offsets buffer
        int bufferLen = Integer.BYTES * (block.getPositionCount() + 1);

        bufs.add(dummyArrowBuf(bufferLen));
        bufWriters.add(out -> {
            if (block.mayHaveMultivaluedFields()) {
                // '<=' is intentional to write the end position of the last item
                for (int i = 0; i <= block.getPositionCount(); i++) {
                    // TODO could we get the block's firstValueIndexes and dump it?
                    out.writeIntLE(block.getFirstValueIndex(i));
                }
            } else {
                for (int i = 0; i <= block.getPositionCount(); i++) {
                    out.writeIntLE(i);
                }
            }

            return bufferLen;
        });
    }
}
