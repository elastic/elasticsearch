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
import org.elasticsearch.core.Releasable;

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
     * @param releasables resources retained until the deferred buffer writers have completed
     */
    public abstract void convert(
        Block block,
        boolean multivalued,
        List<ArrowBuf> bufs,
        List<BufWriter> bufWriters,
        List<Releasable> releasables
    );

    /**
     * Conversion of Double blocks
     */
    public static class AsFloat64 extends BlockConverter {

        public AsFloat64(String esqlType) {
            super(esqlType, Types.MinorType.FLOAT8);
        }

        @Override
        public void convert(Block b, boolean multivalued, List<ArrowBuf> bufs, List<BufWriter> bufWriters, List<Releasable> releasables) {
            DoubleBlock block = (DoubleBlock) b;

            if (multivalued) {
                addListOffsets(bufs, bufWriters, block);
            }
            accumulateVectorValidity(bufs, bufWriters, block, multivalued);

            bufs.add(dummyArrowBuf(vectorByteSize(block, multivalued)));
            bufWriters.add(out -> {
                // TODO could we "just" get the memory of the array and dump it?
                int count = arrowValueCount(block, multivalued);
                for (int position = 0; position < block.getPositionCount(); position++) {
                    if (block.isNull(position)) {
                        if (multivalued == false) {
                            out.writeDoubleLE(0);
                        }
                        continue;
                    }
                    int start = block.getFirstValueIndex(position);
                    int end = start + block.getValueCount(position);
                    for (int valueIndex = start; valueIndex < end; valueIndex++) {
                        out.writeDoubleLE(block.getDouble(valueIndex));
                    }
                }
                return (long) count * Double.BYTES;
            });
        }

        private static int vectorByteSize(DoubleBlock b, boolean multivalued) {
            return Double.BYTES * arrowValueCount(b, multivalued);
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
        public void convert(Block b, boolean multivalued, List<ArrowBuf> bufs, List<BufWriter> bufWriters, List<Releasable> releasables) {
            IntBlock block = (IntBlock) b;

            if (multivalued) {
                addListOffsets(bufs, bufWriters, block);
            }
            accumulateVectorValidity(bufs, bufWriters, block, multivalued);

            bufs.add(dummyArrowBuf(vectorByteSize(block, multivalued)));
            bufWriters.add(out -> {
                // TODO could we "just" get the memory of the array and dump it?
                int count = arrowValueCount(block, multivalued);
                for (int position = 0; position < block.getPositionCount(); position++) {
                    if (block.isNull(position)) {
                        if (multivalued == false) {
                            out.writeIntLE(0);
                        }
                        continue;
                    }
                    int start = block.getFirstValueIndex(position);
                    int end = start + block.getValueCount(position);
                    for (int valueIndex = start; valueIndex < end; valueIndex++) {
                        out.writeIntLE(block.getInt(valueIndex));
                    }
                }
                return (long) count * Integer.BYTES;
            });
        }

        private static int vectorByteSize(Block b, boolean multivalued) {
            return Integer.BYTES * arrowValueCount(b, multivalued);
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
        public void convert(Block b, boolean multivalued, List<ArrowBuf> bufs, List<BufWriter> bufWriters, List<Releasable> releasables) {
            LongBlock block = (LongBlock) b;

            if (multivalued) {
                addListOffsets(bufs, bufWriters, block);
            }
            accumulateVectorValidity(bufs, bufWriters, block, multivalued);

            bufs.add(dummyArrowBuf(vectorByteSize(block, multivalued)));
            bufWriters.add(out -> {
                // TODO could we "just" get the memory of the array and dump it?
                int count = arrowValueCount(block, multivalued);
                for (int position = 0; position < block.getPositionCount(); position++) {
                    if (block.isNull(position)) {
                        if (multivalued == false) {
                            out.writeLongLE(0);
                        }
                        continue;
                    }
                    int start = block.getFirstValueIndex(position);
                    int end = start + block.getValueCount(position);
                    for (int valueIndex = start; valueIndex < end; valueIndex++) {
                        out.writeLongLE(block.getLong(valueIndex));
                    }
                }
                return (long) count * Long.BYTES;
            });
        }

        private static int vectorByteSize(LongBlock b, boolean multivalued) {
            return Long.BYTES * arrowValueCount(b, multivalued);
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
        public void convert(Block b, boolean multivalued, List<ArrowBuf> bufs, List<BufWriter> bufWriters, List<Releasable> releasables) {
            BooleanBlock block = (BooleanBlock) b;

            if (multivalued) {
                addListOffsets(bufs, bufWriters, block);
            }
            accumulateVectorValidity(bufs, bufWriters, block, multivalued);

            bufs.add(dummyArrowBuf(vectorByteSize(block, multivalued)));
            bufWriters.add(out -> {
                int count = arrowValueCount(block, multivalued);
                BitSet bits = new BitSet();

                // Only set the bits that are true, writeBitSet will take
                // care of adding zero bytes if needed.
                int outputIndex = 0;
                for (int position = 0; position < block.getPositionCount(); position++) {
                    if (block.isNull(position)) {
                        if (multivalued == false) {
                            outputIndex++;
                        }
                        continue;
                    }
                    int start = block.getFirstValueIndex(position);
                    int end = start + block.getValueCount(position);
                    for (int valueIndex = start; valueIndex < end; valueIndex++) {
                        if (block.getBoolean(valueIndex)) {
                            bits.set(outputIndex);
                        }
                        outputIndex++;
                    }
                }

                return BlockConverter.writeBitSet(out, bits, count);
            });
        }

        private static int vectorByteSize(BooleanBlock b, boolean multivalued) {
            return BlockConverter.bitSetLength(arrowValueCount(b, multivalued));
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
        public void convert(Block b, boolean multivalued, List<ArrowBuf> bufs, List<BufWriter> bufWriters, List<Releasable> releasables) {
            BytesRefBlock block = (BytesRefBlock) b;

            if (multivalued) {
                addListOffsets(bufs, bufWriters, block);
            }
            accumulateVectorValidity(bufs, bufWriters, block, multivalued);

            // Offsets vector
            bufs.add(dummyArrowBuf(offsetvectorByteSize(block, multivalued)));

            bufWriters.add(out -> writeOffsets(out, block, multivalued));

            // Data vector
            bufs.add(BlockConverter.dummyArrowBuf(dataVectorByteSize(block)));

            bufWriters.add(out -> writeData(out, block));
        }

        protected BytesRef getBytesRef(BytesRefBlock block, int valueIndex, BytesRef scratch) {
            return block.getBytesRef(valueIndex, scratch);
        }

        private static int offsetvectorByteSize(BytesRefBlock block, boolean multivalued) {
            return Integer.BYTES * (arrowValueCount(block, multivalued) + 1);
        }

        private long writeOffsets(RecyclerBytesStreamOutput out, BytesRefBlock block, boolean multivalued) throws IOException {
            // TODO could we "just" get the memory of the array and dump it?
            BytesRef scratch = new BytesRef();
            int offset = 0;
            out.writeIntLE(offset);
            for (int position = 0; position < block.getPositionCount(); position++) {
                if (block.isNull(position)) {
                    if (multivalued == false) {
                        out.writeIntLE(offset);
                    }
                    continue;
                }
                int start = block.getFirstValueIndex(position);
                int end = start + block.getValueCount(position);
                for (int valueIndex = start; valueIndex < end; valueIndex++) {
                    // FIXME: add a ByteRefsVector.getLength(position): there are some cases
                    // where getBytesRef will allocate, which isn't needed here.
                    offset += getBytesRef(block, valueIndex, scratch).length;
                    out.writeIntLE(offset);
                }
            }
            return offsetvectorByteSize(block, multivalued);
        }

        private long writeData(RecyclerBytesStreamOutput out, BytesRefBlock block) throws IOException {
            // TODO could we "just" get the memory of the array and dump it?
            BytesRef scratch = new BytesRef();
            long startPosition = out.position();
            for (int position = 0; position < block.getPositionCount(); position++) {
                if (block.isNull(position)) {
                    continue;
                }
                int start = block.getFirstValueIndex(position);
                int end = start + block.getValueCount(position);
                for (int valueIndex = start; valueIndex < end; valueIndex++) {
                    BytesRef value = getBytesRef(block, valueIndex, scratch);
                    out.write(value.bytes, value.offset, value.length);
                }
            }
            return out.position() - startPosition;
        }

        private int dataVectorByteSize(BytesRefBlock block) {
            // TODO we can probably get the length from the vector without all this sum
            int length = 0;
            BytesRef scratch = new BytesRef();
            for (int position = 0; position < block.getPositionCount(); position++) {
                int start = block.getFirstValueIndex(position);
                int end = start + block.getValueCount(position);
                for (int valueIndex = start; valueIndex < end; valueIndex++) {
                    length += getBytesRef(block, valueIndex, scratch).length;
                }
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
        public void convert(Block b, boolean multivalued, List<ArrowBuf> bufs, List<BufWriter> bufWriters, List<Releasable> releasables) {
            BytesRefBlock transformed = transformValues((BytesRefBlock) b);
            releasables.add(transformed);
            super.convert(transformed, multivalued, bufs, bufWriters, releasables);
        }

        private BytesRefBlock transformValues(BytesRefBlock block) {
            try (BytesRefBlock.Builder builder = block.blockFactory().newBytesRefBlockBuilder(block.getPositionCount())) {
                BytesRef scratch = new BytesRef();
                for (int position = 0; position < block.getPositionCount(); position++) {
                    if (block.isNull(position)) {
                        builder.appendNull();
                        continue;
                    }
                    int start = block.getFirstValueIndex(position);
                    int end = start + block.getValueCount(position);
                    if (end - start == 1) {
                        convertAndAppend(builder, block, start, scratch);
                        continue;
                    }
                    builder.beginPositionEntry();
                    for (int valueIndex = start; valueIndex < end; valueIndex++) {
                        convertAndAppend(builder, block, valueIndex, scratch);
                    }
                    builder.endPositionEntry();
                }
                return builder.build();
            }
        }

        private void convertAndAppend(BytesRefBlock.Builder builder, BytesRefBlock block, int valueIndex, BytesRef scratch) {
            BytesRef bytes = block.getBytesRef(valueIndex, scratch);
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
        public void convert(
            Block block,
            boolean multivalued,
            List<ArrowBuf> bufs,
            List<BufWriter> bufWriters,
            List<Releasable> releasables
        ) {
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

    private static int arrowValueCount(Block block, boolean multivalued) {
        return multivalued ? block.getTotalValueCount() : block.getPositionCount();
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
            // isNull uses position indices, not value indices.
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
            int offset = 0;
            out.writeIntLE(offset);
            for (int position = 0; position < block.getPositionCount(); position++) {
                offset += block.getValueCount(position);
                out.writeIntLE(offset);
            }

            return bufferLen;
        });
    }
}
