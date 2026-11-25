/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.formatter.arrow;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.Types;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;

import java.util.BitSet;
import java.util.List;

/**
 * Arrow formatters for scalar, dense vector, and null ES|QL block types.
 */
final class ScalarArrowFormatters {

    private ScalarArrowFormatters() {}

    /**
     * Encodes dense vectors as Arrow lists with {@code float32} elements. Byte and bit vector elements have already been
     * widened to floats by the field loader.
     */
    static class AsDenseVector extends BlockArrowFormatter {

        AsDenseVector(DataType esqlType) {
            super(esqlType, Types.MinorType.FLOAT4);
        }

        @Override
        public boolean alwaysList() {
            return true;
        }

        @Override
        public void convert(
            Block b,
            boolean asList,
            List<ArrowFieldNode> nodes,
            List<ArrowBuf> bufs,
            List<BufWriter> bufWriters,
            List<Releasable> releasables
        ) {
            FloatBlock block = (FloatBlock) b;
            assert asList;
            addSimpleFieldNodes(block, true, nodes);
            addListOffsets(bufs, bufWriters, block);
            accumulateVectorValidity(bufs, bufWriters, block, true);

            long byteSize = (long) Float.BYTES * block.getTotalValueCount();
            bufs.add(dummyArrowBuf(byteSize));
            bufWriters.add(out -> {
                for (int position = 0; position < block.getPositionCount(); position++) {
                    if (block.isNull(position)) {
                        continue;
                    }
                    int start = block.getFirstValueIndex(position);
                    int end = start + block.getValueCount(position);
                    for (int valueIndex = start; valueIndex < end; valueIndex++) {
                        out.writeIntLE(Float.floatToRawIntBits(block.getFloat(valueIndex)));
                    }
                }
                return byteSize;
            });
        }
    }

    /**
     * Conversion of Double blocks
     */
    static class AsFloat64 extends BlockArrowFormatter {

        AsFloat64(DataType esqlType) {
            super(esqlType, Types.MinorType.FLOAT8);
        }

        @Override
        public void convert(
            Block b,
            boolean asList,
            List<ArrowFieldNode> nodes,
            List<ArrowBuf> bufs,
            List<BufWriter> bufWriters,
            List<Releasable> releasables
        ) {
            if (asList) {
                nodes.add(new ArrowFieldNode(b.getPositionCount(), b.nullValuesCount()));
                addListOffsets(bufs, bufWriters, b);
            }
            convertValueVector(b, asList, nodes, bufs, bufWriters);
        }

        /**
         * Emits the field node, validity bitmap, and value buffer for this double vector without an enclosing Arrow list.
         * When {@code flattened} is {@code true}, null positions are skipped and values are emitted flat (used both for
         * list value vectors and for struct children inside a list).
         */
        void convertValueVector(Block b, boolean flattened, List<ArrowFieldNode> nodes, List<ArrowBuf> bufs, List<BufWriter> bufWriters) {
            DoubleBlock block = (DoubleBlock) b;
            nodes.add(
                flattened
                    ? new ArrowFieldNode(block.getTotalValueCount(), 0)
                    : new ArrowFieldNode(block.getPositionCount(), block.nullValuesCount())
            );
            accumulateVectorValidity(bufs, bufWriters, block, flattened);

            long byteSize = (long) Double.BYTES * arrowValueCount(block, flattened);
            bufs.add(dummyArrowBuf(byteSize));
            bufWriters.add(out -> {
                // TODO could we "just" get the memory of the array and dump it?
                for (int position = 0; position < block.getPositionCount(); position++) {
                    if (block.isNull(position)) {
                        if (flattened == false) {
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
                return byteSize;
            });
        }
    }

    /**
     * Conversion of Int blocks
     */
    static class AsInt32 extends BlockArrowFormatter {

        AsInt32(DataType esqlType) {
            super(esqlType, Types.MinorType.INT);
        }

        @Override
        public void convert(
            Block b,
            boolean asList,
            List<ArrowFieldNode> nodes,
            List<ArrowBuf> bufs,
            List<BufWriter> bufWriters,
            List<Releasable> releasables
        ) {
            IntBlock block = (IntBlock) b;
            addSimpleFieldNodes(block, asList, nodes);

            if (asList) {
                addListOffsets(bufs, bufWriters, block);
            }
            accumulateVectorValidity(bufs, bufWriters, block, asList);

            long byteSize = (long) Integer.BYTES * arrowValueCount(block, asList);
            bufs.add(dummyArrowBuf(byteSize));
            bufWriters.add(out -> {
                // TODO could we "just" get the memory of the array and dump it?
                for (int position = 0; position < block.getPositionCount(); position++) {
                    if (block.isNull(position)) {
                        if (asList == false) {
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
                return byteSize;
            });
        }
    }

    /**
     * Conversion of Long blocks
     */
    static class AsInt64 extends BlockArrowFormatter {
        AsInt64(DataType esqlType) {
            this(esqlType, Types.MinorType.BIGINT);
        }

        AsInt64(DataType esqlType, Types.MinorType minorType) {
            super(esqlType, minorType);
        }

        @Override
        public void convert(
            Block b,
            boolean asList,
            List<ArrowFieldNode> nodes,
            List<ArrowBuf> bufs,
            List<BufWriter> bufWriters,
            List<Releasable> releasables
        ) {
            if (asList) {
                nodes.add(new ArrowFieldNode(b.getPositionCount(), b.nullValuesCount()));
                addListOffsets(bufs, bufWriters, b);
            }
            convertValueVector(b, asList, nodes, bufs, bufWriters);
        }

        /**
         * Emits the field node, validity bitmap, and value buffer for this long vector without an enclosing Arrow list.
         * When {@code flattened} is {@code true}, null positions are skipped and values are emitted flat (used both for
         * list value vectors and for struct children inside a list).
         */
        void convertValueVector(Block b, boolean flattened, List<ArrowFieldNode> nodes, List<ArrowBuf> bufs, List<BufWriter> bufWriters) {
            LongBlock block = (LongBlock) b;
            nodes.add(
                flattened
                    ? new ArrowFieldNode(block.getTotalValueCount(), 0)
                    : new ArrowFieldNode(block.getPositionCount(), block.nullValuesCount())
            );
            accumulateVectorValidity(bufs, bufWriters, block, flattened);

            long byteSize = (long) Long.BYTES * arrowValueCount(block, flattened);
            bufs.add(dummyArrowBuf(byteSize));
            bufWriters.add(out -> {
                // TODO could we "just" get the memory of the array and dump it?
                for (int position = 0; position < block.getPositionCount(); position++) {
                    if (block.isNull(position)) {
                        if (flattened == false) {
                            out.writeLongLE(0);
                        }
                        continue;
                    }
                    int start = block.getFirstValueIndex(position);
                    int end = start + block.getValueCount(position);
                    for (int valueIndex = start; valueIndex < end; valueIndex++) {
                        out.writeLongLE(arrowValue(block.getLong(valueIndex)));
                    }
                }
                return byteSize;
            });
        }

        protected long arrowValue(long blockValue) {
            return blockValue;
        }
    }

    /**
     * Conversion of {@code unsigned_long} blocks, which store values with the sign bit flipped so that they sort as signed
     * longs. Arrow's {@code UInt64} expects the plain unsigned bits.
     */
    static class AsFlippedSignInt64 extends AsInt64 {
        AsFlippedSignInt64(DataType esqlType) {
            super(esqlType, Types.MinorType.UINT8);
        }

        @Override
        protected long arrowValue(long blockValue) {
            return blockValue ^ NumericUtils.TWOS_COMPLEMENT_BITMASK;
        }
    }

    /**
     * Conversion of Boolean blocks
     */
    static class AsBoolean extends BlockArrowFormatter {
        AsBoolean(DataType esqlType) {
            super(esqlType, Types.MinorType.BIT);
        }

        @Override
        public void convert(
            Block b,
            boolean asList,
            List<ArrowFieldNode> nodes,
            List<ArrowBuf> bufs,
            List<BufWriter> bufWriters,
            List<Releasable> releasables
        ) {
            BooleanBlock block = (BooleanBlock) b;
            addSimpleFieldNodes(block, asList, nodes);

            if (asList) {
                addListOffsets(bufs, bufWriters, block);
            }
            accumulateVectorValidity(bufs, bufWriters, block, asList);

            bufs.add(dummyArrowBuf(bitSetLength(arrowValueCount(block, asList))));
            bufWriters.add(out -> {
                int count = arrowValueCount(block, asList);
                BitSet bits = new BitSet();

                // Only set the bits that are true, writeBitSet will take
                // care of adding zero bytes if needed.
                int outputIndex = 0;
                for (int position = 0; position < block.getPositionCount(); position++) {
                    if (block.isNull(position)) {
                        if (asList == false) {
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

                return writeBitSet(out, bits, count);
            });
        }
    }

    /**
     * Conversion of {@code null} and {@code unsupported} columns to Arrow {@code Null} vectors (no data buffers).
     */
    static class AsNull extends BlockArrowFormatter {
        AsNull(DataType esqlType) {
            super(esqlType, Types.MinorType.NULL);
        }

        @Override
        public boolean isMultivalued(String columnName, Block block) {
            return false;
        }

        @Override
        public void convert(
            Block block,
            boolean asList,
            List<ArrowFieldNode> nodes,
            List<ArrowBuf> bufs,
            List<BufWriter> bufWriters,
            List<Releasable> releasables
        ) {
            assert asList == false : "Null columns cannot be encoded as Arrow lists";
            addSimpleFieldNodes(block, false, nodes);
            // Null vector in arrow has no associated buffers
            // See https://arrow.apache.org/docs/format/Columnar.html#null-layout
        }
    }
}
