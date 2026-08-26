/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;

import java.io.IOException;
import java.util.Arrays;

/**
 * Persisted descriptor of a pipeline configuration, tracking stage IDs, block size, and data type.
 *
 * <p>{@code PipelineDescriptor} is the contract between encoder and decoder. The
 * {@link #writeTo}/{@link #readFrom} methods are used by {@link FieldDescriptor}
 * to create self-describing formats. Format evolution is handled by the version
 * byte in {@link FieldDescriptor}.
 *
 * <p>Wire format: {@code [VInt stageCount] [byte blockShift] [byte dataType] [byte[] stageIds]}.
 * Scalars come before the variable-length stage array so the decoder can configure
 * block size and data type before iterating stage IDs.
 *
 * <p>Example: a {@code delta>gcd>bitPack} pipeline on longs with {@code blockSize=128}
 * (blockShift=7) serializes as {@code [03] [07] [00] [01 03 A1]}.
 */
public final class PipelineDescriptor {

    /** Maximum number of stages in a pipeline. */
    public static final int MAX_PIPELINE_LENGTH = 16;

    /** The numeric data type stored in encoded blocks. */
    public enum DataType {
        /** 64-bit signed integer values. */
        LONG((byte) 0x00),
        /** 64-bit IEEE 754 floating-point values. */
        DOUBLE((byte) 0x01),
        /** 32-bit IEEE 754 floating-point values. */
        FLOAT((byte) 0x02);

        /** Persisted byte identifier for this data type. */
        public final byte id;

        DataType(byte id) {
            this.id = id;
        }

        /**
         * Resolves a persisted byte identifier back to its {@link DataType}.
         *
         * @param id the byte identifier read from the encoded data
         * @return the corresponding {@link DataType}
         * @throws IOException if the identifier is unknown (corrupt data)
         */
        public static DataType fromId(byte id) throws IOException {
            return switch (id) {
                case 0x00 -> LONG;
                case 0x01 -> DOUBLE;
                case 0x02 -> FLOAT;
                default -> throw new IOException("Unknown DataType: 0x" + Integer.toHexString(id & 0xFF));
            };
        }
    }

    private final byte[] stageIds;
    private final byte blockShift;
    private final DataType dataType;

    /**
     * Creates a descriptor for a long (integral) pipeline.
     *
     * @param stageIds the ordered stage identifiers
     * @param blockSize the number of values per block (must be a power of 2)
     */
    public PipelineDescriptor(final byte[] stageIds, int blockSize) {
        this(stageIds, blockSize, DataType.LONG);
    }

    /**
     * Creates a descriptor with the specified data type.
     *
     * @param stageIds  the ordered stage identifiers
     * @param blockSize the number of values per block (must be a power of 2)
     * @param dataType  the numeric data type this pipeline operates on
     */
    public PipelineDescriptor(final byte[] stageIds, int blockSize, final DataType dataType) {
        if (stageIds == null || stageIds.length == 0) {
            throw new IllegalArgumentException("Pipeline must have at least one stage");
        }
        if (stageIds.length > MAX_PIPELINE_LENGTH) {
            throw new IllegalArgumentException("Pipeline length " + stageIds.length + " exceeds maximum " + MAX_PIPELINE_LENGTH);
        }
        if (blockSize <= 0 || (blockSize & (blockSize - 1)) != 0) {
            throw new IllegalArgumentException("Block size must be a positive power of 2: " + blockSize);
        }
        this.stageIds = stageIds.clone();
        this.blockShift = (byte) Integer.numberOfTrailingZeros(blockSize);
        this.dataType = dataType;
    }

    private PipelineDescriptor(final byte[] stageIds, byte blockShift, final DataType dataType) {
        this.stageIds = stageIds;
        this.blockShift = blockShift;
        this.dataType = dataType;
    }

    /**
     * Returns the number of stages in this pipeline.
     *
     * @return the pipeline length
     */
    public int pipelineLength() {
        return stageIds.length;
    }

    /**
     * Returns the stage identifier at the given position.
     *
     * @param position the zero-based stage index
     * @return the byte identifier at that position
     */
    public byte stageIdAt(int position) {
        return stageIds[position];
    }

    /**
     * Returns the block size (number of values per block).
     *
     * @return the block size
     */
    public int blockSize() {
        return 1 << blockShift;
    }

    /**
     * Returns the block shift (log2 of block size).
     *
     * @return the block shift
     */
    int blockShift() {
        return blockShift;
    }

    /**
     * Returns the number of bytes needed for the per-block position bitmap.
     * Uses 1 byte for pipelines with 8 or fewer stages, 2 bytes otherwise.
     *
     * @return the bitmap size in bytes (1 or 2)
     */
    int bitmapBytes() {
        return pipelineLength() <= 8 ? 1 : 2;
    }

    /**
     * Returns a defensive copy of the stage identifier array.
     *
     * @return a copy of the stage identifiers
     */
    byte[] stageIds() {
        return stageIds.clone();
    }

    /**
     * Returns the data type this pipeline operates on.
     *
     * @return the data type
     */
    public DataType dataType() {
        return dataType;
    }

    /**
     * Returns a new descriptor with the specified block size, or this instance if unchanged.
     *
     * @param blockSize the desired block size (must be a power of 2)
     * @return a descriptor with the given block size
     */
    public PipelineDescriptor withBlockSize(int blockSize) {
        final byte newBlockShift = (byte) Integer.numberOfTrailingZeros(blockSize);
        if (newBlockShift == this.blockShift) {
            return this;
        }
        return new PipelineDescriptor(stageIds.clone(), blockSize, dataType);
    }

    /**
     * Serializes this descriptor to the given output.
     *
     * @param out the data output stream
     * @throws IOException if an I/O error occurs
     */
    void writeTo(final DataOutput out) throws IOException {
        out.writeVInt(stageIds.length);
        out.writeByte(blockShift);
        out.writeByte(dataType.id);
        out.writeBytes(stageIds, 0, stageIds.length);
    }

    /**
     * Deserializes a descriptor from the given input.
     *
     * @param in the data input stream
     * @return the deserialized descriptor
     * @throws IOException if an I/O error occurs or the data is invalid
     */
    static PipelineDescriptor readFrom(final DataInput in) throws IOException {
        final int length = in.readVInt();
        if (length <= 0 || length > MAX_PIPELINE_LENGTH) {
            throw new IOException("Invalid pipeline length: " + length);
        }
        final byte blockShift = in.readByte();
        if (blockShift < 0 || blockShift > 30) {
            throw new IOException("Invalid block shift: " + blockShift);
        }
        final DataType dataType = DataType.fromId(in.readByte());
        final byte[] stageIds = new byte[length];
        in.readBytes(stageIds, 0, length);
        return new PipelineDescriptor(stageIds, blockShift, dataType);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final PipelineDescriptor that = (PipelineDescriptor) o;
        return blockShift == that.blockShift && dataType == that.dataType && Arrays.equals(stageIds, that.stageIds);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(stageIds);
        result = 31 * result + blockShift;
        result = 31 * result + dataType.id;
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PipelineDescriptor{stages=[");
        for (int i = 0; i < stageIds.length; i++) {
            if (i > 0) sb.append(", ");
            try {
                sb.append(StageId.fromId(stageIds[i]).displayName);
            } catch (final IllegalArgumentException e) {
                sb.append("0x").append(Integer.toHexString(stageIds[i] & 0xFF));
            }
        }
        sb.append("], blockSize=").append(blockSize()).append(", dataType=").append(dataType).append("}");
        return sb.toString();
    }
}
