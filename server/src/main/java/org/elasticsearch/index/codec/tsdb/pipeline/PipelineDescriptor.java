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

// NOTE: PipelineDescriptor tracks stage configuration and is the contract between encoder and decoder.
// The writeTo/readFrom methods are used by FieldDescriptor to create self-describing formats.
public final class PipelineDescriptor {

    public static final int MAX_PIPELINE_LENGTH = 16;

    public enum DataType {
        LONG((byte) 0x00),
        DOUBLE((byte) 0x01),
        FLOAT((byte) 0x02);

        public final byte id;

        DataType(byte id) {
            this.id = id;
        }

        public static DataType fromId(byte id) {
            return switch (id) {
                case 0x00 -> LONG;
                case 0x01 -> DOUBLE;
                case 0x02 -> FLOAT;
                default -> throw new IllegalArgumentException("Unknown DataType: 0x" + Integer.toHexString(id & 0xFF));
            };
        }
    }

    private final byte[] stageIds;
    private final byte blockShift;
    private final DataType dataType;

    public PipelineDescriptor(byte[] stageIds, int blockSize) {
        this(stageIds, blockSize, DataType.LONG);
    }

    public PipelineDescriptor(byte[] stageIds, int blockSize, DataType dataType) {
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

    private PipelineDescriptor(byte[] stageIds, byte blockShift, DataType dataType) {
        this.stageIds = stageIds;
        this.blockShift = blockShift;
        this.dataType = dataType;
    }

    public int pipelineLength() {
        return stageIds.length;
    }

    public byte stageIdAt(int position) {
        return stageIds[position];
    }

    public int blockSize() {
        return 1 << blockShift;
    }

    public int blockShift() {
        return blockShift;
    }

    public int bitmapBytes() {
        return pipelineLength() <= 8 ? 1 : 2;
    }

    public byte[] stageIds() {
        return stageIds.clone();
    }

    public DataType dataType() {
        return dataType;
    }

    public PipelineDescriptor withBlockSize(int blockSize) {
        byte newBlockShift = (byte) Integer.numberOfTrailingZeros(blockSize);
        if (newBlockShift == this.blockShift) {
            return this;
        }
        return new PipelineDescriptor(stageIds.clone(), blockSize, dataType);
    }

    // Wire format: [VInt stageCount] [byte[] stageIds] [byte blockShift] [byte dataType]
    // NOTE: Format evolution is handled by the version byte in FieldDescriptor.
    public void writeTo(DataOutput out) throws IOException {
        out.writeVInt(stageIds.length);
        out.writeBytes(stageIds, 0, stageIds.length);
        out.writeByte(blockShift);
        out.writeByte(dataType.id);
    }

    public static PipelineDescriptor readFrom(DataInput in) throws IOException {
        int length = in.readVInt();
        if (length <= 0 || length > MAX_PIPELINE_LENGTH) {
            throw new IllegalArgumentException("Invalid pipeline length: " + length);
        }
        byte[] stageIds = new byte[length];
        in.readBytes(stageIds, 0, length);
        byte blockShift = in.readByte();
        DataType dataType = DataType.fromId(in.readByte());
        return new PipelineDescriptor(stageIds, blockShift, dataType);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PipelineDescriptor that = (PipelineDescriptor) o;
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
        StringBuilder sb = new StringBuilder("PipelineDescriptor{stages=[");
        for (int i = 0; i < stageIds.length; i++) {
            if (i > 0) sb.append(", ");
            sb.append(StageId.fromId(stageIds[i]).displayName);
        }
        sb.append("], blockSize=").append(blockSize()).append(", dataType=").append(dataType).append("}");
        return sb.toString();
    }
}
