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

// NOTE: PipelineDescriptor is used internally by NumericPipeline to track stage configuration.
// The writeTo/readFrom methods exist for future use via FieldDescriptor to create self-describing
// formats. See FieldDescriptor for details on why this is deferred in the POC.
public final class PipelineDescriptor {

    public static final int MAX_PIPELINE_LENGTH = 16;

    private final byte[] stageIds;
    private final byte blockShift;

    public PipelineDescriptor(byte[] stageIds, int blockSize) {
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
    }

    private PipelineDescriptor(byte[] stageIds, byte blockShift) {
        this.stageIds = stageIds;
        this.blockShift = blockShift;
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

    public void writeTo(DataOutput out) throws IOException {
        out.writeVInt(stageIds.length);
        out.writeBytes(stageIds, 0, stageIds.length);
        out.writeByte(blockShift);
    }

    public static PipelineDescriptor readFrom(DataInput in) throws IOException {
        int length = in.readVInt();
        if (length <= 0 || length > MAX_PIPELINE_LENGTH) {
            throw new IllegalArgumentException("Invalid pipeline length: " + length);
        }
        byte[] stageIds = new byte[length];
        in.readBytes(stageIds, 0, length);
        byte blockShift = in.readByte();
        return new PipelineDescriptor(stageIds, blockShift);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PipelineDescriptor that = (PipelineDescriptor) o;
        return blockShift == that.blockShift && Arrays.equals(stageIds, that.stageIds);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(stageIds);
        result = 31 * result + blockShift;
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("PipelineDescriptor{stages=[");
        for (int i = 0; i < stageIds.length; i++) {
            if (i > 0) sb.append(", ");
            sb.append(StageId.fromId(stageIds[i]).name());
        }
        sb.append("], blockSize=").append(blockSize()).append("}");
        return sb.toString();
    }
}
