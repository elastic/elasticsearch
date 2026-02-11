/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline;

import org.apache.lucene.store.DataOutput;

import java.io.IOException;
import java.util.Arrays;

public final class EncodingContext {

    private final MetadataBuffer metadataBuffer;
    private final int[] positionOffsets;
    private final int blockSize;
    private final int pipelineLength;

    private int valueCount;
    private short positionBitmap;
    private int currentPosition;

    public EncodingContext(int blockSize, int pipelineLength) {
        this(blockSize, pipelineLength, new MetadataBuffer());
    }

    public EncodingContext(int blockSize, int pipelineLength, int metadataCapacity) {
        this(blockSize, pipelineLength, new MetadataBuffer(metadataCapacity));
    }

    EncodingContext(int blockSize, int pipelineLength, final MetadataBuffer metadataBuffer) {
        this.blockSize = blockSize;
        this.pipelineLength = pipelineLength;
        this.metadataBuffer = metadataBuffer;
        this.positionOffsets = new int[PipelineDescriptor.MAX_PIPELINE_LENGTH];
        this.valueCount = 0;
        this.positionBitmap = 0;
        this.currentPosition = -1;
    }

    public int pipelineLength() {
        return pipelineLength;
    }

    // NOTE: Internal use only - called by the encode pipeline to set the current
    // stage position before invoking each stage's encode method.
    public void setCurrentPosition(int position) {
        this.currentPosition = position;
    }

    public int position() {
        return currentPosition;
    }

    public void applyStage(int position) {
        // Only record the offset on the first call for this position.
        // Subsequent calls (e.g., multiple writeLong calls from a stage)
        // should not overwrite the starting offset.
        if ((positionBitmap & (1 << position)) == 0) {
            positionOffsets[position] = metadataBuffer.size();
        }
        positionBitmap = (short) (positionBitmap | (1 << position));
    }

    public boolean isStageApplied(int position) {
        return (positionBitmap & (1 << position)) != 0;
    }

    public short positionBitmap() {
        return positionBitmap;
    }

    public MetadataWriter metadata() {
        assert currentPosition >= 0 : "Current position not set";
        applyStage(currentPosition);
        return metadataBuffer;
    }

    // NOTE: Metadata is written in reverse stage order (stage N-1 first, stage 0 last)
    // because the decoder runs stages in reverse: N-1 -> N-2 -> ... -> 0. By matching
    // the write order to the decode order, the decoder can read metadata as a single
    // sequential forward pass through the stream. Without this, the decoder would need
    // to either buffer all metadata upfront or know each stage's metadata size in advance
    // to seek to the right offset. Each stage's metadata is variable-length (e.g., ALP
    // writes e + f + exceptions, GCD writes a single VLong), so skipping without
    // buffering is not possible.
    public void writeStageMetadata(final DataOutput out) throws IOException {
        int numStages = pipelineLength() - 1;
        int nextEndOffset = metadataBuffer.size();
        for (int pos = numStages - 1; pos >= 0; pos--) {
            if (isStageApplied(pos)) {
                int startOffset = positionOffsets[pos];
                int size = nextEndOffset - startOffset;
                if (size > 0) {
                    metadataBuffer.writeTo(out, startOffset, size);
                }
                nextEndOffset = startOffset;
            }
        }
    }

    public int blockSize() {
        return blockSize;
    }

    public void setValueCount(int count) {
        this.valueCount = count;
    }

    public int valueCount() {
        return valueCount;
    }

    public void clear() {
        metadataBuffer.clear();
        Arrays.fill(positionOffsets, 0);
        valueCount = 0;
        positionBitmap = 0;
        currentPosition = -1;
    }
}
