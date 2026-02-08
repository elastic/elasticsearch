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
    private final short[] positionOffsets;
    private final int blockSize;
    private final int pipelineLength;

    private int valueCount;
    private short positionBitmap;
    private int currentPosition;

    public EncodingContext(int blockSize, int pipelineLength) {
        this(blockSize, pipelineLength, new MetadataBuffer());
    }

    EncodingContext(int blockSize, int pipelineLength, final MetadataBuffer metadataBuffer) {
        this.blockSize = blockSize;
        this.pipelineLength = pipelineLength;
        this.metadataBuffer = metadataBuffer;
        this.positionOffsets = new short[PipelineDescriptor.MAX_PIPELINE_LENGTH];
        this.valueCount = 0;
        this.positionBitmap = 0;
        this.currentPosition = -1;
    }

    public int pipelineLength() {
        return pipelineLength;
    }

    // NOTE: Internal use only - called by NumericPipeline to set the current
    // stage position before invoking each stage's encode method.
    public void setCurrentPosition(int position) {
        this.currentPosition = position;
    }

    public int position() {
        return currentPosition;
    }

    public void applyStage(int position) {
        positionBitmap = (short) (positionBitmap | (1 << position));
        positionOffsets[position] = metadataBuffer.size();
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

    // NOTE: Metadata is written in reverse order (last stage first) so that during
    // decoding, each stage reads its metadata in the order it executes. Decoding
    // runs stages in reverse: stage N-1 -> stage 0, so metadata for stage N-1
    // must appear first in the stream.
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
        Arrays.fill(positionOffsets, (short) 0);
        valueCount = 0;
        positionBitmap = 0;
        currentPosition = -1;
    }
}
