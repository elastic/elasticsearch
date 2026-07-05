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

/**
 * Mutable per-block context for encoding, tracking the position bitmap, metadata buffer,
 * and position offsets. Reused across blocks via {@link #clear()}.
 */
public final class EncodingContext {

    private final MetadataBuffer metadataBuffer;
    private final int[] positionOffsets;
    private final int blockSize;
    private final int pipelineLength;

    private int valueCount;
    private short positionBitmap;
    private int currentPosition;

    /**
     * Creates an encoding context with default metadata buffer capacity.
     *
     * @param blockSize      the number of values per block
     * @param pipelineLength the number of stages in the pipeline
     */
    public EncodingContext(int blockSize, int pipelineLength) {
        this(blockSize, pipelineLength, new MetadataBuffer());
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

    /**
     * Returns the number of stages in the pipeline.
     *
     * @return the pipeline length
     */
    public int pipelineLength() {
        return pipelineLength;
    }

    /**
     * Sets the current stage position before invoking each stage's encode method.
     *
     * <p>NOTE: Internal use only - called by the encode pipeline.
     *
     * @param position the zero-based stage index
     */
    public void setCurrentPosition(int position) {
        if (position < 0 || position >= pipelineLength) {
            throw new IllegalArgumentException("Position out of range: " + position);
        }
        this.currentPosition = position;
    }

    /**
     * Records that the stage at the given position was applied.
     * Tracks the metadata buffer offset on the first call for each position.
     *
     * @param position the zero-based stage index
     */
    public void applyStage(int position) {
        assert position >= 0 && position < pipelineLength : "Position out of range: " + position;
        if ((positionBitmap & (1 << position)) == 0) {
            positionOffsets[position] = metadataBuffer.size();
        }
        positionBitmap = (short) (positionBitmap | (1 << position));
    }

    /**
     * Returns {@code true} if the stage at the given position was applied.
     *
     * @param position the zero-based stage index
     * @return whether the stage was applied
     */
    public boolean isStageApplied(int position) {
        assert position >= 0 && position < pipelineLength : "Position out of range: " + position;
        return (positionBitmap & (1 << position)) != 0;
    }

    /**
     * Returns the bitmap of applied stage positions.
     *
     * @return the position bitmap
     */
    public short positionBitmap() {
        return positionBitmap;
    }

    /**
     * Returns the metadata writer, marking the current position as applied.
     *
     * @return the metadata writer
     * @throws AssertionError if the current position has not been set
     */
    public MetadataWriter metadata() {
        assert currentPosition >= 0 : "Current position not set";
        applyStage(currentPosition);
        return metadataBuffer;
    }

    /**
     * Flushes stage metadata to disk in reverse stage order (N-1 first, 0 last).
     * During encoding, stages run forward so metadata accumulates in the buffer in
     * forward order. During decoding, stages run in reverse and the decoder reads
     * metadata directly from the stream with no buffering. By reordering on flush,
     * the on-disk layout matches decode execution order, giving the decoder a free
     * sequential read without seeking or knowing variable-length metadata sizes.
     *
     * @param out the data output stream
     * @throws IOException if an I/O error occurs
     */
    public void writeStageMetadata(final DataOutput out) throws IOException {
        // NOTE: pipelineLength() - 1 excludes the payload stage (last position).
        // Payload stages write directly to the DataOutput via BlockFormat.writeBlock,
        // not through the MetadataBuffer, so there is nothing to flush for them.
        final int numStages = pipelineLength() - 1;
        int nextEndOffset = metadataBuffer.size();
        for (int pos = numStages - 1; pos >= 0; pos--) {
            if (isStageApplied(pos)) {
                final int startOffset = positionOffsets[pos];
                final int size = nextEndOffset - startOffset;
                if (size > 0) {
                    metadataBuffer.writeTo(out, startOffset, size);
                }
                nextEndOffset = startOffset;
            }
        }
    }

    /**
     * Returns the block size.
     *
     * @return the number of values per block
     */
    public int blockSize() {
        return blockSize;
    }

    /**
     * Sets the number of values in the current block.
     *
     * @param count the value count
     */
    public void setValueCount(int count) {
        this.valueCount = count;
    }

    /**
     * Returns the number of values in the current block.
     *
     * @return the value count
     */
    public int valueCount() {
        return valueCount;
    }

    /** Resets this context for reuse with the next block. */
    public void clear() {
        metadataBuffer.clear();
        Arrays.fill(positionOffsets, 0);
        valueCount = 0;
        positionBitmap = 0;
        currentPosition = -1;
    }
}
