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

import java.io.IOException;

/**
 * Mutable per-block context for decoding, tracking the position bitmap and
 * delegating metadata reads to the underlying {@link DataInput}. Reused
 * across blocks via {@link #clear()}.
 */
public final class DecodingContext implements MetadataReader {

    private final int pipelineLength;
    private final int blockSize;

    private DataInput dataInput;
    private short positionBitmap;

    /**
     * Creates a decoding context.
     *
     * @param blockSize      the number of values per block
     * @param pipelineLength the number of stages in the pipeline
     */
    public DecodingContext(int blockSize, int pipelineLength) {
        this.blockSize = blockSize;
        this.pipelineLength = pipelineLength;
    }

    /**
     * Sets the data input stream for reading block data and stage metadata.
     *
     * @param dataInput the data input stream
     */
    public void setDataInput(final DataInput dataInput) {
        this.dataInput = dataInput;
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
     * Sets the bitmap of applied stage positions, read from the block header.
     *
     * @param bitmap the position bitmap
     */
    void setPositionBitmap(short bitmap) {
        this.positionBitmap = bitmap;
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
     * Returns the metadata reader for accessing stage metadata.
     *
     * @return the metadata reader
     */
    public MetadataReader metadata() {
        return this;
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
     * Resets this context for reuse with the next block.
     *
     * <p>NOTE: dataInput is intentionally nulled. Unlike EncodingContext which
     * owns its MetadataBuffer, DecodingContext does not own the DataInput (it is injected).
     * Nulling forces the caller to provide a fresh DataInput via {@link #setDataInput} before
     * each block, which is a fail-fast against silently reading garbage from a stale stream.
     */
    public void clear() {
        positionBitmap = 0;
        dataInput = null;
    }

    @Override
    public byte readByte() throws IOException {
        return dataInput.readByte();
    }

    @Override
    public int readZInt() throws IOException {
        return dataInput.readZInt();
    }

    @Override
    public long readZLong() throws IOException {
        return dataInput.readZLong();
    }

    @Override
    public long readLong() throws IOException {
        return dataInput.readLong();
    }

    @Override
    public int readInt() throws IOException {
        return dataInput.readInt();
    }

    @Override
    public int readVInt() throws IOException {
        return dataInput.readVInt();
    }

    @Override
    public long readVLong() throws IOException {
        return dataInput.readVLong();
    }

    @Override
    public void readBytes(final byte[] bytes, int offset, int length) throws IOException {
        dataInput.readBytes(bytes, offset, length);
    }
}
