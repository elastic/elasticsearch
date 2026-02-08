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

public final class DecodingContext implements MetadataReader {

    private final byte[] stageIds;
    private final int blockSize;

    private DataInput dataInput;
    private short positionBitmap;

    public DecodingContext(int blockSize, final byte[] stageIds) {
        this.blockSize = blockSize;
        this.stageIds = stageIds;
    }

    public void setDataInput(final DataInput dataInput) {
        this.dataInput = dataInput;
    }

    public int pipelineLength() {
        return stageIds.length;
    }

    public void setPositionBitmap(short bitmap) {
        this.positionBitmap = bitmap;
    }

    public boolean isStageApplied(int position) {
        return (positionBitmap & (1 << position)) != 0;
    }

    public MetadataReader metadata() {
        return this;
    }

    public int blockSize() {
        return blockSize;
    }

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
