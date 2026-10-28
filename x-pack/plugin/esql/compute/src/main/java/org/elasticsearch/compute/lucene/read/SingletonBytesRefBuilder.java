/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.read;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.index.mapper.BlockLoader;

public final class SingletonBytesRefBuilder implements BlockLoader.SingletonBytesRefBuilder {
    private final int count;
    private final BlockFactory blockFactory;
    private byte[] bytes;
    private int[] startOffsets;

    public SingletonBytesRefBuilder(int count, BlockFactory blockFactory) {
        this.count = count;
        this.blockFactory = blockFactory;
    }

    @Override
    public SingletonBytesRefBuilder appendBytesRefs(byte[] bytes, int[] offsets) {
        this.bytes = bytes;
        this.startOffsets = offsets;
        return this;
    }

    @Override
    public SingletonBytesRefBuilder appendBytesRefs(byte[] bytes, int fixedLength) {
        final int[] offsets = new int[count + 1];
        for (int i = 0; i <= count; i++) {
            offsets[i] = i * fixedLength;
        }
        return appendBytesRefs(bytes, offsets);
    }

    @Override
    public BlockLoader.Block build() {
        BytesRefBlock block = blockFactory.newDirectBytesRefVector(bytes, startOffsets, count).asBlock();
        bytes = null;
        startOffsets = null;
        return block;
    }

    @Override
    public BlockLoader.Builder appendNull() {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockLoader.Builder beginPositionEntry() {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockLoader.Builder endPositionEntry() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {}
}
