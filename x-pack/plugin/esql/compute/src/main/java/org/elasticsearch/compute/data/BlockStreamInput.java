/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.IOUtils;

import java.io.IOException;

/**
 * Stream for reading {@link Block}s. You must {@link #close} this reader when
 * you are done using it. While this wraps a {@link StreamInput} to do the
 * actual reading of the bytes the {@link #close} method doesn't close the
 * wrapped reader. Instead it decrements any reference counts on {@link Block}s
 * cached in the reader.
 */
public class BlockStreamInput extends NamedWriteableAwareStreamInput {
    private final BlockFactory blockFactory;
    /**
     * Cache of the last constant null block returned by {@link #readConstantNullBlock}.
     * Every call to {@link #readConstantNullBlock} will {@link Block#incRef} the reference
     * count so the total reference count for this block is {@code count_of_calls + 1} with
     * the {@code 1} representing this reference. We'll call {@link Block#close} when this
     * cache is replaced with another block or when this stream is
     * {@link StreamInput#close() closed}.
     * <p>
     *     Since every block in a {@link Page} has the same size we can reuse this block
     *     for all constant null blocks in the same Page.
     * </p>
     */
    private Block lastConstantNullBlock;

    public BlockStreamInput(StreamInput delegate, BlockFactory blockFactory) {
        super(delegate, delegate.namedWriteableRegistry());
        this.blockFactory = blockFactory;
    }

    public BlockFactory blockFactory() {
        return blockFactory;
    }

    /**
     * Reads the length for a {@link ConstantNullBlock} from the stream and
     * returns a {@link ConstantNullBlock} with that size. Subsequent calls
     * to this method that return a block with the same length will return
     * the same block.
     */
    Block readConstantNullBlock() throws IOException {
        int positions = readVInt();
        if (lastConstantNullBlock == null) {
            lastConstantNullBlock = blockFactory.newConstantNullBlock(positions);
        } else if (lastConstantNullBlock.getPositionCount() != positions) {
            lastConstantNullBlock.decRef();
            lastConstantNullBlock = blockFactory.newConstantNullBlock(positions);
        }
        lastConstantNullBlock.incRef();
        return lastConstantNullBlock;
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(lastConstantNullBlock);
    }
}
