/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nio;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.function.BiConsumer;

public class FlushOperation {

    private static final ByteBuffer[] EMPTY_ARRAY = new ByteBuffer[0];

    private final BiConsumer<Void, Exception> listener;
    private final ByteBuffer[] buffers;
    private final int[] offsets;
    private final int length;
    private int internalIndex;

    public FlushOperation(ByteBuffer[] buffers, BiConsumer<Void, Exception> listener) {
        this.listener = listener;
        this.buffers = buffers;
        this.offsets = new int[buffers.length];
        int offset = 0;
        for (int i = 0; i < buffers.length; i++) {
            ByteBuffer buffer = buffers[i];
            offsets[i] = offset;
            offset += buffer.remaining();
        }
        length = offset;
    }

    public BiConsumer<Void, Exception> getListener() {
        return listener;
    }

    public boolean isFullyFlushed() {
        assert length >= internalIndex : "Should never have an index that is greater than the length [length=" + length + ", index="
            + internalIndex + "]";
        return internalIndex == length;
    }

    public void incrementIndex(int delta) {
        internalIndex += delta;
        assert length >= internalIndex : "Should never increment index past length [length=" + length + ", post-increment index="
            + internalIndex + ", delta=" + delta + "]";
    }

    public ByteBuffer[] getBuffersToWrite() {
        return getBuffersToWrite(length);
    }

    public ByteBuffer[] getBuffersToWrite(int maxBytes) {
        final int index = Arrays.binarySearch(offsets, internalIndex);
        final int offsetIndex = index < 0 ? (-(index + 1)) - 1 : index;
        final int finalIndex = Arrays.binarySearch(offsets, Math.min(internalIndex + maxBytes, length));
        final int finalOffsetIndex = finalIndex < 0 ? (-(finalIndex + 1)) - 1 : finalIndex;

        int nBuffers = (finalOffsetIndex - offsetIndex) + 1;

        int firstBufferPosition = internalIndex - offsets[offsetIndex];
        ByteBuffer firstBuffer = buffers[offsetIndex].duplicate();
        firstBuffer.position(firstBufferPosition);
        if (nBuffers == 1 && firstBuffer.remaining() == 0) {
            return EMPTY_ARRAY;
        }

        ByteBuffer[] postIndexBuffers = new ByteBuffer[nBuffers];
        postIndexBuffers[0] = firstBuffer;
        int finalOffset = offsetIndex + nBuffers;
        int nBytes = firstBuffer.remaining();
        int j = 1;
        for (int i = (offsetIndex + 1); i < finalOffset; ++i) {
            ByteBuffer buffer = buffers[i].duplicate();
            nBytes += buffer.remaining();
            postIndexBuffers[j++] = buffer;
        }

        int excessBytes = Math.max(0, nBytes - maxBytes);
        ByteBuffer lastBuffer = postIndexBuffers[postIndexBuffers.length - 1];
        lastBuffer.limit(lastBuffer.limit() - excessBytes);
        return postIndexBuffers;
    }
}
