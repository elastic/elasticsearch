/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
