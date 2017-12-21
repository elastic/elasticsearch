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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.function.BiConsumer;

public class WriteOperation {

    private final NioSocketChannel channel;
    private final BiConsumer<Void, Throwable> listener;
    private final ByteBuffer[] buffers;
    private final int[] offsets;
    private final int length;
    private int internalIndex;

    public WriteOperation(NioSocketChannel channel, ByteBuffer[] buffers, BiConsumer<Void, Throwable> listener) {
        this.channel = channel;
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

    public ByteBuffer[] getByteBuffers() {
        return buffers;
    }

    public BiConsumer<Void, Throwable> getListener() {
        return listener;
    }

    public NioSocketChannel getChannel() {
        return channel;
    }

    public boolean isFullyFlushed() {
        return internalIndex == length;
    }

    public int flush() throws IOException {
        int written = channel.write(getBuffersToWrite());
        internalIndex += written;
        return written;
    }

    private ByteBuffer[] getBuffersToWrite() {
        int offsetIndex = getOffsetIndex(internalIndex);

        ByteBuffer[] postIndexBuffers = new ByteBuffer[buffers.length - offsetIndex];

        ByteBuffer firstBuffer = buffers[offsetIndex].duplicate();
        firstBuffer.position(internalIndex - offsets[offsetIndex]);
        postIndexBuffers[0] = firstBuffer;
        int j = 1;
        for (int i = (offsetIndex + 1); i < buffers.length; ++i) {
            postIndexBuffers[j++] = buffers[i].duplicate();
        }

        return postIndexBuffers;
    }

    private int getOffsetIndex(int offset) {
        final int i = Arrays.binarySearch(offsets, offset);
        return i < 0 ? (-(i + 1)) - 1 : i;
    }
}
