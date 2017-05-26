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

package org.elasticsearch.transport.nio.channel;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.transport.nio.ByteBufferReference;
import org.elasticsearch.transport.nio.CompositeByteBufferReference;
import org.elasticsearch.transport.nio.TcpReadHandler;

import java.io.IOException;
import java.nio.ByteBuffer;

public class TcpReadContext implements ReadContext {

    private static final int DEFAULT_READ_LENGTH = 1 << 14;

    private final TcpReadHandler handler;
    private final NioSocketChannel channel;
    private final TcpFrameDecoder frameDecoder;
    private final CompositeByteBufferReference references;

    public TcpReadContext(NioSocketChannel channel, TcpReadHandler handler) {
        this(channel, handler, new TcpFrameDecoder());
    }

    public TcpReadContext(NioSocketChannel channel, TcpReadHandler handler, TcpFrameDecoder frameDecoder) {
        this.handler = handler;
        this.channel = channel;
        this.frameDecoder = frameDecoder;
        this.references = new CompositeByteBufferReference(ByteBufferReference.heapBuffer(new BytesArray(new byte[DEFAULT_READ_LENGTH])));
    }

    @Override
    public int read() throws IOException {
        int diff = Math.min(DEFAULT_READ_LENGTH / 2 - references.getWriteRemaining(), Integer.MAX_VALUE);
        if (diff > 0) {
            this.references.addBuffer(ByteBufferReference.heapBuffer(new BytesArray(new byte[DEFAULT_READ_LENGTH])));
        }

        ByteBuffer[] buffers = references.getWriteByteBuffers();

        int bytesRead;
        if (buffers.length == 1) {
            bytesRead = channel.read(buffers[0]);
        } else {
            // The buffers are bounded by Integer.MAX_VALUE
            bytesRead = (int) channel.vectorizedRead(buffers);
        }

        if (bytesRead == -1) {
            return bytesRead;
        }

        references.incrementWrite(bytesRead);

        BytesReference message;
        while ((message = frameDecoder.decode(references, references.getWriteIndex())) != null) {
            references.dropUpTo(message.length());

            try {
                message = message.slice(6, message.length() - 6);
                handler.handleMessage(message, channel, channel.getProfile(), message.length());
            } catch (Exception e) {
                handler.handleException(channel, e);
            }
        }

        return bytesRead;
    }
}
