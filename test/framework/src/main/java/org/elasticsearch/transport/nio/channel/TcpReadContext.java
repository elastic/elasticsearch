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

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.transport.nio.TcpReadHandler;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;

public class TcpReadContext implements ReadContext {

    private final TcpReadHandler handler;
    private final NioSocketChannel channel;
    private final TcpFrameDecoder frameDecoder;
    private BytesReference reference;
    private BytesReference partialMessage;

    public TcpReadContext(NioSocketChannel channel, TcpReadHandler handler) {
        this(channel, handler, new TcpFrameDecoder());
    }

    public TcpReadContext(NioSocketChannel channel, TcpReadHandler handler, TcpFrameDecoder frameDecoder) {
        this.handler = handler;
        this.channel = channel;
        this.frameDecoder = frameDecoder;
        this.reference = new BytesArray(new byte[frameDecoder.nextReadLength()]);
    }

    @Override
    public int read() throws IOException {
        int diff = Math.min(frameDecoder.nextReadLength() - reference.length(), Integer.MAX_VALUE);
        if (diff > 0) {
            reference = new CompositeBytesReference(reference, new BytesArray(new byte[diff]));
        }

        ByteBuffer[] buffers;
        LinkedList<ByteBuffer> linkedBuffers = new LinkedList<>();
        BytesRefIterator byteRefIterator = reference.iterator();
        BytesRef r;
        while ((r = byteRefIterator.next()) != null) {
            linkedBuffers.add(ByteBuffer.wrap(r.bytes, r.offset, r.length));
        }
        buffers = linkedBuffers.toArray(new ByteBuffer[linkedBuffers.size()]);

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

        BytesReference message;
        int currentBufferSize = (partialMessage != null ? partialMessage.length() : 0) + bytesRead;
        while ((message = frameDecoder.decode(combineWithPartial(partialMessage, reference), currentBufferSize)) != null) {
            partialMessage = null;
            int messageLength = message.length();
            reference = reference.slice(message.length(), reference.length() - messageLength);
            currentBufferSize -= messageLength;

            try {
                message = message.slice(6, message.length() - 6);
                handler.handleMessage(message, channel, channel.getProfile(), message.length());
            } catch (Exception e) {
                handler.handleException(channel, e);
            }
        }
        int remainderBytes = currentBufferSize - (partialMessage != null ? partialMessage.length() : 0);
        partialMessage = combineWithPartial(partialMessage, reference.slice(0, remainderBytes));
        reference = reference.slice(bytesRead, reference.length() - bytesRead);

        return bytesRead;
    }

    private BytesReference combineWithPartial(BytesReference partialMessage, BytesReference reference) {
        if (partialMessage == null || partialMessage.length() == 0) {
            return reference;
        } else if (reference.length() == 0) {
            return partialMessage;
        } else {
            return new CompositeBytesReference(this.partialMessage, this.reference);
        }
    }
}
