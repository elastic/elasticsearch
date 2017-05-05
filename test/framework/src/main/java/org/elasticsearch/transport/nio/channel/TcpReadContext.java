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
import org.elasticsearch.transport.nio.HeapByteBuffer;
import org.elasticsearch.transport.nio.TcpReadHandler;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.LinkedList;

public class TcpReadContext implements ReadContext {

    private final TcpReadHandler handler;
    private final NioSocketChannel channel;
    private final TcpFrameDecoder frameDecoder;
    private LinkedList<HeapByteBuffer> references = new LinkedList<>();

    public TcpReadContext(NioSocketChannel channel, TcpReadHandler handler) {
        this(channel, handler, new TcpFrameDecoder());
    }

    public TcpReadContext(NioSocketChannel channel, TcpReadHandler handler, TcpFrameDecoder frameDecoder) {
        this.handler = handler;
        this.channel = channel;
        this.frameDecoder = frameDecoder;
        this.references.add(new HeapByteBuffer(new BytesArray(new byte[frameDecoder.nextReadLength()])));
    }

    @Override
    public int read() throws IOException {
//        int diff = Math.min(frameDecoder.nextReadLength() - reference.length(), Integer.MAX_VALUE);
//        if (diff > 0) {
//            this.references.add(new HeapByteBuffer(new BytesArray(new byte[diff])));
//        }

        ByteBuffer[] buffers = new ByteBuffer[references.size()];


        int i = 0;
        int currentBytes = 0;
        for (HeapByteBuffer buffer : references) {
            int writeIndex = buffer.getWriteIndex();
            currentBytes += writeIndex;
            if (buffer.length() != writeIndex) {
                buffers[i++] = buffer.getWriteByteBuffer();
            }
        }

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
        int currentBufferSize = currentBytes + bytesRead;
        BytesReference composite = new CompositeBytesReference(references.toArray(new BytesReference[references.size()]));
        int bytesMessagesProduced = 0;
        while ((message = frameDecoder.decode(composite, currentBufferSize)) != null) {
            int messageLength = message.length();
            bytesMessagesProduced += messageLength;
            composite = composite.slice(message.length(), composite.length() - messageLength);
            currentBufferSize -= messageLength;

            try {
                message = message.slice(6, message.length() - 6);
                handler.handleMessage(message, channel, channel.getProfile(), message.length());
            } catch (Exception e) {
                handler.handleException(channel, e);
            }
        }
//        int remainderBytes = currentBufferSize - (partialMessage != null ? partialMessage.length() : 0);

        HeapByteBuffer buffer;
        while ((buffer = references.peek()) != null) {
            int bufferLength = buffer.length();
            if (bufferLength <= bytesMessagesProduced) {
                references.poll();
                bytesMessagesProduced -= bufferLength;
            } else {
                break;
            }
        }

        return bytesRead;
    }

//    private BytesReference combineWithPartial(BytesReference partialMessage, BytesReference reference) {
//        if (partialMessage == null || partialMessage.length() == 0) {
//            return reference;
//        } else if (reference.length() == 0) {
//            return partialMessage;
//        } else {
//            return new CompositeBytesReference(this.partialMessage, this.reference);
//        }
//    }

    private class CompositeHeapByteBuffer extends BytesReference {

        @Override
        public byte get(int index) {
            return 0;
        }

        @Override
        public int length() {
            return 0;
        }

        @Override
        public BytesReference slice(int from, int length) {
            return null;
        }

        @Override
        public BytesRef toBytesRef() {
            return null;
        }

        @Override
        public long ramBytesUsed() {
            return 0;
        }
    }
}
