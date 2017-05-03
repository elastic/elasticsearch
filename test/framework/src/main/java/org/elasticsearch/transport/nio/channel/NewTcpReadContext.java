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
import java.util.ArrayList;
import java.util.LinkedList;

public class NewTcpReadContext implements ReadContext {

    private final TcpFrameDecoder frameDecoder = new TcpFrameDecoder();
    private final TcpReadHandler handler;
    private final NioSocketChannel channel;
    private BytesReference reference;
    private int currentBufferSize;

    public NewTcpReadContext(NioSocketChannel channel, TcpReadHandler handler) {
        this.handler = handler;
        this.channel = channel;
        this.reference = new BytesArray(new byte[frameDecoder.nextReadLength()]);
        this.currentBufferSize = 0;
    }

    @Override
    public int read() throws IOException {
        int diff = frameDecoder.nextReadLength() - reference.length();
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

        // TODO: Do not cast
        int bytesRead = (int) channel.vectorizedRead(buffers);
        if (bytesRead == -1) {
            return bytesRead;
        }
        currentBufferSize += bytesRead;

        BytesReference message;
        while ((message = frameDecoder.decode(reference, currentBufferSize)) != null) {
            reference = reference.slice(message.length(), reference.length() - message.length());
            currentBufferSize -= message.length();
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
