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
import org.elasticsearch.transport.nio.ChannelBuffer;
import org.elasticsearch.transport.nio.ChannelMessage;
import org.elasticsearch.transport.nio.HeapNetworkBytes;
import org.elasticsearch.transport.nio.NetworkBytesReference;
import org.elasticsearch.transport.nio.TcpReadHandler;

import java.io.IOException;
import java.util.function.Supplier;

public class TcpReadContext implements ReadContext {

    private static final int DEFAULT_READ_LENGTH = 1 << 14;

    private final TcpReadHandler handler;
    private final NioSocketChannel channel;
    private final TcpFrameDecoder frameDecoder;
    private final ChannelBuffer channelBuffer;
    private final Supplier<NetworkBytesReference> allocator = () -> HeapNetworkBytes.wrap(new BytesArray(new byte[DEFAULT_READ_LENGTH]));

    public TcpReadContext(NioSocketChannel channel, TcpReadHandler handler) {
        this(channel, handler, new TcpFrameDecoder());
    }

    public TcpReadContext(NioSocketChannel channel, TcpReadHandler handler, TcpFrameDecoder frameDecoder) {
        this.handler = handler;
        this.channel = channel;
        this.frameDecoder = frameDecoder;
        this.channelBuffer = new ChannelBuffer(allocator.get());
    }

    @Override
    public int read() throws IOException {
        // TODO: Decide when to start adding more bytes. We do not actually want to wait until buffer is exhausted.
        if (channelBuffer.hasWriteRemaining() == false) {
            channelBuffer.addBuffer(allocator.get());
        }

        int bytesRead = channel.read(channelBuffer);

        if (bytesRead == -1) {
            return bytesRead;
        }

        ChannelMessage message;

        // Frame decoder will throw an exception if the message is improperly formatted, the header is incorrect,
        // or the message is corrupted
        while ((message = frameDecoder.decode(channelBuffer)) != null) {
            BytesReference messageBytes = message.getContent();
            int messageLengthWithHeader = messageBytes.length();

            try {
                BytesReference messageWithoutHeader = messageBytes.slice(6, messageBytes.length() - 6);

                // A message length of 6 bytes it is just a ping. Ignore for now.
                if (messageLengthWithHeader != 6) {
                    handler.handleMessage(messageWithoutHeader, channel, channel.getProfile(), messageWithoutHeader.length());
                }
            } catch (Exception e) {
                handler.handleException(channel, e);
            } finally {
                message.close();
            }
        }

        return bytesRead;
    }

    @Override
    public void close() {
        channelBuffer.close();
    }
}
