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

package org.elasticsearch.transport.netty4;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.TooLongFrameException;
import org.elasticsearch.transport.TcpHeader;
import org.elasticsearch.transport.TcpTransport;

import java.util.List;

final class Netty4SizeHeaderFrameDecoder extends ByteToMessageDecoder {

    private static final int HEADER_SIZE = TcpHeader.MARKER_BYTES_SIZE + TcpHeader.MESSAGE_LENGTH_SIZE;

    {
        setCumulator(COMPOSITE_CUMULATOR);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        try {
            while (in.readableBytes() >= HEADER_SIZE) {
                int messageLength = TcpTransport.readMessageLength(Netty4Utils.toBytesReference(in));
                if (messageLength == -1) {
                    break;
                } else {
                    int messageLengthWithHeader = messageLength + HEADER_SIZE;
                    // If the message length is greater than the network bytes available, we have not read a complete frame.
                    if (messageLengthWithHeader > in.readableBytes()) {
                        break;
                    } else {
                        final int readerIndex = in.readerIndex();
                        final ByteBuf message = in.retainedSlice(readerIndex + HEADER_SIZE, messageLength);
                        out.add(message);
                        in.readerIndex(readerIndex + messageLengthWithHeader);
                    }
                }
            }
        } catch (IllegalArgumentException ex) {
            throw new TooLongFrameException(ex);
        }
    }

}
