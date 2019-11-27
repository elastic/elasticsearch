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
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.TooLongFrameException;
import org.elasticsearch.transport.TcpHeader;
import org.elasticsearch.transport.TcpTransport;

import java.util.List;

final class Netty4SizeHeaderFrameDecoder extends ByteToMessageDecoder {

    private static final int HEADER_SIZE = TcpHeader.MARKER_BYTES_SIZE + TcpHeader.MESSAGE_LENGTH_SIZE;

    // TODO: Probably remove
    {
        setCumulator(COMPOSITE_CUMULATOR);
    }

    private int remainingBytes = 0;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        try {
            if (isBeginningOfMessage()) {
                if (in.readableBytes() < HEADER_SIZE) {
                    return;
                } else {
                    int messageLength = TcpTransport.readMessageLength(Netty4Utils.toBytesReference(in));
                    if (messageLength < 0) {
                        remainingBytes = 0;
                        throw new DecoderException("Invalid message length: " + remainingBytes);
                    } else {
                        remainingBytes = messageLength + HEADER_SIZE;
                    }
                }
            }
        } catch (IllegalArgumentException ex) {
            remainingBytes = 0;
            throw new TooLongFrameException(ex);
        }

        int bytesToConsume = Math.min(in.readableBytes(), remainingBytes);
        out.add(in.retainedSlice(in.readerIndex(), bytesToConsume));
        in.readerIndex(in.readerIndex() + bytesToConsume);
        remainingBytes = remainingBytes - bytesToConsume;
    }

    private boolean isBeginningOfMessage() {
        return remainingBytes == 0;
    }
}
