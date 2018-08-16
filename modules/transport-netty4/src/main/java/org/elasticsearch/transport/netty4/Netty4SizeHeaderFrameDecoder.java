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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.transport.TcpHeader;
import org.elasticsearch.transport.TcpTransport;

import java.util.List;

final class Netty4SizeHeaderFrameDecoder extends ByteToMessageDecoder {

    private static final int HEADER_SIZE = TcpHeader.MARKER_BYTES_SIZE + TcpHeader.MESSAGE_LENGTH_SIZE;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        try {
            BytesReference networkBytes = Netty4Utils.toBytesReference(in);
            int messageLength = TcpTransport.readMessageLength(networkBytes);
            // If the message length is -1, we have not read a complete header.
            if (messageLength != -1) {
                int messageLengthWithHeader = messageLength + HEADER_SIZE;
                // If the message length is greater than the network bytes available, we have not read a complete frame.
                if (messageLengthWithHeader <= networkBytes.length()) {
                    final ByteBuf message = in.skipBytes(HEADER_SIZE);
                    // 6 bytes would mean it is a ping. And we should ignore.
                    if (messageLengthWithHeader != 6) {
                        out.add(message);
                    }
                }
            }
        } catch (IllegalArgumentException ex) {
            throw new TooLongFrameException(ex);
        }
    }

}
