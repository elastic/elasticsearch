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

package org.elasticsearch.transport.netty;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.rest.RestStatus;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.jboss.netty.handler.codec.frame.TooLongFrameException;

import java.io.IOException;
import java.io.StreamCorruptedException;

/**
 */
public class SizeHeaderFrameDecoder extends FrameDecoder {

    private static final long NINETY_PER_HEAP_SIZE = (long) (JvmInfo.jvmInfo().getMem().getHeapMax().bytes() * 0.9);

    @Override
    protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) throws Exception {
        if (buffer.readableBytes() < 6) {
            return null;
        }

        int readerIndex = buffer.readerIndex();
        if (buffer.getByte(readerIndex) != 'E' || buffer.getByte(readerIndex + 1) != 'S') {
            // special handling for what is probably HTTP
            if (bufferStartsWith(buffer, readerIndex, "GET ") ||
                bufferStartsWith(buffer, readerIndex, "POST ") ||
                bufferStartsWith(buffer, readerIndex, "PUT ") ||
                bufferStartsWith(buffer, readerIndex, "HEAD ") ||
                bufferStartsWith(buffer, readerIndex, "DELETE ") ||
                bufferStartsWith(buffer, readerIndex, "OPTIONS ") ||
                bufferStartsWith(buffer, readerIndex, "PATCH ") ||
                bufferStartsWith(buffer, readerIndex, "TRACE ")) {

                throw new HttpOnTransportException("This is not a HTTP port");
            }

            // we have 6 readable bytes, show 4 (should be enough)
            throw new StreamCorruptedException("invalid internal transport message format, got ("
                    + Integer.toHexString(buffer.getByte(readerIndex) & 0xFF) + ","
                    + Integer.toHexString(buffer.getByte(readerIndex + 1) & 0xFF) + ","
                    + Integer.toHexString(buffer.getByte(readerIndex + 2) & 0xFF) + ","
                    + Integer.toHexString(buffer.getByte(readerIndex + 3) & 0xFF) + ")");
        }

        int dataLen = buffer.getInt(buffer.readerIndex() + 2);
        if (dataLen == NettyHeader.PING_DATA_SIZE) {
            // discard the messages we read and continue, this is achieved by skipping the bytes
            // and returning null
            buffer.skipBytes(6);
            return null;
        }
        if (dataLen <= 0) {
            throw new StreamCorruptedException("invalid data length: " + dataLen);
        }
        // safety against too large frames being sent
        if (dataLen > NINETY_PER_HEAP_SIZE) {
            throw new TooLongFrameException(
                    "transport content length received [" + new ByteSizeValue(dataLen) + "] exceeded [" + new ByteSizeValue(NINETY_PER_HEAP_SIZE) + "]");
        }

        if (buffer.readableBytes() < dataLen + 6) {
            return null;
        }
        buffer.skipBytes(6);
        return buffer;
    }

    private boolean bufferStartsWith(ChannelBuffer buffer, int readerIndex, String method) {
        char[] chars = method.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            if (buffer.getByte(readerIndex + i) != chars[i]) {
                return false;
            }
        }

        return true;
    }

    /**
     * A helper exception to mark an incoming connection as potentially being HTTP
     * so an appropriate error code can be returned
     */
    public static class HttpOnTransportException extends ElasticsearchException {

        public HttpOnTransportException(String msg) {
            super(msg);
        }

        @Override
        public RestStatus status() {
            return RestStatus.BAD_REQUEST;
        }

        public HttpOnTransportException(StreamInput in) throws IOException{
            super(in);
        }
    }
}