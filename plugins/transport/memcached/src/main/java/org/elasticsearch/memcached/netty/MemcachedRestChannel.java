/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.memcached.netty;

import org.elasticsearch.common.Bytes;
import org.elasticsearch.common.Unicode;
import org.elasticsearch.common.netty.buffer.ChannelBuffer;
import org.elasticsearch.common.netty.buffer.ChannelBuffers;
import org.elasticsearch.common.netty.channel.Channel;
import org.elasticsearch.memcached.MemcachedRestRequest;
import org.elasticsearch.memcached.MemcachedTransportException;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;

import java.nio.charset.Charset;

/**
 * @author kimchy (shay.banon)
 */
public class MemcachedRestChannel implements RestChannel {

    public static final ChannelBuffer CRLF = ChannelBuffers.copiedBuffer("\r\n", Charset.forName("US-ASCII"));
    private static final ChannelBuffer VALUE = ChannelBuffers.copiedBuffer("VALUE ", Charset.forName("US-ASCII"));
    private static final ChannelBuffer EXISTS = ChannelBuffers.copiedBuffer("EXISTS\r\n", Charset.forName("US-ASCII"));
    private static final ChannelBuffer NOT_FOUND = ChannelBuffers.copiedBuffer("NOT_FOUND\r\n", Charset.forName("US-ASCII"));
    private static final ChannelBuffer NOT_STORED = ChannelBuffers.copiedBuffer("NOT_STORED\r\n", Charset.forName("US-ASCII"));
    private static final ChannelBuffer STORED = ChannelBuffers.copiedBuffer("STORED\r\n", Charset.forName("US-ASCII"));
    private static final ChannelBuffer DELETED = ChannelBuffers.copiedBuffer("DELETED\r\n", Charset.forName("US-ASCII"));
    private static final ChannelBuffer END = ChannelBuffers.copiedBuffer("END\r\n", Charset.forName("US-ASCII"));
    private static final ChannelBuffer OK = ChannelBuffers.copiedBuffer("OK\r\n", Charset.forName("US-ASCII"));
    private static final ChannelBuffer ERROR = ChannelBuffers.copiedBuffer("ERROR\r\n", Charset.forName("US-ASCII"));
    private static final ChannelBuffer CLIENT_ERROR = ChannelBuffers.copiedBuffer("CLIENT_ERROR\r\n", Charset.forName("US-ASCII"));

    private final Channel channel;

    private final MemcachedRestRequest request;

    public MemcachedRestChannel(Channel channel, MemcachedRestRequest request) {
        this.channel = channel;
        this.request = request;
    }

    @Override public void sendResponse(RestResponse response) {
        if (request.isBinary()) {
            try {
                ChannelBuffer writeBuffer = ChannelBuffers.dynamicBuffer(24 + request.getUriBytes().length + response.contentLength() + 12);
                writeBuffer.writeByte((byte) 0x81);  // magic
                if (request.method() == RestRequest.Method.GET) {
                    writeBuffer.writeByte((byte) 0x00); // opcode
                } else if (request.method() == RestRequest.Method.POST) {
                    writeBuffer.writeByte((byte) 0x01); // opcode
                } else if (request.method() == RestRequest.Method.DELETE) {
                    writeBuffer.writeByte((byte) 0x04); // opcode
                }
                short keyLength = request.method() == RestRequest.Method.GET ? (short) request.getUriBytes().length : 0;
                writeBuffer.writeShort(keyLength);
                int extrasLength = request.method() == RestRequest.Method.GET ? 4 : 0;
                writeBuffer.writeByte((byte) extrasLength); // extra length = flags + expiry
                writeBuffer.writeByte((byte) 0); // data type unused

                if (response.status().getStatus() >= 500) {
                    // TODO should we use this?
                    writeBuffer.writeShort((short) 0x0A); // status code
                } else {
                    writeBuffer.writeShort((short) 0x0000); // OK
                }

                int dataLength = request.method() == RestRequest.Method.GET ? response.contentLength() : 0;
                writeBuffer.writeInt(dataLength + keyLength + extrasLength); // data length
                writeBuffer.writeInt(request.getOpaque()); // opaque
                writeBuffer.writeLong(0); // cas

                if (extrasLength > 0) {
                    writeBuffer.writeShort((short) 0);
                    writeBuffer.writeShort((short) 0);
                }
                if (keyLength > 0) {
                    writeBuffer.writeBytes(request.getUriBytes());
                }
                if (dataLength > 0) {
                    writeBuffer.writeBytes(response.content(), 0, response.contentLength());
                }
                channel.write(writeBuffer);
            } catch (Exception e) {
                throw new MemcachedTransportException("Failed to write response", e);
            }
        } else {
            if (response.status().getStatus() >= 500) {
                channel.write(ERROR.duplicate());
            } else {
                if (request.method() == RestRequest.Method.POST) {
                    // TODO this is SET, can we send a payload?
                    channel.write(STORED.duplicate());
                } else if (request.method() == RestRequest.Method.DELETE) {
                    channel.write(DELETED.duplicate());
                } else { // GET
                    try {
                        ChannelBuffer writeBuffer = ChannelBuffers.dynamicBuffer(response.contentLength() + 512);
                        writeBuffer.writeBytes(VALUE.duplicate());
                        writeBuffer.writeBytes(Unicode.fromStringAsBytes(request.uri()));
                        writeBuffer.writeByte((byte) ' ');
                        writeBuffer.writeByte((byte) '0');
                        writeBuffer.writeByte((byte) ' ');
                        writeBuffer.writeBytes(Bytes.itoa(response.contentLength()));
                        writeBuffer.writeByte((byte) '\r');
                        writeBuffer.writeByte((byte) '\n');
                        writeBuffer.writeBytes(response.content(), 0, response.contentLength());
                        writeBuffer.writeByte((byte) '\r');
                        writeBuffer.writeByte((byte) '\n');
                        writeBuffer.writeBytes(END.duplicate());
                        channel.write(writeBuffer);
                    } catch (Exception e) {
                        throw new MemcachedTransportException("Failed to write 'get' response", e);
                    }
                }
            }
        }
    }
}
