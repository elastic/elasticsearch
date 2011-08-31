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
import org.elasticsearch.common.io.stream.CachedStreamOutput;
import org.elasticsearch.common.netty.buffer.ChannelBuffer;
import org.elasticsearch.common.netty.buffer.ChannelBuffers;
import org.elasticsearch.common.netty.channel.Channel;
import org.elasticsearch.common.netty.channel.ChannelFuture;
import org.elasticsearch.common.netty.channel.ChannelFutureListener;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.memcached.MemcachedRestRequest;
import org.elasticsearch.memcached.MemcachedTransportException;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.XContentRestResponse;
import org.elasticsearch.transport.netty.NettyTransport;

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
            if (request.isQuiet() && response.status().getStatus() < 500) {
                // nothing to send and all is well
                return;
            }
            try {
                ChannelBuffer writeBuffer = ChannelBuffers.dynamicBuffer(24 + request.getUriBytes().length + response.contentLength() + 12);
                writeBuffer.writeByte(0x81);  // magic
                if (request.method() == RestRequest.Method.GET) {
                    writeBuffer.writeByte(0x00); // opcode
                } else if (request.method() == RestRequest.Method.POST) {
                    if (request.isQuiet()) {
                        writeBuffer.writeByte(0x11); // opcode
                    } else {
                        writeBuffer.writeByte(0x01); // opcode
                    }
                } else if (request.method() == RestRequest.Method.DELETE) {
                    writeBuffer.writeByte(0x04); // opcode
                }
                short keyLength = request.method() == RestRequest.Method.GET ? (short) request.getUriBytes().length : 0;
                writeBuffer.writeShort(keyLength);
                int extrasLength = request.method() == RestRequest.Method.GET ? 4 : 0;
                writeBuffer.writeByte(extrasLength); // extra length = flags + expiry
                writeBuffer.writeByte(0); // data type unused

                if (response.status().getStatus() >= 500) {
                    // TODO should we use this?
                    writeBuffer.writeShort(0x0A); // status code
                } else {
                    writeBuffer.writeShort(0x0000); // OK
                }

                int dataLength = request.method() == RestRequest.Method.GET ? response.contentLength() : 0;
                writeBuffer.writeInt(dataLength + keyLength + extrasLength); // data length
                writeBuffer.writeInt(request.getOpaque()); // opaque
                writeBuffer.writeLong(0); // cas

                if (extrasLength > 0) {
                    writeBuffer.writeShort(0);
                    writeBuffer.writeShort(0);
                }
                if (keyLength > 0) {
                    writeBuffer.writeBytes(request.getUriBytes());
                }
                ChannelFutureListener releaseContentListener = null;
                if (dataLength > 0) {
                    if (response instanceof XContentRestResponse) {
                        // if its a builder based response, and it was created with a CachedStreamOutput, we can release it
                        // after we write the response, and no need to do an extra copy because its not thread safe
                        XContentBuilder builder = ((XContentRestResponse) response).builder();
                        if (builder.payload() instanceof CachedStreamOutput.Entry) {
                            releaseContentListener = new NettyTransport.CacheFutureListener((CachedStreamOutput.Entry) builder.payload());
                            ChannelBuffer buf = ChannelBuffers.wrappedBuffer(builder.underlyingBytes(), 0, builder.underlyingBytesLength());
                            writeBuffer = ChannelBuffers.wrappedBuffer(writeBuffer, buf);
                        } else if (response.contentThreadSafe()) {
                            ChannelBuffer buf = ChannelBuffers.wrappedBuffer(builder.underlyingBytes(), 0, builder.underlyingBytesLength());
                            writeBuffer = ChannelBuffers.wrappedBuffer(writeBuffer, buf);
                        } else {
                            writeBuffer.writeBytes(response.content(), 0, response.contentLength());
                        }
                    } else {
                        if (response.contentThreadSafe()) {
                            ChannelBuffer buf = ChannelBuffers.wrappedBuffer(response.content(), 0, response.contentLength());
                            writeBuffer = ChannelBuffers.wrappedBuffer(writeBuffer, buf);
                        } else {
                            writeBuffer.writeBytes(response.content(), 0, response.contentLength());
                        }
                    }
                }
                ChannelFuture future = channel.write(writeBuffer);
                if (releaseContentListener != null) {
                    future.addListener(releaseContentListener);
                }
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
                        writeBuffer.writeByte(' ');
                        writeBuffer.writeByte('0');
                        writeBuffer.writeByte(' ');
                        writeBuffer.writeBytes(Bytes.itoa(response.contentLength()));
                        writeBuffer.writeByte('\r');
                        writeBuffer.writeByte('\n');
                        writeBuffer.writeBytes(response.content(), 0, response.contentLength());
                        writeBuffer.writeByte('\r');
                        writeBuffer.writeByte('\n');
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
