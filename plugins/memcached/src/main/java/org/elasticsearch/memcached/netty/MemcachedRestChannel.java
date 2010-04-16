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

import org.elasticsearch.memcached.MemcachedRestRequest;
import org.elasticsearch.memcached.MemcachedTransportException;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.util.Bytes;
import org.elasticsearch.util.Unicode;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;

/**
 * @author kimchy (shay.banon)
 */
public class MemcachedRestChannel implements RestChannel {

    public static final ChannelBuffer CRLF = ChannelBuffers.copiedBuffer("\r\n", "US-ASCII");
    private static final ChannelBuffer VALUE = ChannelBuffers.copiedBuffer("VALUE ", "US-ASCII");
    private static final ChannelBuffer EXISTS = ChannelBuffers.copiedBuffer("EXISTS\r\n", "US-ASCII");
    private static final ChannelBuffer NOT_FOUND = ChannelBuffers.copiedBuffer("NOT_FOUND\r\n", "US-ASCII");
    private static final ChannelBuffer NOT_STORED = ChannelBuffers.copiedBuffer("NOT_STORED\r\n", "US-ASCII");
    private static final ChannelBuffer STORED = ChannelBuffers.copiedBuffer("STORED\r\n", "US-ASCII");
    private static final ChannelBuffer DELETED = ChannelBuffers.copiedBuffer("DELETED\r\n", "US-ASCII");
    private static final ChannelBuffer END = ChannelBuffers.copiedBuffer("END\r\n", "US-ASCII");
    private static final ChannelBuffer OK = ChannelBuffers.copiedBuffer("OK\r\n", "US-ASCII");
    private static final ChannelBuffer ERROR = ChannelBuffers.copiedBuffer("ERROR\r\n", "US-ASCII");
    private static final ChannelBuffer CLIENT_ERROR = ChannelBuffers.copiedBuffer("CLIENT_ERROR\r\n", "US-ASCII");

    private final Channel channel;

    private final MemcachedRestRequest request;

    public MemcachedRestChannel(Channel channel, MemcachedRestRequest request) {
        this.channel = channel;
        this.request = request;
    }

    @Override public void sendResponse(RestResponse response) {
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
