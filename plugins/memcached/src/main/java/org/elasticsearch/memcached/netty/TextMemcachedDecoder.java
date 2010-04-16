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
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.util.Unicode;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

import java.io.StreamCorruptedException;
import java.util.regex.Pattern;

/**
 * @author kimchy (shay.banon)
 */
public class TextMemcachedDecoder extends FrameDecoder {

    private final Pattern lineSplit = Pattern.compile(" +");

    public static final byte CR = 13;
    public static final byte LF = 10;
    public static final byte[] CRLF = new byte[]{CR, LF};

    public static enum Reply {
        STORED, NOT_STORED, EXISTS, NOT_FOUND, DELETED, STAT, VALUE, END, OK, VERSION,
        ERROR, CLIENT_ERROR, SERVER_ERROR;

        private final byte[] bytes;

        Reply() {
            this.bytes = this.toString().getBytes();
        }

        public byte[] bytes() {
            return bytes;
        }
    }

    private volatile StringBuffer sb = new StringBuffer();

    private volatile MemcachedRestRequest request;

    public TextMemcachedDecoder() {
        super(false);
    }

    @Override protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) throws Exception {
        MemcachedRestRequest request = this.request;
        if (request == null) {
            buffer.markReaderIndex();
            // need to read a header
            boolean done = false;
            StringBuffer sb = this.sb;
            int readableBytes = buffer.readableBytes();
            for (int i = 0; i < readableBytes; i++) {
                byte next = buffer.readByte();
                if (next == CR) {
                    next = buffer.readByte();
                    if (next == LF) {
                        done = true;
                        break;
                    }
                } else if (next == LF) {
                    done = true;
                    break;
                } else {
                    sb.append((char) next);
                }
            }
            if (!done) {
                buffer.resetReaderIndex();
                return null;
            }

            String[] args = lineSplit.split(sb);
            // we read the text, clear it
            sb.setLength(0);

            String cmd = args[0];
            String uri = args[1];
            if ("get".equals(cmd)) {
                request = new MemcachedRestRequest(RestRequest.Method.GET, uri, -1, false);
                if (args.length > 3) {
                    request.setData(Unicode.fromStringAsBytes(args[2]));
                }
                return request;
            } else if ("delete".equals(cmd)) {
                request = new MemcachedRestRequest(RestRequest.Method.DELETE, uri, -1, false);
//                if (args.length > 3) {
//                    request.setData(Unicode.fromStringAsBytes(args[2]));
//                }
                return request;
            } else if ("set".equals(cmd)) {
                this.request = new MemcachedRestRequest(RestRequest.Method.POST, uri, Integer.parseInt(args[4]), false);
                buffer.markReaderIndex();
            } else if ("quit".equals(cmd)) {
                channel.disconnect();
            }
        } else {
            if (buffer.readableBytes() < (request.getDataSize() + 2)) {
                return null;
            }
            byte[] data = new byte[request.getDataSize()];
            buffer.readBytes(data, 0, data.length);
            byte next = buffer.readByte();
            if (next == CR) {
                next = buffer.readByte();
                if (next == LF) {
                    request.setData(data);
                    // reset
                    this.request = null;
                    return request;
                } else {
                    this.request = null;
                    throw new StreamCorruptedException("Expecting \r\n after data block");
                }
            } else {
                this.request = null;
                throw new StreamCorruptedException("Expecting \r\n after data block");
            }
        }
        return null;
    }

    @Override public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        this.request = null;
        sb.setLength(0);
        e.getCause().printStackTrace();
    }
}
