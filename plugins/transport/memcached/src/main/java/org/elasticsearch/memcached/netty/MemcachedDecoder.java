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

import org.elasticsearch.Version;
import org.elasticsearch.common.Unicode;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.netty.buffer.ChannelBuffer;
import org.elasticsearch.common.netty.buffer.ChannelBuffers;
import org.elasticsearch.common.netty.channel.Channel;
import org.elasticsearch.common.netty.channel.ChannelHandlerContext;
import org.elasticsearch.common.netty.channel.ExceptionEvent;
import org.elasticsearch.common.netty.handler.codec.frame.FrameDecoder;
import org.elasticsearch.memcached.MemcachedRestRequest;
import org.elasticsearch.rest.RestRequest;

import java.io.StreamCorruptedException;
import java.util.regex.Pattern;

/**
 * @author kimchy (shay.banon)
 */
public class MemcachedDecoder extends FrameDecoder {

    private final ESLogger logger;

    private final Pattern lineSplit = Pattern.compile(" +");

    public static final byte CR = 13;
    public static final byte LF = 10;
    public static final byte[] CRLF = new byte[]{CR, LF};

    private volatile StringBuffer sb = new StringBuffer();

    private volatile MemcachedRestRequest request;
    private volatile boolean ending = false;

    public MemcachedDecoder(ESLogger logger) {
        super(false);
        this.logger = logger;
    }

    @Override protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) throws Exception {
        MemcachedRestRequest request = this.request;
        if (request == null) {
            buffer.markReaderIndex();

            if (buffer.readableBytes() < 1) {
                return null;
            }
            short magic = buffer.readUnsignedByte();
            if (magic == 0x80) {
                if (buffer.readableBytes() < 23) {
                    buffer.resetReaderIndex(); // but back magic
                    return null;
                }
                short opcode = buffer.readUnsignedByte();
                short keyLength = buffer.readShort();
                short extraLength = buffer.readUnsignedByte();
                short dataType = buffer.readUnsignedByte();   // unused
                short reserved = buffer.readShort(); // unused
                int totalBodyLength = buffer.readInt();
                int opaque = buffer.readInt();
                long cas = buffer.readLong();

                // we want the whole of totalBodyLength; otherwise, keep waiting.
                if (buffer.readableBytes() < totalBodyLength) {
                    buffer.resetReaderIndex();
                    return null;
                }

                buffer.skipBytes(extraLength); // get extras, can be empty

                if (opcode == 0x00) { // GET
                    byte[] key = new byte[keyLength];
                    buffer.readBytes(key);
                    String uri = Unicode.fromBytes(key);
                    request = new MemcachedRestRequest(RestRequest.Method.GET, uri, key, -1, true);
                    request.setOpaque(opaque);
                    return request;
                } else if (opcode == 0x04) { // DELETE
                    byte[] key = new byte[keyLength];
                    buffer.readBytes(key);
                    String uri = Unicode.fromBytes(key);
                    request = new MemcachedRestRequest(RestRequest.Method.DELETE, uri, key, -1, true);
                    request.setOpaque(opaque);
                    return request;
                } else if (opcode == 0x01/* || opcode == 0x11*/) { // SET
                    byte[] key = new byte[keyLength];
                    buffer.readBytes(key);
                    String uri = Unicode.fromBytes(key);
                    // the remainder of the message -- that is, totalLength - (keyLength + extraLength) should be the payload
                    int size = totalBodyLength - keyLength - extraLength;
                    request = new MemcachedRestRequest(RestRequest.Method.POST, uri, key, size, true);
                    request.setOpaque(opaque);
                    byte[] data = new byte[size];
                    buffer.readBytes(data, 0, size);
                    request.setData(data);
                    request.setQuiet(opcode == 0x11);
                    return request;
                } else if (opcode == 0x0A || opcode == 0x10) { // NOOP or STATS
                    // TODO once we support setQ we need to wait for them to flush
                    ChannelBuffer writeBuffer = ChannelBuffers.dynamicBuffer(24);
                    writeBuffer.writeByte(0x81);  // magic
                    writeBuffer.writeByte(opcode); // opcode
                    writeBuffer.writeShort(0); // key length
                    writeBuffer.writeByte(0); // extra length = flags + expiry
                    writeBuffer.writeByte(0); // data type unused
                    writeBuffer.writeShort(0x0000); // OK
                    writeBuffer.writeInt(0); // data length
                    writeBuffer.writeInt(opaque); // opaque
                    writeBuffer.writeLong(0); // cas
                    channel.write(writeBuffer);
                    return MemcachedDispatcher.IGNORE_REQUEST;
                } else if (opcode == 0x07) { // QUIT
                    channel.disconnect();
                } else {
                    logger.error("Unsupported opcode [0x{}], ignoring and closing connection", Integer.toHexString(opcode));
                    channel.disconnect();
                    return null;
                }
            } else {
                buffer.resetReaderIndex(); // reset to get to the first byte
                // need to read a header
                boolean done = false;
                StringBuffer sb = this.sb;
                int readableBytes = buffer.readableBytes();
                for (int i = 0; i < readableBytes; i++) {
                    byte next = buffer.readByte();
                    if (!ending && next == CR) {
                        ending = true;
                    } else if (ending && next == LF) {
                        ending = false;
                        done = true;
                        break;
                    } else if (ending) {
                        logger.error("Corrupt stream, expected LF, found [0x{}]", Integer.toHexString(next));
                        throw new StreamCorruptedException("Expecting LF after CR");
                    } else {
                        sb.append((char) next);
                    }
                }
                if (!done) {
                    // let's keep the buffer and bytes read
//                    buffer.discardReadBytes();
                    buffer.markReaderIndex();
                    return null;
                }

                String[] args = lineSplit.split(sb);
                // we read the text, clear it
                sb.setLength(0);

                String cmd = args[0];
                if ("get".equals(cmd)) {
                    request = new MemcachedRestRequest(RestRequest.Method.GET, args[1], null, -1, false);
                    if (args.length > 3) {
                        request.setData(Unicode.fromStringAsBytes(args[2]));
                    }
                    return request;
                } else if ("delete".equals(cmd)) {
                    request = new MemcachedRestRequest(RestRequest.Method.DELETE, args[1], null, -1, false);
                    //                if (args.length > 3) {
                    //                    request.setData(Unicode.fromStringAsBytes(args[2]));
                    //                }
                    return request;
                } else if ("set".equals(cmd)) {
                    this.request = new MemcachedRestRequest(RestRequest.Method.POST, args[1], null, Integer.parseInt(args[4]), false);
                    buffer.markReaderIndex();
                } else if ("version".equals(cmd)) { // sent as a noop
                    byte[] bytes = Version.CURRENT.toString().getBytes();
                    ChannelBuffer writeBuffer = ChannelBuffers.dynamicBuffer(bytes.length);
                    writeBuffer.writeBytes(bytes);
                    channel.write(writeBuffer);
                    return MemcachedDispatcher.IGNORE_REQUEST;
                } else if ("quit".equals(cmd)) {
                    if (channel.isConnected()) { // we maybe in the process of clearing the queued bits
                        channel.disconnect();
                    }
                } else {
                    logger.error("Unsupported command [{}], ignoring and closing connection", cmd);
                    if (channel.isConnected()) { // we maybe in the process of clearing the queued bits
                        channel.disconnect();
                    }
                    return null;
                }
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
                    throw new StreamCorruptedException("Expecting separator after data block");
                }
            } else {
                this.request = null;
                throw new StreamCorruptedException("Expecting separator after data block");
            }
        }
        return null;
    }

    @Override public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        this.request = null;
        this.ending = false;
        this.sb.setLength(0);

        if (ctx.getChannel().isConnected()) {
            ctx.getChannel().disconnect();
        }

        logger.error("caught exception on memcached decoder", e);
    }
}