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
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.internal.StringUtil;
import org.elasticsearch.Version;
import org.elasticsearch.common.compress.Compressor;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.transport.TcpHeader;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.TransportStatus;

import java.io.IOException;
import java.io.UncheckedIOException;

final class ESLoggingHandler extends LoggingHandler {

    ESLoggingHandler() {
        super(LogLevel.TRACE);
    }

    @Override
    protected String format(final ChannelHandlerContext ctx, final String eventName, final Object arg) {
        if (arg instanceof ByteBuf) {
            try {
                return format(ctx, eventName, (ByteBuf) arg);
            } catch (final Exception e) {
                // we really do not want to allow a bug in the formatting handling to escape
                logger.trace("an exception occurred formatting a trace message", e);
                // we are going to let this be formatted via the default formatting
                return super.format(ctx, eventName, arg);
            }
        } else {
            return super.format(ctx, eventName, arg);
        }
    }

    private static final int MESSAGE_LENGTH_OFFSET = TcpHeader.MARKER_BYTES_SIZE;
    private static final int REQUEST_ID_OFFSET = MESSAGE_LENGTH_OFFSET + TcpHeader.MESSAGE_LENGTH_SIZE;
    private static final int STATUS_OFFSET = REQUEST_ID_OFFSET + TcpHeader.REQUEST_ID_SIZE;
    private static final int VERSION_ID_OFFSET = STATUS_OFFSET + TcpHeader.STATUS_SIZE;
    private static final int ACTION_OFFSET = VERSION_ID_OFFSET + TcpHeader.VERSION_ID_SIZE;

    private String format(final ChannelHandlerContext ctx, final String eventName, final ByteBuf arg) throws IOException {
        final int readableBytes = arg.readableBytes();
        if (readableBytes == 0) {
            return super.format(ctx, eventName, arg);
        } else if (readableBytes >= 2) {
            final StringBuilder sb = new StringBuilder();
            sb.append(ctx.channel().toString());
            final int offset = arg.readerIndex();
            // this might be an ES message, check the header
            if (arg.getByte(offset) == (byte) 'E' && arg.getByte(offset + 1) == (byte) 'S') {
                if (readableBytes == TcpHeader.MARKER_BYTES_SIZE + TcpHeader.MESSAGE_LENGTH_SIZE) {
                    final int length = arg.getInt(offset + MESSAGE_LENGTH_OFFSET);
                    if (length == TcpTransport.PING_DATA_SIZE) {
                        sb.append(" [ping]").append(' ').append(eventName).append(": ").append(readableBytes).append('B');
                        return sb.toString();
                    }
                }
                else if (readableBytes >= TcpHeader.HEADER_SIZE) {
                    // we are going to try to decode this as an ES message
                    final int length = arg.getInt(offset + MESSAGE_LENGTH_OFFSET);
                    final long requestId = arg.getLong(offset + REQUEST_ID_OFFSET);
                    final byte status = arg.getByte(offset + STATUS_OFFSET);
                    final boolean isRequest = TransportStatus.isRequest(status);
                    final String type = isRequest ? "request" : "response";
                    final String version = Version.fromId(arg.getInt(offset + VERSION_ID_OFFSET)).toString();
                    sb.append(" [length: ").append(length);
                    sb.append(", request id: ").append(requestId);
                    sb.append(", type: ").append(type);
                    sb.append(", version: ").append(version);
                    if (isRequest) {
                        // it looks like an ES request, try to decode the action
                        final int remaining = readableBytes - ACTION_OFFSET;
                        final ByteBuf slice = arg.slice(offset + ACTION_OFFSET, remaining);
                        // the stream might be compressed
                        try (StreamInput in = in(status, slice, remaining)) {
                            // the first bytes in the message is the context headers
                            try (ThreadContext context = new ThreadContext(Settings.EMPTY)) {
                                context.readHeaders(in);
                            }
                            // now we can decode the action name
                            sb.append(", action: ").append(in.readString());
                        }
                    }
                    sb.append(']');
                    sb.append(' ').append(eventName).append(": ").append(readableBytes).append('B');
                    return sb.toString();
                }
            }
        }
        // we could not decode this as an ES message, use the default formatting
        return super.format(ctx, eventName, arg);
    }

    private StreamInput in(final Byte status, final ByteBuf slice, final int remaining) throws IOException {
        final ByteBufStreamInput in = new ByteBufStreamInput(slice, remaining);
        if (TransportStatus.isCompress(status)) {
            final Compressor compressor = CompressorFactory.compressor(Netty4Utils.toBytesReference(slice));
            return compressor.streamInput(in);
        } else {
            return in;
        }
    }

}
