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

package org.elasticsearch.transport.netty;

import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;
import org.elasticsearch.util.io.ThrowableObjectInputStream;
import org.elasticsearch.util.io.stream.HandlesStreamInput;
import org.elasticsearch.util.io.stream.StreamInput;
import org.elasticsearch.util.io.stream.Streamable;
import org.elasticsearch.util.logging.ESLogger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.*;

import java.io.IOException;

import static org.elasticsearch.transport.Transport.Helper.*;

/**
 * @author kimchy (shay.banon)
 */
public class MessageChannelHandler extends SimpleChannelUpstreamHandler {

    private final ESLogger logger;

    private final ThreadPool threadPool;

    private final TransportServiceAdapter transportServiceAdapter;

    private final NettyTransport transport;

    public MessageChannelHandler(NettyTransport transport, ESLogger logger) {
        this.threadPool = transport.threadPool();
        this.transportServiceAdapter = transport.transportServiceAdapter();
        this.transport = transport;
        this.logger = logger;
    }

    @Override public void writeComplete(ChannelHandlerContext ctx, WriteCompletionEvent e) throws Exception {
        transportServiceAdapter.sent(e.getWrittenAmount());
        super.writeComplete(ctx, e);
    }

    @Override public void messageReceived(ChannelHandlerContext ctx, MessageEvent event) throws Exception {
        ChannelBuffer buffer = (ChannelBuffer) event.getMessage();

        int size = buffer.getInt(buffer.readerIndex() - 4);

        transportServiceAdapter.received(size);

        int markedReaderIndex = buffer.readerIndex();
        int expectedIndexReader = markedReaderIndex + size;

        StreamInput streamIn = new ChannelBufferStreamInput(buffer);
        streamIn = HandlesStreamInput.Cached.cached(streamIn);

        long requestId = buffer.readLong();
        byte status = buffer.readByte();
        boolean isRequest = isRequest(status);

        TransportResponseHandler handler = null;
        if (isRequest) {
            handleRequest(event, streamIn, requestId);
        } else {
            handler = transportServiceAdapter.remove(requestId);
            // ignore if its null, the adapter logs it
            if (handler != null) {
                if (isError(status)) {
                    handlerResponseError(streamIn, handler);
                } else {
                    handleResponse(streamIn, handler);
                }
            } else {
                // if its null, skip those bytes
                buffer.readerIndex(markedReaderIndex + size);
            }
        }
        if (buffer.readerIndex() < expectedIndexReader) {
            logger.warn("Message not fully read for [{}] and handler {}, resetting", requestId, handler);
            buffer.readerIndex(expectedIndexReader);
        }
    }

    private void handleResponse(StreamInput buffer, final TransportResponseHandler handler) {
        final Streamable streamable = handler.newInstance();
        try {
            streamable.readFrom(buffer);
        } catch (Exception e) {
            handleException(handler, new TransportSerializationException("Failed to deserialize response of type [" + streamable.getClass().getName() + "]", e));
            return;
        }
        if (handler.spawn()) {
            threadPool.execute(new Runnable() {
                @SuppressWarnings({"unchecked"}) @Override public void run() {
                    try {
                        handler.handleResponse(streamable);
                    } catch (Exception e) {
                        handleException(handler, new ResponseHandlerFailureTransportException("Failed to handler response", e));
                    }
                }
            });
        } else {
            try {
                //noinspection unchecked
                handler.handleResponse(streamable);
            } catch (Exception e) {
                handleException(handler, new ResponseHandlerFailureTransportException("Failed to handler response", e));
            }
        }
    }

    private void handlerResponseError(StreamInput buffer, final TransportResponseHandler handler) {
        Throwable error;
        try {
            ThrowableObjectInputStream ois = new ThrowableObjectInputStream(buffer);
            error = (Throwable) ois.readObject();
        } catch (Exception e) {
            error = new TransportSerializationException("Failed to deserialize exception response from stream", e);
        }
        handleException(handler, error);
    }

    private void handleException(final TransportResponseHandler handler, Throwable error) {
        if (!(error instanceof RemoteTransportException)) {
            error = new RemoteTransportException("None remote transport exception", error);
        }
        final RemoteTransportException rtx = (RemoteTransportException) error;
        if (handler.spawn()) {
            threadPool.execute(new Runnable() {
                @Override public void run() {
                    try {
                        handler.handleException(rtx);
                    } catch (Exception e) {
                        logger.error("Failed to handle exception response", e);
                    }
                }
            });
        } else {
            handler.handleException(rtx);
        }
    }

    private void handleRequest(MessageEvent event, StreamInput buffer, long requestId) throws IOException {
        final String action = buffer.readUTF();

        final NettyTransportChannel transportChannel = new NettyTransportChannel(transport, action, event.getChannel(), requestId);
        try {
            final TransportRequestHandler handler = transportServiceAdapter.handler(action);
            if (handler == null) {
                logger.warn("No handler found for action [{}]", action);
            }
            final Streamable streamable = handler.newInstance();
            streamable.readFrom(buffer);
            if (handler.spawn()) {
                threadPool.execute(new Runnable() {
                    @SuppressWarnings({"unchecked"}) @Override public void run() {
                        try {
                            handler.messageReceived(streamable, transportChannel);
                        } catch (Throwable e) {
                            try {
                                transportChannel.sendResponse(e);
                            } catch (IOException e1) {
                                logger.warn("Failed to send error message back to client for action [" + action + "]", e1);
                                logger.warn("Actual Exception", e);
                            }
                        }
                    }
                });
            } else {
                //noinspection unchecked
                handler.messageReceived(streamable, transportChannel);
            }
        } catch (Exception e) {
            try {
                transportChannel.sendResponse(e);
            } catch (IOException e1) {
                logger.warn("Failed to send error message back to client for action [" + action + "]", e);
                logger.warn("Actual Exception", e1);
            }
        }
    }

    @Override public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        transport.exceptionCaught(ctx, e);
    }
}
