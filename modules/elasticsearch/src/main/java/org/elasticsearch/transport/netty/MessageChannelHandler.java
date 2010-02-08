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
import org.elasticsearch.util.io.DataInputInputStream;
import org.elasticsearch.util.io.Streamable;
import org.elasticsearch.util.io.ThrowableObjectInputStream;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.channel.*;
import org.slf4j.Logger;

import java.io.IOException;

import static org.elasticsearch.transport.Transport.Helper.*;

/**
 * @author kimchy (Shay Banon)
 */
@ChannelPipelineCoverage("one")
public class MessageChannelHandler extends SimpleChannelUpstreamHandler {

    private final Logger logger;

    private final ThreadPool threadPool;

    private final TransportServiceAdapter transportServiceAdapter;

    private final NettyTransport transport;

    public MessageChannelHandler(NettyTransport transport, Logger logger) {
        this.threadPool = transport.threadPool();
        this.transportServiceAdapter = transport.transportServiceAdapter();
        this.transport = transport;
        this.logger = logger;
    }

    @Override public void messageReceived(ChannelHandlerContext ctx, MessageEvent event) throws Exception {
        ChannelBufferInputStream buffer = (ChannelBufferInputStream) event.getMessage();

        long requestId = buffer.readLong();
        byte status = buffer.readByte();
        boolean isRequest = isRequest(status);

        if (isRequest) {
            handleRequest(event, buffer, requestId);
        } else {
            final TransportResponseHandler handler = transportServiceAdapter.remove(requestId);
            if (handler == null) {
                throw new ResponseHandlerNotFoundTransportException(requestId);
            }
            if (isError(status)) {
                handlerResponseError(buffer, handler);
            } else {
                handleResponse(buffer, handler);
            }
        }
    }

    private void handleResponse(ChannelBufferInputStream buffer, final TransportResponseHandler handler) {
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

    private void handlerResponseError(ChannelBufferInputStream buffer, final TransportResponseHandler handler) {
        Throwable error;
        try {
            ThrowableObjectInputStream ois = new ThrowableObjectInputStream(new DataInputInputStream(buffer));
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

    private void handleRequest(MessageEvent event, ChannelBufferInputStream buffer, long requestId) throws IOException {
        final String action = buffer.readUTF();

        final NettyTransportChannel transportChannel = new NettyTransportChannel(transport, action, event.getChannel(), requestId);
        try {
            final TransportRequestHandler handler = transportServiceAdapter.handler(action);
            if (handler == null) {
                throw new ActionNotFoundTransportException("Action [" + action + "] not found");
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
