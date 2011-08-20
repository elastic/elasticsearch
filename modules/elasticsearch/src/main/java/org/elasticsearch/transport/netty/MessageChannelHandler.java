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

import org.elasticsearch.common.io.ThrowableObjectInputStream;
import org.elasticsearch.common.io.stream.CachedStreamInput;
import org.elasticsearch.common.io.stream.HandlesStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.netty.buffer.ChannelBuffer;
import org.elasticsearch.common.netty.channel.ChannelHandlerContext;
import org.elasticsearch.common.netty.channel.ExceptionEvent;
import org.elasticsearch.common.netty.channel.MessageEvent;
import org.elasticsearch.common.netty.channel.SimpleChannelUpstreamHandler;
import org.elasticsearch.common.netty.channel.WriteCompletionEvent;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ActionNotFoundTransportException;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.ResponseHandlerFailureTransportException;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportSerializationException;
import org.elasticsearch.transport.TransportServiceAdapter;
import org.elasticsearch.transport.support.TransportStreams;

import java.io.IOException;

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

        transportServiceAdapter.received(size + 4);

        int markedReaderIndex = buffer.readerIndex();
        int expectedIndexReader = markedReaderIndex + size;

        StreamInput streamIn = new ChannelBufferStreamInput(buffer, size);

        long requestId = buffer.readLong();
        byte status = buffer.readByte();
        boolean isRequest = TransportStreams.statusIsRequest(status);

        HandlesStreamInput handlesStream;
        if (TransportStreams.statusIsCompress(status)) {
            handlesStream = CachedStreamInput.cachedHandlesLzf(streamIn);
        } else {
            handlesStream = CachedStreamInput.cachedHandles(streamIn);
        }

        if (isRequest) {
            String action = handleRequest(event, handlesStream, requestId);
            if (buffer.readerIndex() != expectedIndexReader) {
                if (buffer.readerIndex() < expectedIndexReader) {
                    logger.warn("Message not fully read (request) for [{}] and action [{}], resetting", requestId, action);
                } else {
                    logger.warn("Message read past expected size (request) for [{}] and action [{}], resetting", requestId, action);
                }
                buffer.readerIndex(expectedIndexReader);
            }
        } else {
            TransportResponseHandler handler = transportServiceAdapter.remove(requestId);
            // ignore if its null, the adapter logs it
            if (handler != null) {
                if (TransportStreams.statusIsError(status)) {
                    handlerResponseError(handlesStream, handler);
                } else {
                    handleResponse(handlesStream, handler);
                }
            } else {
                // if its null, skip those bytes
                buffer.readerIndex(markedReaderIndex + size);
            }
            if (buffer.readerIndex() != expectedIndexReader) {
                if (buffer.readerIndex() < expectedIndexReader) {
                    logger.warn("Message not fully read (response) for [{}] handler {}, error [{}], resetting", requestId, handler, TransportStreams.statusIsError(status));
                } else {
                    logger.warn("Message read past expected size (response) for [{}] handler {}, error [{}], resetting", requestId, handler, TransportStreams.statusIsError(status));
                }
                buffer.readerIndex(expectedIndexReader);
            }
        }
        handlesStream.cleanHandles();
    }

    private void handleResponse(StreamInput buffer, final TransportResponseHandler handler) {
        final Streamable streamable = handler.newInstance();
        try {
            streamable.readFrom(buffer);
        } catch (Exception e) {
            handleException(handler, new TransportSerializationException("Failed to deserialize response of type [" + streamable.getClass().getName() + "]", e));
            return;
        }
        try {
            if (handler.executor() == ThreadPool.Names.SAME) {
                //noinspection unchecked
                handler.handleResponse(streamable);
            } else {
                threadPool.executor(handler.executor()).execute(new ResponseHandler(handler, streamable));
            }
        } catch (Exception e) {
            handleException(handler, new ResponseHandlerFailureTransportException(e));
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
            error = new RemoteTransportException(error.getMessage(), error);
        }
        final RemoteTransportException rtx = (RemoteTransportException) error;
        if (handler.executor() == ThreadPool.Names.SAME) {
            handler.handleException(rtx);
        } else {
            threadPool.executor(handler.executor()).execute(new Runnable() {
                @Override public void run() {
                    try {
                        handler.handleException(rtx);
                    } catch (Exception e) {
                        logger.error("Failed to handle exception response", e);
                    }
                }
            });
        }
    }

    private String handleRequest(MessageEvent event, StreamInput buffer, long requestId) throws IOException {
        final String action = buffer.readUTF();

        final NettyTransportChannel transportChannel = new NettyTransportChannel(transport, action, event.getChannel(), requestId);
        try {
            final TransportRequestHandler handler = transportServiceAdapter.handler(action);
            if (handler == null) {
                throw new ActionNotFoundTransportException(action);
            }
            final Streamable streamable = handler.newInstance();
            streamable.readFrom(buffer);
            if (handler.executor() == ThreadPool.Names.SAME) {
                //noinspection unchecked
                handler.messageReceived(streamable, transportChannel);
            } else {
                threadPool.executor(handler.executor()).execute(new RequestHandler(handler, streamable, transportChannel, action));
            }
        } catch (Exception e) {
            try {
                transportChannel.sendResponse(e);
            } catch (IOException e1) {
                logger.warn("Failed to send error message back to client for action [" + action + "]", e);
                logger.warn("Actual Exception", e1);
            }
        }
        return action;
    }

    @Override public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        transport.exceptionCaught(ctx, e);
    }

    class ResponseHandler implements Runnable {

        private final TransportResponseHandler handler;
        private final Streamable streamable;

        public ResponseHandler(TransportResponseHandler handler, Streamable streamable) {
            this.handler = handler;
            this.streamable = streamable;
        }

        @SuppressWarnings({"unchecked"}) @Override public void run() {
            try {
                handler.handleResponse(streamable);
            } catch (Exception e) {
                handleException(handler, new ResponseHandlerFailureTransportException(e));
            }
        }
    }

    class RequestHandler implements Runnable {
        private final TransportRequestHandler handler;
        private final Streamable streamable;
        private final NettyTransportChannel transportChannel;
        private final String action;

        public RequestHandler(TransportRequestHandler handler, Streamable streamable, NettyTransportChannel transportChannel, String action) {
            this.handler = handler;
            this.streamable = streamable;
            this.transportChannel = transportChannel;
            this.action = action;
        }

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
    }
}
