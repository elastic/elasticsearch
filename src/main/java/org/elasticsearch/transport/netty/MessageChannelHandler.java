/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;
import org.elasticsearch.transport.support.TransportStreams;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;

import java.io.IOException;
import java.io.StreamCorruptedException;

/**
 * A handler (must be the last one!) that does size based frame decoding and forwards the actual message
 * to the relevant action.
 */
public class MessageChannelHandler extends SimpleChannelUpstreamHandler {

    private final ESLogger logger;

    private final ThreadPool threadPool;

    private final TransportServiceAdapter transportServiceAdapter;

    private final NettyTransport transport;

    // from FrameDecoder
    private ChannelBuffer cumulation;

    public MessageChannelHandler(NettyTransport transport, ESLogger logger) {
        this.threadPool = transport.threadPool();
        this.transportServiceAdapter = transport.transportServiceAdapter();
        this.transport = transport;
        this.logger = logger;
    }

    @Override
    public void writeComplete(ChannelHandlerContext ctx, WriteCompletionEvent e) throws Exception {
        transportServiceAdapter.sent(e.getWrittenAmount());
        super.writeComplete(ctx, e);
    }

    // similar logic to FrameDecoder, we don't use FrameDecoder because we can use the data len header value
    // to guess the size of the cumulation buffer to allocate, and because we make a fresh copy of the cumulation
    // buffer so we can readBytesReference from it without other request writing into the same one in case
    // two one message and a partial next message exists within the same input

    // we can readBytesReference because NioWorker always copies the input buffer into a fresh buffer, and we
    // don't reuse cumumlation buffer
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {

        Object m = e.getMessage();
        if (!(m instanceof ChannelBuffer)) {
            ctx.sendUpstream(e);
            return;
        }

        ChannelBuffer input = (ChannelBuffer) m;
        if (!input.readable()) {
            return;
        }

        ChannelBuffer cumulation = this.cumulation;
        if (cumulation != null && cumulation.readable()) {
            cumulation.discardReadBytes();
            cumulation.writeBytes(input);
            callDecode(ctx, e.getChannel(), cumulation, true);
        } else {
            int actualSize = callDecode(ctx, e.getChannel(), input, false);
            if (input.readable()) {
                if (actualSize > 0) {
                    cumulation = ChannelBuffers.dynamicBuffer(actualSize, ctx.getChannel().getConfig().getBufferFactory());
                } else {
                    cumulation = ChannelBuffers.dynamicBuffer(ctx.getChannel().getConfig().getBufferFactory());
                }
                cumulation.writeBytes(input);
                this.cumulation = cumulation;
            }
        }
    }

    @Override
    public void channelDisconnected(
            ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        cleanup(ctx, e);
    }

    @Override
    public void channelClosed(
            ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        cleanup(ctx, e);
    }

    private int callDecode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer, boolean cumulationBuffer) throws Exception {
        int actualSize = 0;
        while (buffer.readable()) {
            actualSize = 0;
            // Changes from Frame Decoder, to combine SizeHeader and this decoder into one...
            if (buffer.readableBytes() < 4) {
                break; // we need more data
            }

            int dataLen = buffer.getInt(buffer.readerIndex());
            if (dataLen <= 0) {
                throw new StreamCorruptedException("invalid data length: " + dataLen);
            }

            actualSize = dataLen + 4;
            if (buffer.readableBytes() < actualSize) {
                break;
            }

            buffer.skipBytes(4);

            process(ctx, channel, buffer, dataLen);
        }

        if (cumulationBuffer) {
            assert buffer == this.cumulation;
            if (!buffer.readable()) {
                this.cumulation = null;
            } else if (buffer.readerIndex() > 0) {
                // make a fresh copy of the cumalation buffer, so we
                // can readBytesReference from it, and also, don't keep it around

                // its not that big of an overhead since discardReadBytes in the next round messageReceived will
                // copy over the bytes to the start again
                if (actualSize > 0) {
                    this.cumulation = ChannelBuffers.dynamicBuffer(actualSize, ctx.getChannel().getConfig().getBufferFactory());
                } else {
                    this.cumulation = ChannelBuffers.dynamicBuffer(ctx.getChannel().getConfig().getBufferFactory());
                }
                this.cumulation.writeBytes(buffer);
            }
        }

        return actualSize;
    }


    private void cleanup(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        try {
            ChannelBuffer cumulation = this.cumulation;
            if (cumulation == null) {
                return;
            } else {
                this.cumulation = null;
            }

            if (cumulation.readable()) {
                // Make sure all frames are read before notifying a closed channel.
                callDecode(ctx, ctx.getChannel(), cumulation, true);
            }

            // Call decodeLast() finally.  Please note that decodeLast() is
            // called even if there's nothing more to read from the buffer to
            // notify a user that the connection was closed explicitly.

            // Change from FrameDecoder: we don't need it...
//            Object partialFrame = decodeLast(ctx, ctx.getChannel(), cumulation);
//            if (partialFrame != null) {
//                unfoldAndFireMessageReceived(ctx, null, partialFrame);
//            }
        } finally {
            ctx.sendUpstream(e);
        }
    }

    private void process(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer, int size) throws Exception {
        transportServiceAdapter.received(size + 4);

        int markedReaderIndex = buffer.readerIndex();
        int expectedIndexReader = markedReaderIndex + size;

        // netty always copies a buffer, either in NioWorker in its read handler, where it copies to a fresh
        // buffer, or in the cumlation buffer, which is cleaned each time
        StreamInput streamIn = new ChannelBufferStreamInput(buffer, size);

        long requestId = buffer.readLong();
        byte status = buffer.readByte();
        boolean isRequest = TransportStreams.statusIsRequest(status);

        HandlesStreamInput wrappedStream;
        if (TransportStreams.statusIsCompress(status)) {
            wrappedStream = CachedStreamInput.cachedHandlesLzf(streamIn);
        } else {
            wrappedStream = CachedStreamInput.cachedHandles(streamIn);
        }

        if (isRequest) {
            String action = handleRequest(channel, wrappedStream, requestId);
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
                    handlerResponseError(wrappedStream, handler);
                } else {
                    handleResponse(wrappedStream, handler);
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
        wrappedStream.cleanHandles();
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
            ThrowableObjectInputStream ois = new ThrowableObjectInputStream(buffer, transport.settings().getClassLoader());
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
                @Override
                public void run() {
                    try {
                        handler.handleException(rtx);
                    } catch (Exception e) {
                        logger.error("Failed to handle exception response", e);
                    }
                }
            });
        }
    }

    private String handleRequest(Channel channel, StreamInput buffer, long requestId) throws IOException {
        final String action = buffer.readUTF();

        final NettyTransportChannel transportChannel = new NettyTransportChannel(transport, action, channel, requestId);
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

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        transport.exceptionCaught(ctx, e);
    }

    class ResponseHandler implements Runnable {

        private final TransportResponseHandler handler;
        private final Streamable streamable;

        public ResponseHandler(TransportResponseHandler handler, Streamable streamable) {
            this.handler = handler;
            this.streamable = streamable;
        }

        @SuppressWarnings({"unchecked"})
        @Override
        public void run() {
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

        @SuppressWarnings({"unchecked"})
        @Override
        public void run() {
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
