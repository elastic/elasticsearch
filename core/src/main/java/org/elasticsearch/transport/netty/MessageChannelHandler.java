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

package org.elasticsearch.transport.netty;

import org.apache.lucene.util.IOUtils;
import org.elasticsearch.Version;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.compress.Compressor;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.compress.NotCompressedException;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ActionNotFoundTransportException;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.RequestHandlerRegistry;
import org.elasticsearch.transport.ResponseHandlerFailureTransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportSerializationException;
import org.elasticsearch.transport.TransportServiceAdapter;
import org.elasticsearch.transport.Transports;
import org.elasticsearch.transport.support.TransportStatus;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.WriteCompletionEvent;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * A handler (must be the last one!) that does size based frame decoding and forwards the actual message
 * to the relevant action.
 */
public class MessageChannelHandler extends SimpleChannelUpstreamHandler {

    protected final ESLogger logger;
    protected final ThreadPool threadPool;
    protected final TransportServiceAdapter transportServiceAdapter;
    protected final NettyTransport transport;
    protected final String profileName;
    private final ThreadContext threadContext;

    public MessageChannelHandler(NettyTransport transport, ESLogger logger, String profileName) {
        this.threadPool = transport.threadPool();
        this.threadContext = threadPool.getThreadContext();
        this.transportServiceAdapter = transport.transportServiceAdapter();
        this.transport = transport;
        this.logger = logger;
        this.profileName = profileName;
    }

    @Override
    public void writeComplete(ChannelHandlerContext ctx, WriteCompletionEvent e) throws Exception {
        transportServiceAdapter.sent(e.getWrittenAmount());
        super.writeComplete(ctx, e);
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        Transports.assertTransportThread();
        Object m = e.getMessage();
        if (!(m instanceof ChannelBuffer)) {
            ctx.sendUpstream(e);
            return;
        }
        ChannelBuffer buffer = (ChannelBuffer) m;
        Marker marker = new Marker(buffer);
        int size = marker.messageSizeWithRemainingHeaders();
        transportServiceAdapter.received(marker.messageSizeWithAllHeaders());

        // we have additional bytes to read, outside of the header
        boolean hasMessageBytesToRead = marker.messageSize() != 0;

        // netty always copies a buffer, either in NioWorker in its read handler, where it copies to a fresh
        // buffer, or in the cumulation buffer, which is cleaned each time
        StreamInput streamIn = ChannelBufferStreamInputFactory.create(buffer, size);
        boolean success = false;
        try (ThreadContext.StoredContext tCtx = threadContext.stashContext()) {
            long requestId = streamIn.readLong();
            byte status = streamIn.readByte();
            Version version = Version.fromId(streamIn.readInt());

            if (TransportStatus.isCompress(status) && hasMessageBytesToRead && buffer.readable()) {
                Compressor compressor;
                try {
                    compressor = CompressorFactory.compressor(buffer);
                } catch (NotCompressedException ex) {
                    int maxToRead = Math.min(buffer.readableBytes(), 10);
                    int offset = buffer.readerIndex();
                    StringBuilder sb = new StringBuilder("stream marked as compressed, but no compressor found, first [").append(maxToRead)
                            .append("] content bytes out of [").append(buffer.readableBytes())
                            .append("] readable bytes with message size [").append(size).append("] ").append("] are [");
                    for (int i = 0; i < maxToRead; i++) {
                        sb.append(buffer.getByte(offset + i)).append(",");
                    }
                    sb.append("]");
                    throw new IllegalStateException(sb.toString());
                }
                streamIn = compressor.streamInput(streamIn);
            }
            if (version.onOrAfter(Version.CURRENT.minimumCompatibilityVersion()) == false || version.major != Version.CURRENT.major) {
                throw new IllegalStateException("Received message from unsupported version: [" + version
                    + "] minimal compatible version is: [" +Version.CURRENT.minimumCompatibilityVersion() + "]");
            }
            streamIn.setVersion(version);
            if (TransportStatus.isRequest(status)) {
                threadContext.readHeaders(streamIn);
                handleRequest(ctx.getChannel(), marker, streamIn, requestId, size, version);
            } else {
                TransportResponseHandler<?> handler = transportServiceAdapter.onResponseReceived(requestId);
                // ignore if its null, the adapter logs it
                if (handler != null) {
                    if (TransportStatus.isError(status)) {
                        handlerResponseError(streamIn, handler);
                    } else {
                        handleResponse(ctx.getChannel(), streamIn, handler);
                    }
                    marker.validateResponse(streamIn, requestId, handler, TransportStatus.isError(status));
                }
            }
            success = true;
        } finally {
            try {
                if (success) {
                    IOUtils.close(streamIn);
                } else {
                    IOUtils.closeWhileHandlingException(streamIn);
                }
            } finally {
                // Set the expected position of the buffer, no matter what happened
                buffer.readerIndex(marker.expectedReaderIndex());
            }
        }
    }

    protected void handleResponse(Channel channel, StreamInput buffer, final TransportResponseHandler handler) {
        buffer = new NamedWriteableAwareStreamInput(buffer, transport.namedWriteableRegistry);
        final TransportResponse response = handler.newInstance();
        response.remoteAddress(new InetSocketTransportAddress((InetSocketAddress) channel.getRemoteAddress()));
        response.remoteAddress();
        try {
            response.readFrom(buffer);
        } catch (Throwable e) {
            handleException(handler, new TransportSerializationException(
                    "Failed to deserialize response of type [" + response.getClass().getName() + "]", e));
            return;
        }
        try {
            if (ThreadPool.Names.SAME.equals(handler.executor())) {
                //noinspection unchecked
                handler.handleResponse(response);
            } else {
                threadPool.executor(handler.executor()).execute(new ResponseHandler(handler, response));
            }
        } catch (Throwable e) {
            handleException(handler, new ResponseHandlerFailureTransportException(e));
        }
    }

    private void handlerResponseError(StreamInput buffer, final TransportResponseHandler handler) {
        Throwable error;
        try {
            error = buffer.readThrowable();
        } catch (Throwable e) {
            error = new TransportSerializationException("Failed to deserialize exception response from stream", e);
        }
        handleException(handler, error);
    }

    private void handleException(final TransportResponseHandler handler, Throwable error) {
        if (!(error instanceof RemoteTransportException)) {
            error = new RemoteTransportException(error.getMessage(), error);
        }
        final RemoteTransportException rtx = (RemoteTransportException) error;
        if (ThreadPool.Names.SAME.equals(handler.executor())) {
            try {
                handler.handleException(rtx);
            } catch (Throwable e) {
                logger.error("failed to handle exception response [{}]", e, handler);
            }
        } else {
            threadPool.executor(handler.executor()).execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        handler.handleException(rtx);
                    } catch (Throwable e) {
                        logger.error("failed to handle exception response [{}]", e, handler);
                    }
                }
            });
        }
    }

    protected String handleRequest(Channel channel, Marker marker, StreamInput buffer, long requestId, int messageLengthBytes,
                                   Version version) throws IOException {
        buffer = new NamedWriteableAwareStreamInput(buffer, transport.namedWriteableRegistry);
        final String action = buffer.readString();
        transportServiceAdapter.onRequestReceived(requestId, action);
        NettyTransportChannel transportChannel = null;
        try {
            final RequestHandlerRegistry reg = transportServiceAdapter.getRequestHandler(action);
            if (reg == null) {
                throw new ActionNotFoundTransportException(action);
            }
            if (reg.canTripCircuitBreaker()) {
                transport.inFlightRequestsBreaker().addEstimateBytesAndMaybeBreak(messageLengthBytes, "<transport_request>");
            } else {
                transport.inFlightRequestsBreaker().addWithoutBreaking(messageLengthBytes);
            }
            transportChannel = new NettyTransportChannel(transport, transportServiceAdapter, action, channel,
                requestId, version, profileName, messageLengthBytes);
            final TransportRequest request = reg.newRequest();
            request.remoteAddress(new InetSocketTransportAddress((InetSocketAddress) channel.getRemoteAddress()));
            request.readFrom(buffer);
            // in case we throw an exception, i.e. when the limit is hit, we don't want to verify
            validateRequest(marker, buffer, requestId, action);
            if (ThreadPool.Names.SAME.equals(reg.getExecutor())) {
                //noinspection unchecked
                reg.processMessageReceived(request, transportChannel);
            } else {
                threadPool.executor(reg.getExecutor()).execute(new RequestHandler(reg, request, transportChannel));
            }
        } catch (Throwable e) {
            // the circuit breaker tripped
            if (transportChannel == null) {
                transportChannel = new NettyTransportChannel(transport, transportServiceAdapter, action, channel,
                    requestId, version, profileName, 0);
            }
            try {
                transportChannel.sendResponse(e);
            } catch (IOException e1) {
                logger.warn("Failed to send error message back to client for action [{}]", e, action);
                logger.warn("Actual Exception", e1);
            }
        }
        return action;
    }

    // This template method is needed to inject custom error checking logic in tests.
    protected void validateRequest(Marker marker, StreamInput buffer, long requestId, String action) throws IOException {
        marker.validateRequest(buffer, requestId, action);
    }


    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        transport.exceptionCaught(ctx, e);
    }

    class ResponseHandler implements Runnable {

        private final TransportResponseHandler handler;
        private final TransportResponse response;

        public ResponseHandler(TransportResponseHandler handler, TransportResponse response) {
            this.handler = handler;
            this.response = response;
        }

        @SuppressWarnings({"unchecked"})
        @Override
        public void run() {
            try {
                handler.handleResponse(response);
            } catch (Throwable e) {
                handleException(handler, new ResponseHandlerFailureTransportException(e));
            }
        }
    }

    class RequestHandler extends AbstractRunnable {
        private final RequestHandlerRegistry reg;
        private final TransportRequest request;
        private final NettyTransportChannel transportChannel;

        public RequestHandler(RequestHandlerRegistry reg, TransportRequest request, NettyTransportChannel transportChannel) {
            this.reg = reg;
            this.request = request;
            this.transportChannel = transportChannel;
        }

        @SuppressWarnings({"unchecked"})
        @Override
        protected void doRun() throws Exception {
            reg.processMessageReceived(request, transportChannel);
        }

        @Override
        public boolean isForceExecution() {
            return reg.isForceExecution();
        }

        @Override
        public void onFailure(Throwable e) {
            if (transport.lifecycleState() == Lifecycle.State.STARTED) {
                // we can only send a response transport is started....
                try {
                    transportChannel.sendResponse(e);
                } catch (Throwable e1) {
                    logger.warn("Failed to send error message back to client for action [{}]", e1, reg.getAction());
                    logger.warn("Actual Exception", e);
                }
            }
        }
    }

    /**
     * Internal helper class to store characteristic offsets of a buffer during processing
     */
    protected static final class Marker {
        private final ChannelBuffer buffer;
        private final int remainingMessageSize;
        private final int expectedReaderIndex;

        public Marker(ChannelBuffer buffer) {
            this.buffer = buffer;
            // when this constructor is called, we have read already two parts of the message header: the marker bytes and the message
            // message length (see SizeHeaderFrameDecoder). Hence we have to rewind the index for MESSAGE_LENGTH_SIZE bytes to read the
            // remaining message length again.
            this.remainingMessageSize = buffer.getInt(buffer.readerIndex() - NettyHeader.MESSAGE_LENGTH_SIZE);
            this.expectedReaderIndex = buffer.readerIndex() + remainingMessageSize;
        }

        /**
         * @return the number of bytes that have yet to be read from the buffer
         */
        public int messageSizeWithRemainingHeaders() {
            return remainingMessageSize;
        }

        /**
         * @return the number in bytes for the message including all headers (even the ones that have been read from the buffer already)
         */
        public int messageSizeWithAllHeaders() {
            return remainingMessageSize + NettyHeader.MARKER_BYTES_SIZE + NettyHeader.MESSAGE_LENGTH_SIZE;
        }

        /**
         * @return the number of bytes for the message itself (excluding all headers).
         */
        public int messageSize() {
            return messageSizeWithAllHeaders() - NettyHeader.HEADER_SIZE;
        }

        /**
         * @return the expected index of the buffer's reader after the message has been consumed entirely.
         */
        public int expectedReaderIndex() {
            return expectedReaderIndex;
        }

        /**
         * Validates that a request has been fully read (not too few bytes but also not too many bytes).
         *
         * @param stream    A stream that is associated with the buffer that is tracked by this marker.
         * @param requestId The current request id.
         * @param action    The currently executed action.
         * @throws IOException           Iff the stream could not be read.
         * @throws IllegalStateException Iff the request has not been fully read.
         */
        public void validateRequest(StreamInput stream, long requestId, String action) throws IOException {
            final int nextByte = stream.read();
            // calling read() is useful to make sure the message is fully read, even if there some kind of EOS marker
            if (nextByte != -1) {
                throw new IllegalStateException("Message not fully read (request) for requestId [" + requestId + "], action [" + action
                    + "], readerIndex [" + buffer.readerIndex() + "] vs expected [" + expectedReaderIndex + "]; resetting");
            }
            if (buffer.readerIndex() < expectedReaderIndex) {
                throw new IllegalStateException("Message is fully read (request), yet there are "
                    + (expectedReaderIndex - buffer.readerIndex()) + " remaining bytes; resetting");
            }
            if (buffer.readerIndex() > expectedReaderIndex) {
                throw new IllegalStateException(
                    "Message read past expected size (request) for requestId [" + requestId + "], action [" + action
                        + "], readerIndex [" + buffer.readerIndex() + "] vs expected [" + expectedReaderIndex + "]; resetting");
            }
        }

        /**
         * Validates that a response has been fully read (not too few bytes but also not too many bytes).
         *
         * @param stream    A stream that is associated with the buffer that is tracked by this marker.
         * @param requestId The corresponding request id for this response.
         * @param handler   The current response handler.
         * @param error     Whether validate an error response.
         * @throws IOException           Iff the stream could not be read.
         * @throws IllegalStateException Iff the request has not been fully read.
         */
        public void validateResponse(StreamInput stream, long requestId,
                                     TransportResponseHandler<?> handler, boolean error) throws IOException {
            // Check the entire message has been read
            final int nextByte = stream.read();
            // calling read() is useful to make sure the message is fully read, even if there is an EOS marker
            if (nextByte != -1) {
                throw new IllegalStateException("Message not fully read (response) for requestId [" + requestId + "], handler ["
                    + handler + "], error [" + error + "]; resetting");
            }
            if (buffer.readerIndex() < expectedReaderIndex) {
                throw new IllegalStateException("Message is fully read (response), yet there are "
                    + (expectedReaderIndex - buffer.readerIndex()) + " remaining bytes; resetting");
            }
            if (buffer.readerIndex() > expectedReaderIndex) {
                throw new IllegalStateException("Message read past expected size (response) for requestId [" + requestId
                    + "], handler [" + handler + "], error [" + error + "]; resetting");
            }
        }
    }
}
