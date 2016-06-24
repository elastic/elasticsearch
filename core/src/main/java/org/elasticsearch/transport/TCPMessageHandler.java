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

package org.elasticsearch.transport;

import org.apache.lucene.util.IOUtils;
import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.compress.Compressor;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.compress.NotCompressedException;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.netty.NettyUtils;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.support.TransportStatus;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.function.IntSupplier;

/**
 * A handler (must be the last one!) that does size based frame decoding and forwards the actual message
 * to the relevant action.
 */
public class TCPMessageHandler {

    protected final ESLogger logger;
    protected final ThreadPool threadPool;
    protected final TransportServiceAdapter transportServiceAdapter;
    protected final Transport transport;
    protected final ThreadContext threadContext;
    protected final NamedWriteableRegistry namedWriteableRegistry;

    public TCPMessageHandler(ThreadPool threadPool, Transport transport, TransportServiceAdapter adapter,
                             NamedWriteableRegistry namedWriteableRegistry, ESLogger logger) {
        this.threadPool = threadPool;
        this.threadContext = threadPool.getThreadContext();
        this.transportServiceAdapter = adapter;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.transport = transport;
        this.logger = logger;
    }

    public final void messageReceived(BytesReference reference, Marker marker, ChannelFactory channelFactory,
                                      InetSocketAddress remoteAddress, int messageLengthBytes) throws IOException {
        transportServiceAdapter.received(marker.messageSizeWithAllHeaders());
        // we have additional bytes to read, outside of the header
        boolean hasMessageBytesToRead = marker.messageSize() != 0;
        StreamInput streamIn = reference.streamInput();
        boolean success = false;
        try (ThreadContext.StoredContext tCtx = threadContext.stashContext()) {
            long requestId = streamIn.readLong();
            byte status = streamIn.readByte();
            Version version = Version.fromId(streamIn.readInt());
            if (TransportStatus.isCompress(status) && hasMessageBytesToRead) {
                Compressor compressor;
                try {
                    compressor = CompressorFactory.compressor(reference);
                } catch (NotCompressedException ex) {
                    int maxToRead = Math.min(reference.length(), 10);
                    StringBuilder sb = new StringBuilder("stream marked as compressed, but no compressor found, first [").append(maxToRead)
                        .append("] content bytes out of [").append(reference.length())
                        .append("] readable bytes with message size [").append(messageLengthBytes).append("] ").append("] are [");
                    for (int i = 0; i < maxToRead; i++) {
                        sb.append(reference.get(i)).append(",");
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
                handleRequest(channelFactory, marker, streamIn, requestId, messageLengthBytes, version, remoteAddress);
            } else {
                TransportResponseHandler<?> handler = transportServiceAdapter.onResponseReceived(requestId);
                // ignore if its null, the adapter logs it
                if (handler != null) {
                    if (TransportStatus.isError(status)) {
                        handlerResponseError(streamIn, handler);
                    } else {
                        handleResponse(remoteAddress, streamIn, handler);
                    }
                    marker.validateResponse(streamIn, requestId, handler, TransportStatus.isError(status));
                }
            }
            success = true;
        } finally {
            if (success) {
                IOUtils.close(streamIn);
            } else {
                IOUtils.closeWhileHandlingException(streamIn);
            }
        }
    }

    protected void handleResponse(InetSocketAddress remoteAddress, StreamInput buffer, final TransportResponseHandler handler) {
        buffer = new NamedWriteableAwareStreamInput(buffer, namedWriteableRegistry);
        final TransportResponse response = handler.newInstance();
        response.remoteAddress(new InetSocketTransportAddress(remoteAddress));
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

    protected String handleRequest(ChannelFactory channelFactory, Marker marker, StreamInput buffer, long requestId,
                                   int messageLengthBytes, Version version, InetSocketAddress remoteAddress) throws IOException {
        buffer = new NamedWriteableAwareStreamInput(buffer, namedWriteableRegistry);
        final String action = buffer.readString();
        transportServiceAdapter.onRequestReceived(requestId, action);
        TransportChannel transportChannel = null;
        try {
            final RequestHandlerRegistry reg = transportServiceAdapter.getRequestHandler(action);
            if (reg == null) {
                throw new ActionNotFoundTransportException(action);
            }
            if (reg.canTripCircuitBreaker()) {
                transport.getInFlightRequestBreaker().addEstimateBytesAndMaybeBreak(messageLengthBytes, "<transport_request>");
            } else {
                transport.getInFlightRequestBreaker().addWithoutBreaking(messageLengthBytes);
            }
            transportChannel = channelFactory.create(action, requestId, version, messageLengthBytes);
            final TransportRequest request = reg.newRequest();
            request.remoteAddress(new InetSocketTransportAddress(remoteAddress));
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
                transportChannel = channelFactory.create(action, requestId, version, 0);
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

    public interface ChannelFactory {
        TransportChannel create(String actionName, long requestId, Version version, long reservedBytes);
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
        private final TransportChannel transportChannel;

        public RequestHandler(RequestHandlerRegistry reg, TransportRequest request, TransportChannel transportChannel) {
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
    public static final class Marker {
        private final IntSupplier markerOffset;
        private final int remainingMessageSize;
        private final int expectedReaderIndex;

        public Marker(IntSupplier markerOffset, int remainingMessageSize) {
            this.markerOffset = markerOffset;
            // when this constructor is called, we have read already two parts of the message header: the marker bytes and the message
            // message length (see SizeHeaderFrameDecoder). Hence we have to rewind the index for MESSAGE_LENGTH_SIZE bytes to read the
            // remaining message length again.
            this.remainingMessageSize = remainingMessageSize;
            this.expectedReaderIndex = this.markerOffset.getAsInt() + remainingMessageSize;
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
            return remainingMessageSize + TCPHeader.MARKER_BYTES_SIZE + TCPHeader.MESSAGE_LENGTH_SIZE;
        }

        /**
         * @return the number of bytes for the message itself (excluding all headers).
         */
        public int messageSize() {
            return messageSizeWithAllHeaders() - TCPHeader.HEADER_SIZE;
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
                    + "], readerIndex [" + markerOffset.getAsInt() + "] vs expected [" + expectedReaderIndex + "]; resetting");
            }
            if (markerOffset.getAsInt() < expectedReaderIndex) {
                throw new IllegalStateException("Message is fully read (request), yet there are "
                    + (expectedReaderIndex - markerOffset.getAsInt()) + " remaining bytes; resetting");
            }
            if (markerOffset.getAsInt() > expectedReaderIndex) {
                throw new IllegalStateException(
                    "Message read past expected size (request) for requestId [" + requestId + "], action [" + action
                        + "], readerIndex [" + markerOffset.getAsInt() + "] vs expected [" + expectedReaderIndex + "]; resetting");
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
            if (markerOffset.getAsInt() < expectedReaderIndex) {
                throw new IllegalStateException("Message is fully read (response), yet there are "
                    + (expectedReaderIndex - markerOffset.getAsInt()) + " remaining bytes; resetting");
            }
            if (markerOffset.getAsInt() > expectedReaderIndex) {
                throw new IllegalStateException("Message read past expected size (response) for requestId [" + requestId
                    + "], handler [" + handler + "], error [" + error + "]; resetting");
            }
        }
    }
}
