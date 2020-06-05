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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.NotifyOnceListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.network.CloseableChannel;
import org.elasticsearch.common.transport.NetworkExceptionHelper;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;

public final class OutboundHandler {

    private static final Logger logger = LogManager.getLogger(OutboundHandler.class);

    private final String nodeName;
    private final Version version;
    private final StatsTracker statsTracker;
    private final ThreadPool threadPool;
    private final BigArrays bigArrays;
    private volatile TransportMessageListener messageListener = TransportMessageListener.NOOP_LISTENER;

    OutboundHandler(String nodeName, Version version, StatsTracker statsTracker, ThreadPool threadPool, BigArrays bigArrays) {
        this.nodeName = nodeName;
        this.version = version;
        this.statsTracker = statsTracker;
        this.threadPool = threadPool;
        this.bigArrays = bigArrays;
    }

    void sendBytes(TcpChannel channel, BytesReference bytes, ActionListener<Void> listener) {
        SendContext sendContext = new SendContext(channel, () -> bytes, listener);
        internalSend(channel, sendContext);
    }

    /**
     * Sends the request to the given channel. This method should be used to send {@link TransportRequest}
     * objects back to the caller.
     */
    void sendRequest(final DiscoveryNode node, final TcpChannel channel, final long requestId, final String action,
                     final TransportRequest request, final TransportRequestOptions options, final Version channelVersion,
                     final boolean compressRequest, final boolean isHandshake) throws IOException, TransportException {
        Version version = Version.min(this.version, channelVersion);
        OutboundMessage.Request message =
            new OutboundMessage.Request(threadPool.getThreadContext(), request, version, action, requestId, isHandshake, compressRequest);
        ActionListener<Void> listener = ActionListener.wrap(() ->
            messageListener.onRequestSent(node, requestId, action, request, options));
        sendMessage(channel, message, listener);
    }

    /**
     * Sends the response to the given channel. This method should be used to send {@link TransportResponse}
     * objects back to the caller.
     *
     * @see #sendErrorResponse(Version, TcpChannel, long, String, Exception) for sending error responses
     */
    void sendResponse(final Version nodeVersion, final TcpChannel channel, final long requestId, final String action,
                      final TransportResponse response, final boolean compress, final boolean isHandshake) throws IOException {
        Version version = Version.min(this.version, nodeVersion);
        OutboundMessage.Response message = new OutboundMessage.Response(threadPool.getThreadContext(), response, version,
            requestId, isHandshake, compress);
        ActionListener<Void> listener = ActionListener.wrap(() -> messageListener.onResponseSent(requestId, action, response));
        sendMessage(channel, message, listener);
    }

    /**
     * Sends back an error response to the caller via the given channel
     */
    void sendErrorResponse(final Version nodeVersion, final TcpChannel channel, final long requestId, final String action,
                           final Exception error) throws IOException {
        Version version = Version.min(this.version, nodeVersion);
        TransportAddress address = new TransportAddress(channel.getLocalAddress());
        RemoteTransportException tx = new RemoteTransportException(nodeName, address, action, error);
        OutboundMessage.Response message = new OutboundMessage.Response(threadPool.getThreadContext(), tx, version, requestId,
            false, false);
        ActionListener<Void> listener = ActionListener.wrap(() -> messageListener.onResponseSent(requestId, action, error));
        sendMessage(channel, message, listener);
    }

    private void sendMessage(TcpChannel channel, OutboundMessage networkMessage, ActionListener<Void> listener) throws IOException {
        MessageSerializer serializer = new MessageSerializer(networkMessage, bigArrays);
        SendContext sendContext = new SendContext(channel, serializer, listener, serializer);
        internalSend(channel, sendContext);
    }

    private void internalSend(TcpChannel channel, SendContext sendContext) {
        channel.getChannelStats().markAccessed(threadPool.relativeTimeInMillis());
        try {
            channel.sendMessage(sendContext);
        } catch (RuntimeException ex) {
            sendContext.onFailure(ex);
            CloseableChannel.closeChannel(channel);
            throw ex;
        }

    }

    void setMessageListener(TransportMessageListener listener) {
        if (messageListener == TransportMessageListener.NOOP_LISTENER) {
            messageListener = listener;
        } else {
            throw new IllegalStateException("Cannot set message listener twice");
        }
    }

    private static class MessageSerializer implements CheckedSupplier<BytesReference, IOException>, Releasable {

        private OutboundMessage message;
        private final BigArrays bigArrays;
        private volatile ReleasableBytesStreamOutput bytesStreamOutput;

        private MessageSerializer(OutboundMessage message, BigArrays bigArrays) {
            this.message = message;
            this.bigArrays = bigArrays;
        }

        @Override
        public BytesReference get() throws IOException {
            try {
                bytesStreamOutput = new ReleasableBytesStreamOutput(bigArrays);
                return message.serialize(bytesStreamOutput);
            } finally {
                message = null;
            }
        }

        @Override
        public void close() {
            IOUtils.closeWhileHandlingException(bytesStreamOutput);
        }
    }

    public class SendContext extends NotifyOnceListener<Void> implements CheckedSupplier<BytesReference, IOException> {

        private final TcpChannel channel;
        private CheckedSupplier<BytesReference, IOException> messageSupplier;
        private final ActionListener<Void> listener;
        private final Releasable optionalReleasable;
        private long messageSize = -1;

        private SendContext(TcpChannel channel, CheckedSupplier<BytesReference, IOException> messageSupplier,
                            ActionListener<Void> listener) {
            this(channel, messageSupplier, listener, null);
        }

        private SendContext(TcpChannel channel, CheckedSupplier<BytesReference, IOException> messageSupplier,
                            ActionListener<Void> listener, Releasable optionalReleasable) {
            this.channel = channel;
            this.messageSupplier = messageSupplier;
            this.listener = listener;
            this.optionalReleasable = optionalReleasable;
        }

        @Override
        public BytesReference get() throws IOException {
            BytesReference message;
            try {
                assert messageSupplier != null;
                message = messageSupplier.get();
                messageSupplier = null;
                messageSize = message.length();
                TransportLogger.logOutboundMessage(channel, message);
                return message;
            } catch (Exception e) {
                onFailure(e);
                throw e;
            }
        }

        @Override
        protected void innerOnResponse(Void v) {
            assert messageSize != -1 : "If onResponse is being called, the message should have been serialized";
            statsTracker.markBytesWritten(messageSize);
            closeAndCallback(() -> listener.onResponse(v));
        }

        @Override
        protected void innerOnFailure(Exception e) {
            messageSupplier = null;
            if (NetworkExceptionHelper.isCloseConnectionException(e)) {
                logger.debug(() -> new ParameterizedMessage("send message failed [channel: {}]", channel), e);
            } else {
                logger.warn(() -> new ParameterizedMessage("send message failed [channel: {}]", channel), e);
            }
            closeAndCallback(() -> listener.onFailure(e));
        }

        private void closeAndCallback(Runnable runnable) {
            Releasables.close(optionalReleasable, runnable::run);
        }
    }
}
