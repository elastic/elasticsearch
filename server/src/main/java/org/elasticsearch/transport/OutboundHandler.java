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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.NotifyOnceListener;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.network.CloseableChannel;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;

final class OutboundHandler {

    private static final Logger logger = LogManager.getLogger(OutboundHandler.class);

    private final MeanMetric transmittedBytesMetric = new MeanMetric();
    private final ThreadPool threadPool;
    private final BigArrays bigArrays;
    private final TransportLogger transportLogger;

    OutboundHandler(ThreadPool threadPool, BigArrays bigArrays, TransportLogger transportLogger) {
        this.threadPool = threadPool;
        this.bigArrays = bigArrays;
        this.transportLogger = transportLogger;
    }

    void sendBytes(TcpChannel channel, BytesReference bytes, ActionListener<Void> listener) {
        channel.getChannelStats().markAccessed(threadPool.relativeTimeInMillis());
        SendContext sendContext = new SendContext(channel, () -> bytes, listener);
        try {
            internalSendMessage(channel, sendContext);
        } catch (IOException e) {
            // This should not happen as the bytes are already serialized
            throw new AssertionError(e);
        }
    }

    void sendMessage(TcpChannel channel, OutboundMessage networkMessage, ActionListener<Void> listener) throws IOException {
        channel.getChannelStats().markAccessed(threadPool.relativeTimeInMillis());
        MessageSerializer serializer = new MessageSerializer(networkMessage, bigArrays);
        SendContext sendContext = new SendContext(channel, serializer, listener, serializer);
        internalSendMessage(channel, sendContext);
    }

    /**
     * sends a message to the given channel, using the given callbacks.
     */
    private void internalSendMessage(TcpChannel channel,  SendContext sendContext) throws IOException {
        channel.getChannelStats().markAccessed(threadPool.relativeTimeInMillis());
        BytesReference reference = sendContext.get();
        try {
            channel.sendMessage(reference, sendContext);
        } catch (RuntimeException ex) {
            sendContext.onFailure(ex);
            CloseableChannel.closeChannel(channel);
            throw ex;
        }

    }

    MeanMetric getTransmittedBytes() {
        return transmittedBytesMetric;
    }

    private static class MessageSerializer implements CheckedSupplier<BytesReference, IOException>, Releasable {

        private final OutboundMessage message;
        private final BigArrays bigArrays;
        private volatile ReleasableBytesStreamOutput bytesStreamOutput;

        private MessageSerializer(OutboundMessage message, BigArrays bigArrays) {
            this.message = message;
            this.bigArrays = bigArrays;
        }

        @Override
        public BytesReference get() throws IOException {
            bytesStreamOutput = new ReleasableBytesStreamOutput(bigArrays);
            return message.serialize(bytesStreamOutput);
        }

        @Override
        public void close() {
            IOUtils.closeWhileHandlingException(bytesStreamOutput);
        }
    }

    private class SendContext extends NotifyOnceListener<Void> implements CheckedSupplier<BytesReference, IOException> {

        private final TcpChannel channel;
        private final CheckedSupplier<BytesReference, IOException> messageSupplier;
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

        public BytesReference get() throws IOException {
            BytesReference message;
            try {
                message = messageSupplier.get();
                messageSize = message.length();
                transportLogger.logOutboundMessage(channel, message);
                return message;
            } catch (Exception e) {
                onFailure(e);
                throw e;
            }
        }

        @Override
        protected void innerOnResponse(Void v) {
            assert messageSize != -1 : "If onResponse is being called, the message should have been serialized";
            transmittedBytesMetric.inc(messageSize);
            closeAndCallback(() -> listener.onResponse(v));
        }

        @Override
        protected void innerOnFailure(Exception e) {
            logger.warn(() -> new ParameterizedMessage("send message failed [channel: {}]", channel), e);
            closeAndCallback(() -> listener.onFailure(e));
        }

        private void closeAndCallback(Runnable runnable) {
            Releasables.close(optionalReleasable, runnable::run);
        }
    }
}
