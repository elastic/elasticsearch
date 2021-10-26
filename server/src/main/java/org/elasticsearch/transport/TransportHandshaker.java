/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Sends and receives transport-level connection handshakes. This class will send the initial handshake,
 * manage state/timeouts while the handshake is in transit, and handle the eventual response.
 */
final class TransportHandshaker {

    static final String HANDSHAKE_ACTION_NAME = "internal:tcp/handshake";
    private final ConcurrentMap<Long, HandshakeResponseHandler> pendingHandshakes = new ConcurrentHashMap<>();
    private final CounterMetric numHandshakes = new CounterMetric();

    private final Version version;
    private final ThreadPool threadPool;
    private final HandshakeRequestSender handshakeRequestSender;

    TransportHandshaker(Version version, ThreadPool threadPool, HandshakeRequestSender handshakeRequestSender) {
        this.version = version;
        this.threadPool = threadPool;
        this.handshakeRequestSender = handshakeRequestSender;
    }

    void sendHandshake(long requestId, DiscoveryNode node, TcpChannel channel, TimeValue timeout, ActionListener<Version> listener) {
        numHandshakes.inc();
        final HandshakeResponseHandler handler = new HandshakeResponseHandler(requestId, version, listener);
        pendingHandshakes.put(requestId, handler);
        channel.addCloseListener(ActionListener.wrap(
            () -> handler.handleLocalException(new TransportException("handshake failed because connection reset"))));
        boolean success = false;
        try {
            // for the request we use the minCompatVersion since we don't know what's the version of the node we talk to
            // we also have no payload on the request but the response will contain the actual version of the node we talk
            // to as the payload.
            final Version minCompatVersion = version.minimumCompatibilityVersion();
            handshakeRequestSender.sendRequest(node, channel, requestId, minCompatVersion);

            threadPool.schedule(
                () -> handler.handleLocalException(new ConnectTransportException(node, "handshake_timeout[" + timeout + "]")),
                timeout,
                ThreadPool.Names.GENERIC);
            success = true;
        } catch (Exception e) {
            handler.handleLocalException(new ConnectTransportException(node, "failure to send " + HANDSHAKE_ACTION_NAME, e));
        } finally {
            if (success == false) {
                TransportResponseHandler<?> removed = pendingHandshakes.remove(requestId);
                assert removed == null : "Handshake should not be pending if exception was thrown";
            }
        }
    }

    void handleHandshake(TransportChannel channel, long requestId, StreamInput stream) throws IOException {
        // Must read the handshake request to exhaust the stream
        HandshakeRequest handshakeRequest = new HandshakeRequest(stream);
        final int nextByte = stream.read();
        if (nextByte != -1) {
            throw new IllegalStateException("Handshake request not fully read for requestId [" + requestId + "], action ["
                + TransportHandshaker.HANDSHAKE_ACTION_NAME + "], available [" + stream.available() + "]; resetting");
        }
        channel.sendResponse(new HandshakeResponse(this.version));
    }

    TransportResponseHandler<HandshakeResponse> removeHandlerForHandshake(long requestId) {
        return pendingHandshakes.remove(requestId);
    }

    int getNumPendingHandshakes() {
        return pendingHandshakes.size();
    }

    long getNumHandshakes() {
        return numHandshakes.count();
    }

    private class HandshakeResponseHandler implements TransportResponseHandler<HandshakeResponse> {

        private final long requestId;
        private final Version currentVersion;
        private final ActionListener<Version> listener;
        private final AtomicBoolean isDone = new AtomicBoolean(false);

        private HandshakeResponseHandler(long requestId, Version currentVersion, ActionListener<Version> listener) {
            this.requestId = requestId;
            this.currentVersion = currentVersion;
            this.listener = listener;
        }

        @Override
        public HandshakeResponse read(StreamInput in) throws IOException {
            return new HandshakeResponse(in);
        }

        @Override
        public void handleResponse(HandshakeResponse response) {
            if (isDone.compareAndSet(false, true)) {
                Version responseVersion = response.responseVersion;
                if (currentVersion.isCompatible(responseVersion) == false) {
                    listener.onFailure(new IllegalStateException("Received message from unsupported version: [" + responseVersion
                        + "] minimal compatible version is: [" + currentVersion.minimumCompatibilityVersion() + "]"));
                } else {
                    listener.onResponse(responseVersion);
                }
            }
        }

        @Override
        public void handleException(TransportException e) {
            if (isDone.compareAndSet(false, true)) {
                listener.onFailure(new IllegalStateException("handshake failed", e));
            }
        }

        void handleLocalException(TransportException e) {
            if (removeHandlerForHandshake(requestId) != null && isDone.compareAndSet(false, true)) {
                listener.onFailure(e);
            }
        }
    }

    static final class HandshakeRequest extends TransportRequest {

        private final Version version;

        HandshakeRequest(Version version) {
            this.version = version;
        }

        HandshakeRequest(StreamInput streamInput) throws IOException {
            super(streamInput);
            BytesReference remainingMessage;
            try {
                remainingMessage = streamInput.readBytesReference();
            } catch (EOFException e) {
                remainingMessage = null;
            }
            if (remainingMessage == null) {
                version = null;
            } else {
                try (StreamInput messageStreamInput = remainingMessage.streamInput()) {
                    this.version = Version.readVersion(messageStreamInput);
                }
            }
        }

        @Override
        public void writeTo(StreamOutput streamOutput) throws IOException {
            super.writeTo(streamOutput);
            assert version != null;
            try (BytesStreamOutput messageStreamOutput = new BytesStreamOutput(4)) {
                Version.writeVersion(version, messageStreamOutput);
                BytesReference reference = messageStreamOutput.bytes();
                streamOutput.writeBytesReference(reference);
            }
        }
    }

    static final class HandshakeResponse extends TransportResponse {

        private final Version responseVersion;

        HandshakeResponse(Version responseVersion) {
            this.responseVersion = responseVersion;
        }

        private HandshakeResponse(StreamInput in) throws IOException {
            super(in);
            responseVersion = Version.readVersion(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            assert responseVersion != null;
            Version.writeVersion(responseVersion, out);
        }

        Version getResponseVersion() {
            return responseVersion;
        }
    }

    @FunctionalInterface
    interface HandshakeRequestSender {

        void sendRequest(DiscoveryNode node, TcpChannel channel, long requestId, Version version) throws IOException;
    }
}
