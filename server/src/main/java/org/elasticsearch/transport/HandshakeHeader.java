/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.Tuple;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class HandshakeHeader implements MessageHeader {

    static final int EARLIEST_HANDSHAKE_VERSION = 6080099;
    static final int CAN_SEND_ERROR_RESPONSE = 7170099;

    private final int networkMessageSize;
    private final Integer handshakeVersion;
    private final long requestId;
    private final byte status;

    public HandshakeHeader(int networkMessageSize, long requestId, byte status, int handshakeVersion) {
        if (TransportStatus.isHandshake(status) == false) {
            throw new IllegalArgumentException("Message status does not indicate a handshake");
        }
        this.networkMessageSize = networkMessageSize;
        this.handshakeVersion = handshakeVersion;
        this.requestId = requestId;
        this.status = status;
    }

    @Override
    public int getNetworkMessageSize() {
        return networkMessageSize;
    }

    @Override
    public Integer getVersion() {
        return handshakeVersion;
    }

    @Override
    public long getRequestId() {
        return requestId;
    }

    @Override
    public boolean isRequest() {
        return TransportStatus.isRequest(status);
    }

    @Override
    public boolean isResponse() {
        return TransportStatus.isRequest(status) == false;
    }

    @Override
    public boolean isError() {
        return false;
    }

    @Override
    public boolean isHandshake() {
        return true;
    }

    @Override
    public boolean isCompressed() {
        return false;
    }

    @Override
    public String getActionName() {
        return TransportService.HANDSHAKE_ACTION_NAME;
    }

    @Override
    public Compression.Scheme getCompressionScheme() {
        return null;
    }

    @Override
    public boolean needsToReadVariableHeader() {
        return false;
    }

    @Override
    public Tuple<Map<String, String>, Map<String, Set<String>>> getHeaders() {
        return new Tuple<>(Map.of(), Map.of());
    }

    @Override
    public void finishParsingHeader(StreamInput input) throws IOException {
        throw new IllegalStateException("No headers to parse");
    }

    @Override
    public void setCompressionScheme(Compression.Scheme compressionScheme) {}
}
