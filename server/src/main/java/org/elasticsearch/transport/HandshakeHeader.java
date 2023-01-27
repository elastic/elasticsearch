/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Tuple;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class HandshakeHeader extends MessageHeader {

    static final int EARLIEST_HANDSHAKE_VERSION = 6080099;
    static final int CAN_SEND_ERROR_RESPONSE = 7170099;
    static final int HAS_FEATURES = 8000099;

    private final Integer handshakeVersion;
    private String actionName;

    public HandshakeHeader(int networkMessageSize, long requestId, byte status, int handshakeVersion) {
        super(networkMessageSize, requestId, status);
        if (TransportStatus.isHandshake(status) == false) {
            throw new IllegalArgumentException("Message status does not indicate a handshake");
        }
        this.handshakeVersion = handshakeVersion;
    }

    @Override
    public Integer getVersion() {
        return handshakeVersion;
    }

    @Override
    public String getActionName() {
        return actionName;
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
        var headers = ThreadContext.readHeadersFromStream(input);
        assert headers.v1().isEmpty() && headers.v2().isEmpty() : "Handshakes should have no headers";

        if (isRequest()) {
            if (handshakeVersion < HAS_FEATURES) {
                // discard features
                input.readStringArray();
            }
            this.actionName = input.readString();
        } else {
            this.actionName = RESPONSE_NAME;
        }
    }
}
