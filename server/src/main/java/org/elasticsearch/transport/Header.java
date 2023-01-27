/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Tuple;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class Header extends MessageHeader {

    private final TransportVersion version;
    // These are directly set by tests
    String actionName;
    Tuple<Map<String, String>, Map<String, Set<String>>> headers;

    Header(int networkMessageSize, long requestId, byte status, TransportVersion version) {
        super(networkMessageSize, requestId, status);
        if (TransportStatus.isHandshake(status)) {
            throw new IllegalArgumentException("Normal header cannot represent a handshake");
        }
        this.version = version;
    }

    @Override
    public TransportVersion getVersion() {
        return version;
    }

    @Override
    public String getActionName() {
        return actionName;
    }

    @Override
    public boolean needsToReadVariableHeader() {
        return headers == null;
    }

    @Override
    public Tuple<Map<String, String>, Map<String, Set<String>>> getHeaders() {
        return headers;
    }

    @Override
    public void finishParsingHeader(StreamInput input) throws IOException {
        this.headers = ThreadContext.readHeadersFromStream(input);

        if (isRequest()) {
            if (version.before(TransportVersion.V_8_0_0)) {
                // discard features
                input.readStringArray();
            }
            this.actionName = input.readString();
        } else {
            this.actionName = RESPONSE_NAME;
        }
    }
}
