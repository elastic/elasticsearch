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

public abstract class MessageHeader {

    static final String RESPONSE_NAME = "NO_ACTION_NAME_FOR_RESPONSES";

    private final int networkMessageSize;
    private final long requestId;
    private final byte status;

    private Compression.Scheme compressionScheme;

    protected MessageHeader(int networkMessageSize, long requestId, byte status) {
        this.networkMessageSize = networkMessageSize;
        this.requestId = requestId;
        this.status = status;
    }

    public int getNetworkMessageSize() {
        return networkMessageSize;
    }

    public abstract Object getVersion();

    public long getRequestId() {
        return requestId;
    }

    public boolean isRequest() {
        return TransportStatus.isRequest(status);
    }

    public boolean isResponse() {
        return TransportStatus.isRequest(status) == false;
    }

    public boolean isError() {
        return TransportStatus.isError(status);
    }

    public boolean isHandshake() {
        return TransportStatus.isHandshake(status);
    }

    public boolean isCompressed() {
        return TransportStatus.isCompress(status);
    }

    public abstract String getActionName();

    public Compression.Scheme getCompressionScheme() {
        return compressionScheme;
    }

    public abstract boolean needsToReadVariableHeader();

    public abstract Tuple<Map<String, String>, Map<String, Set<String>>> getHeaders();

    public abstract void finishParsingHeader(StreamInput input) throws IOException;

    public void setCompressionScheme(Compression.Scheme compressionScheme) {
        assert isCompressed();
        this.compressionScheme = compressionScheme;
    }

    @Override
    public String toString() {
        return "Header{"
            + networkMessageSize
            + "}{"
            + getVersion()
            + "}{"
            + requestId
            + "}{"
            + isRequest()
            + "}{"
            + isError()
            + "}{"
            + isHandshake()
            + "}{"
            + isCompressed()
            + "}{"
            + getActionName()
            + "}";
    }
}
