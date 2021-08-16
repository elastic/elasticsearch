/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.Version;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class Header {

    private static final String RESPONSE_NAME = "NO_ACTION_NAME_FOR_RESPONSES";

    private final int networkMessageSize;
    private final Version version;
    private final long requestId;
    private final byte status;
    // These are directly set by tests
    String actionName;
    Tuple<Map<String, String>, Map<String, Set<String>>> headers;
    Set<String> features;
    private Compression.Scheme compressionScheme = null;

    Header(int networkMessageSize, long requestId, byte status, Version version) {
        this.networkMessageSize = networkMessageSize;
        this.version = version;
        this.requestId = requestId;
        this.status = status;
    }

    public int getNetworkMessageSize() {
        return networkMessageSize;
    }

    Version getVersion() {
        return version;
    }

    long getRequestId() {
        return requestId;
    }

    byte getStatus() {
        return status;
    }

    boolean isRequest() {
        return TransportStatus.isRequest(status);
    }

    boolean isResponse() {
        return TransportStatus.isRequest(status) == false;
    }

    boolean isError() {
        return TransportStatus.isError(status);
    }

    boolean isHandshake() {
        return TransportStatus.isHandshake(status);
    }

    boolean isCompressed() {
        return TransportStatus.isCompress(status);
    }

    public String getActionName() {
        return actionName;
    }

    public Compression.Scheme getCompressionScheme() {
        return compressionScheme;
    }

    boolean needsToReadVariableHeader() {
        return headers == null;
    }

    public Set<String> getFeatures() {
        return features;
    }

    Tuple<Map<String, String>, Map<String, Set<String>>> getHeaders() {
        return headers;
    }

    void finishParsingHeader(StreamInput input) throws IOException {
        this.headers = ThreadContext.readHeadersFromStream(input);

        if (isRequest()) {
            if (version.onOrAfter(Version.V_6_3_0)) {
                final String[] featuresFound = input.readStringArray();
                if (featuresFound.length == 0) {
                    features = Collections.emptySet();
                } else {
                    features = Collections.unmodifiableSet(new TreeSet<>(Arrays.asList(featuresFound)));
                }
            } else {
                features = Collections.emptySet();
            }
            this.actionName = input.readString();
        } else {
            this.actionName = RESPONSE_NAME;
        }
    }

    void setCompressionScheme(Compression.Scheme compressionScheme) {
        assert isCompressed();
        this.compressionScheme = compressionScheme;
    }

    @Override
    public String toString() {
        return "Header{" + networkMessageSize + "}{" + version + "}{" + requestId + "}{" + isRequest() + "}{" + isError() + "}{"
                + isHandshake() + "}{" + isCompressed() + "}{" + actionName + "}";
    }
}
