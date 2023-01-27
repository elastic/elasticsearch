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

public interface MessageHeader {
    int getNetworkMessageSize();

    Object getVersion();

    long getRequestId();

    boolean isRequest();

    boolean isResponse();

    boolean isError();

    boolean isHandshake();

    boolean isCompressed();

    String getActionName();

    Compression.Scheme getCompressionScheme();

    boolean needsToReadVariableHeader();

    Tuple<Map<String, String>, Map<String, Set<String>>> getHeaders();

    void finishParsingHeader(StreamInput input) throws IOException;

    void setCompressionScheme(Compression.Scheme compressionScheme);
}
