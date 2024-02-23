/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.io.IOException;

public class TestOutboundRequestMessage extends OutboundMessage.Request {
    public TestOutboundRequestMessage(
        ThreadContext threadContext,
        Writeable message,
        TransportVersion version,
        String action,
        long requestId,
        boolean isHandshake,
        Compression.Scheme compressionScheme
    ) {
        super(threadContext, message, version, action, requestId, isHandshake, compressionScheme);

    }

    @Override
    public BytesReference serialize(RecyclerBytesStreamOutput bytesStream) throws IOException {
        return super.serialize(bytesStream);
    }
}
