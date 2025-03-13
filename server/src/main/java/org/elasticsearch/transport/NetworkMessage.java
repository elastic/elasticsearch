/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.transport;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.ThreadContext;

/**
 * Represents a transport message sent over the network. Subclasses implement serialization and
 * deserialization.
 */
public abstract class NetworkMessage {

    protected final TransportVersion version;
    protected final Writeable threadContext;
    protected final long requestId;
    protected final byte status;
    protected final Compression.Scheme compressionScheme;

    NetworkMessage(
        ThreadContext threadContext,
        TransportVersion version,
        byte status,
        long requestId,
        Compression.Scheme compressionScheme
    ) {
        this.threadContext = threadContext.captureAsWriteable();
        this.version = version;
        this.requestId = requestId;
        this.compressionScheme = compressionScheme;
        if (this.compressionScheme != null) {
            this.status = TransportStatus.setCompress(status);
        } else {
            this.status = status;
        }
    }

    boolean isCompress() {
        return TransportStatus.isCompress(status);
    }

    boolean isHandshake() {
        return TransportStatus.isHandshake(status);
    }

    boolean isError() {
        return TransportStatus.isError(status);
    }
}
