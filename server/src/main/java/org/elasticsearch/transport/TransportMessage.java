/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.core.RefCounted;

public abstract class TransportMessage implements Writeable, RefCounted {

    private TransportAddress remoteAddress;

    public void remoteAddress(TransportAddress remoteAddress) {
        this.remoteAddress = remoteAddress;
    }

    public TransportAddress remoteAddress() {
        return remoteAddress;
    }

    /**
     * Constructs a new empty transport message
     */
    public TransportMessage() {}

    /**
     * Constructs a new transport message with the data from the {@link StreamInput}. This is
     * currently a no-op
     */
    public TransportMessage(StreamInput in) {}

    @Override
    public void incRef() {
        // noop, override to manage the life-cycle of resources held by a transport message
    }

    @Override
    public boolean tryIncRef() {
        return true;
    }

    @Override
    public boolean decRef() {
        // noop, override to manage the life-cycle of resources held by a transport message
        return false;
    }
}
