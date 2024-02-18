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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RefCounted;

import java.net.InetSocketAddress;

public abstract class TransportMessage implements Writeable, RefCounted {

    @Nullable // set by the transport service on inbound messages; unset on outbound messages
    private InetSocketAddress remoteAddress;

    public void remoteAddress(InetSocketAddress remoteAddress) {
        this.remoteAddress = remoteAddress;
    }

    @Nullable // set by the transport service on inbound messages; unset on outbound messages
    public InetSocketAddress remoteAddress() {
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
        // noop, override to manage the life-cycle of resources held by a transport message
        return true;
    }

    /**
     * {@inheritDoc}
     *
     * Note that the lifetime of an outbound {@link TransportMessage} lasts at least until it has been fully sent over the network, and it
     * may be closed on a network thread in a context in which there's a risk of stack overflows if on close it calls back into the network
     * layer in a manner that might end up nesting too deeply. When in doubt, dispatch any further work onto a separate thread.
     */
    @Override
    public boolean decRef() {
        // noop, override to manage the life-cycle of resources held by a transport message
        return false;
    }

    @Override
    public boolean hasReferences() {
        // noop, override to manage the life-cycle of resources held by a transport message
        return true;
    }
}
