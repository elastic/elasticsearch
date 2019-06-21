/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport.nio;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.nio.DelegateHandler;
import org.elasticsearch.nio.InboundChannelBuffer;
import org.elasticsearch.nio.NioChannelHandler;
import org.elasticsearch.xpack.security.transport.filter.IPFilter;

import java.io.IOException;
import java.net.InetSocketAddress;

public final class NioIPFilter extends DelegateHandler {

    private InetSocketAddress remoteAddress;
    private final IPFilter filter;
    private final String profile;
    private boolean denied = false;

    NioIPFilter(NioChannelHandler delegate, InetSocketAddress remoteAddress, @Nullable IPFilter filter, String profile) {
        super(delegate);
        this.remoteAddress = remoteAddress;
        this.filter = filter;
        this.profile = profile;
    }

    @Override
    public void channelRegistered() {
        if (allowChannel()) {
            super.channelRegistered();
        } else {
            denied = true;
        }
    }

    @Override
    public int consumeReads(InboundChannelBuffer channelBuffer) throws IOException {
        if (denied) {
            // Do not consume any reads if channel is disallowed
            return 0;
        } else {
            return super.consumeReads(channelBuffer);
        }
    }

    @Override
    public boolean closeNow() {
        return denied;
    }

    private boolean allowChannel() {
        if (filter != null) {
            return filter.accept(profile, remoteAddress);
        } else {
            return true;
        }
    }
}
