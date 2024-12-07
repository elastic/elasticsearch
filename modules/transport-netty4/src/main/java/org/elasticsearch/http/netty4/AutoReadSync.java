/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http.netty4;

import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.util.AttributeKey;

import java.util.BitSet;

/**
 * AutoReadSync provides coordinated access to the {@link ChannelConfig#setAutoRead(boolean)}.
 * We use autoRead flag for the data flow control in the channel pipeline to prevent excessive
 * buffering inside channel handlers. Every actor in the pipeline should obtain its own {@link Handle}
 * by calling {@link AutoReadSync#getHandle} channel. Channel autoRead is enabled as long as all Handles
 * are enabled. If one of handles disables autoRead, channel autoRead disables too.
 * Simply, {@code channel.setAutoRead(allHandlesTrue)}.
 * <br><br>
 * TODO: this flow control should be removed when {@link Netty4HttpHeaderValidator} moves to RestController.
 *   And whole control flow can be simplified to {@link io.netty.handler.flow.FlowControlHandler}.
 */
class AutoReadSync {

    private static final AttributeKey<AutoReadSync> AUTO_READ_SYNC_KEY = AttributeKey.valueOf("AutoReadSync");
    private final Channel channel;
    private final ChannelConfig config;

    // A pool of reusable handles and their states. We use sequence number in the bitset for Handle
    // identity. Handles bitset represents leases from pool. Toggles bitset represents state of the handle.
    // Default value for toggle is 0, which means autoRead is enabled.
    private final BitSet handles;
    private final BitSet toggles;

    AutoReadSync(Channel channel) {
        this.channel = channel;
        this.config = channel.config();
        this.handles = new BitSet();
        this.toggles = new BitSet();
    }

    static Handle getHandle(Channel channel) {
        assert channel.eventLoop().inEventLoop();
        var autoRead = channel.attr(AUTO_READ_SYNC_KEY).get();
        if (autoRead == null) {
            autoRead = new AutoReadSync(channel);
            channel.attr(AUTO_READ_SYNC_KEY).set(autoRead);
        }
        return autoRead.getHandle();
    }

    Handle getHandle() {
        var handleId = handles.nextClearBit(0); // next unused handle id
        handles.set(handleId, true); // acquire lease
        return new Handle(handleId);
    }

    class Handle {
        private final int id;
        private boolean released;

        Handle(int id) {
            this.id = id;
        }

        private void assertState() {
            assert channel.eventLoop().inEventLoop();
            assert released == false;
        }

        boolean isEnabled() {
            assertState();
            return toggles.get(id) == false;
        }

        void enable() {
            assertState();
            toggles.set(id, false);
            config.setAutoRead(toggles.isEmpty());
        }

        void disable() {
            assertState();
            toggles.set(id, true);
            config.setAutoRead(false);
        }

        void release() {
            assertState();
            enable();
            handles.set(id, false);
            released = true;
        }
    }

}
