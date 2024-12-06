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

class AutoReadSync {

    private static final AttributeKey<AutoReadSync> AUTO_READ_SYNC_KEY = AttributeKey.valueOf("AutoReadSync");
    private final Channel channel;
    private final ChannelConfig config;
    private final BitSet handles;
    private final BitSet toggles;

    AutoReadSync(Channel channel) {
        this.channel = channel;
        this.config = channel.config();
        this.handles = new BitSet();
        this.toggles = new BitSet();
    }

    static Handle from(Channel channel) {
        assert channel.eventLoop().inEventLoop();
        var autoRead = channel.attr(AUTO_READ_SYNC_KEY).get();
        if (autoRead == null) {
            autoRead = new AutoReadSync(channel);
            channel.attr(AUTO_READ_SYNC_KEY).set(autoRead);
        }
        return autoRead.getHandle();
    }

    Handle getHandle() {
        var handleId = handles.nextClearBit(0);
        handles.set(handleId, true);
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
