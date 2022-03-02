/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport.netty5;

import io.netty.channel.Channel;
import io.netty.channel.EventLoop;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.nio.channels.SocketChannel;

/**
 * Helper class to expose {@link #javaChannel()} method
 */
public class Netty5NioSocketChannel extends NioSocketChannel {

    public Netty5NioSocketChannel(EventLoop eventLoop) {
        super(eventLoop);
    }

    public Netty5NioSocketChannel(Channel parent, SocketChannel socket) {
        super(parent, parent.executor(), socket);
    }

    @Override
    public SocketChannel javaChannel() {
        return super.javaChannel();
    }

}
