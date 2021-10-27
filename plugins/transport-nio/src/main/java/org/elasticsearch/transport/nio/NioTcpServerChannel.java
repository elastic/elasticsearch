/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport.nio;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.nio.NioServerSocketChannel;
import org.elasticsearch.transport.TcpServerChannel;

import java.nio.channels.ServerSocketChannel;

/**
 * This is an implementation of {@link NioServerSocketChannel} that adheres to the {@link TcpServerChannel}
 * interface. As it is a server socket, setting SO_LINGER and sending messages is not supported.
 */
public class NioTcpServerChannel extends NioServerSocketChannel implements TcpServerChannel {

    public NioTcpServerChannel(ServerSocketChannel socketChannel) {
        super(socketChannel);
    }

    public void close() {
        getContext().closeChannel();
    }

    @Override
    public void addCloseListener(ActionListener<Void> listener) {
        addCloseListener(ActionListener.toBiConsumer(listener));
    }

    @Override
    public String toString() {
        return "TcpNioServerSocketChannel{" + "localAddress=" + getLocalAddress() + '}';
    }
}
