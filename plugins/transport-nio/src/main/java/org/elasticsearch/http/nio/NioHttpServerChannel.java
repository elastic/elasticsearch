/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.nio;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.http.HttpServerChannel;
import org.elasticsearch.nio.NioServerSocketChannel;

import java.nio.channels.ServerSocketChannel;

public class NioHttpServerChannel extends NioServerSocketChannel implements HttpServerChannel {

    public NioHttpServerChannel(ServerSocketChannel serverSocketChannel) {
        super(serverSocketChannel);
    }

    @Override
    public void addCloseListener(ActionListener<Void> listener) {
        addCloseListener(ActionListener.toBiConsumer(listener));
    }

    @Override
    public String toString() {
        return "NioHttpServerChannel{localAddress=" + getLocalAddress() + "}";
    }
}
