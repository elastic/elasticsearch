/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.nio;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.http.HttpResponse;
import org.elasticsearch.nio.NioSocketChannel;

import java.nio.channels.SocketChannel;

public class NioHttpChannel extends NioSocketChannel implements HttpChannel {

    public NioHttpChannel(SocketChannel socketChannel) {
        super(socketChannel);
    }

    public void sendResponse(HttpResponse response, ActionListener<Void> listener) {
        getContext().sendMessage(response, ActionListener.toBiConsumer(listener));
    }

    @Override
    public void addCloseListener(ActionListener<Void> listener) {
        addCloseListener(ActionListener.toBiConsumer(listener));
    }

    @Override
    public String toString() {
        return "NioHttpChannel{" +
            "localAddress=" + getLocalAddress() +
            ", remoteAddress=" + getRemoteAddress() +
            '}';
    }
}
