/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.nio;

import java.io.IOException;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;

public class NioServerSocketChannel extends NioChannel {

    private final ServerSocketChannel socketChannel;
    private final AtomicBoolean contextSet = new AtomicBoolean(false);
    private ServerChannelContext context;

    public NioServerSocketChannel(ServerSocketChannel socketChannel) throws IOException {
        super(socketChannel);
        this.socketChannel = socketChannel;
    }

    /**
     * This method sets the context for a server socket channel. The context is called when a new channel is
     * accepted, an exception occurs, or it is time to close the channel.
     *
     * @param context to call
     */
    public void setContext(ServerChannelContext context) {
        if (contextSet.compareAndSet(false, true)) {
            this.context = context;
        } else {
            throw new IllegalStateException("Context on this channel were already set. It should only be once.");
        }
    }

    @Override
    public ServerSocketChannel getRawChannel() {
        return socketChannel;
    }

    @Override
    public ServerChannelContext getContext() {
        return context;
    }

    @Override
    public String toString() {
        return "NioServerSocketChannel{" +
            "localAddress=" + getLocalAddress() +
            '}';
    }
}
