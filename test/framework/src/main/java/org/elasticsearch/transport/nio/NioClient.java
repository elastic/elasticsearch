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

package org.elasticsearch.transport.nio;

import org.elasticsearch.transport.nio.channel.ChannelFactory;
import org.elasticsearch.transport.nio.channel.NioSocketChannel;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Semaphore;
import java.util.function.Supplier;

public class NioClient {

    private final OpenChannels openChannels;
    private final Supplier<SocketSelector> selectorSupplier;
    private final ChannelFactory channelFactory;
    private final Semaphore semaphore = new Semaphore(Integer.MAX_VALUE);

    public NioClient(OpenChannels openChannels, Supplier<SocketSelector> selectorSupplier, ChannelFactory channelFactory) {
        this.openChannels = openChannels;
        this.selectorSupplier = selectorSupplier;
        this.channelFactory = channelFactory;
    }

    public void close() {
        semaphore.acquireUninterruptibly(Integer.MAX_VALUE);
    }

    NioSocketChannel initiateConnection(InetSocketAddress address) throws IOException {
        boolean allowedToConnect = semaphore.tryAcquire();
        if (allowedToConnect == false) {
            return null;
        }

        try {
            SocketSelector selector = selectorSupplier.get();
            NioSocketChannel nioSocketChannel = channelFactory.openNioChannel(address, selector, (c) -> {});
            openChannels.clientChannelOpened(nioSocketChannel);
            return nioSocketChannel;
        } finally {
            semaphore.release();
        }
    }
}
