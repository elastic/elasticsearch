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

package org.elasticsearch.transport.nio.channel;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.transport.TcpChannel;
import org.elasticsearch.transport.nio.AcceptingSelector;

import java.io.IOException;
import java.nio.channels.ServerSocketChannel;

/**
 * This is an implementation of {@link NioServerSocketChannel} that adheres to the {@link TcpChannel}
 * interface. As it is a server socket, setting SO_LINGER and sending messages is not supported.
 */
public class TcpNioServerSocketChannel extends NioServerSocketChannel implements TcpChannel {

    TcpNioServerSocketChannel(ServerSocketChannel socketChannel, TcpChannelFactory channelFactory, AcceptingSelector selector)
        throws IOException {
        super(socketChannel, channelFactory, selector);
    }

    @Override
    public void sendMessage(BytesReference reference, ActionListener<Void> listener) {
        throw new UnsupportedOperationException("Cannot send a message to a server channel.");
    }

    @Override
    public void setSoLinger(int value) throws IOException {
        throw new UnsupportedOperationException("Cannot set SO_LINGER on a server channel.");
    }

    @Override
    public String toString() {
        return "TcpNioServerSocketChannel{" +
            "localAddress=" + getLocalAddress() +
            '}';
    }
}
