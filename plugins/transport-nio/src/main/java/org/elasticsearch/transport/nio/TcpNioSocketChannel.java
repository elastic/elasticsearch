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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.nio.NioSocketChannel;
import org.elasticsearch.transport.TcpChannel;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.channels.SocketChannel;

public class TcpNioSocketChannel extends NioSocketChannel implements TcpChannel {

    private final String profile;

    public TcpNioSocketChannel(String profile, SocketChannel socketChannel) throws IOException {
        super(socketChannel);
        this.profile = profile;
    }

    public void sendMessage(BytesReference reference, ActionListener<Void> listener) {
        getContext().sendMessage(BytesReference.toByteBuffers(reference), ActionListener.toBiConsumer(listener));
    }

    @Override
    public void setSoLinger(int value) throws IOException {
        if (isOpen()) {
            getRawChannel().setOption(StandardSocketOptions.SO_LINGER, value);
        }
    }

    @Override
    public String getProfile() {
        return profile;
    }

    @Override
    public void addCloseListener(ActionListener<Void> listener) {
        addCloseListener(ActionListener.toBiConsumer(listener));
    }

    @Override
    public void close() {
        getContext().closeChannel();
    }

    @Override
    public String toString() {
        return "TcpNioSocketChannel{" +
            "localAddress=" + getLocalAddress() +
            ", remoteAddress=" + getRemoteAddress() +
            '}';
    }
}
