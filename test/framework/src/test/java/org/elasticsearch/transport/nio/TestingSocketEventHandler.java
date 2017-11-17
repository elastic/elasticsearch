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

import org.apache.logging.log4j.Logger;
import org.elasticsearch.transport.nio.channel.NioSocketChannel;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.function.BiConsumer;

public class TestingSocketEventHandler extends SocketEventHandler {

    public TestingSocketEventHandler(Logger logger, BiConsumer<NioSocketChannel, Exception> exceptionHandler, OpenChannels openChannels) {
        super(logger, exceptionHandler, openChannels);
    }

    private Set<NioSocketChannel> hasConnectedMap = Collections.newSetFromMap(new WeakHashMap<>());

    public void handleConnect(NioSocketChannel channel) {
        assert hasConnectedMap.contains(channel) == false : "handleConnect should only be called once per channel";
        hasConnectedMap.add(channel);
        super.handleConnect(channel);
    }

    private Set<NioSocketChannel> hasConnectExceptionMap = Collections.newSetFromMap(new WeakHashMap<>());

    public void connectException(NioSocketChannel channel, Exception e) {
        assert hasConnectExceptionMap.contains(channel) == false : "connectException should only called at maximum once per channel";
        hasConnectExceptionMap.add(channel);
        super.connectException(channel, e);
    }

    public void handleRead(NioSocketChannel channel) throws IOException {
        super.handleRead(channel);
    }

    public void readException(NioSocketChannel channel, Exception e) {
        super.readException(channel, e);
    }

    public void handleWrite(NioSocketChannel channel) throws IOException {
        super.handleWrite(channel);
    }

    public void writeException(NioSocketChannel channel, Exception e) {
        super.writeException(channel, e);
    }

}
