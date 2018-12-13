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

import org.elasticsearch.nio.EventHandler;
import org.elasticsearch.nio.NioSelector;
import org.elasticsearch.nio.SocketChannelContext;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class TestingSocketEventHandler extends EventHandler {

    private Set<SocketChannelContext> hasConnectedMap = Collections.newSetFromMap(new WeakHashMap<>());

    public TestingSocketEventHandler(Consumer<Exception> exceptionHandler, Supplier<NioSelector> selectorSupplier) {
        super(exceptionHandler, selectorSupplier);
    }

    public void handleConnect(SocketChannelContext context) throws IOException {
        assert hasConnectedMap.contains(context) == false : "handleConnect should only be called is a channel is not yet connected";
        super.handleConnect(context);
        if (context.isConnectComplete()) {
            hasConnectedMap.add(context);
        }
    }

    private Set<SocketChannelContext> hasConnectExceptionMap = Collections.newSetFromMap(new WeakHashMap<>());

    public void connectException(SocketChannelContext context, Exception e) {
        assert hasConnectExceptionMap.contains(context) == false : "connectException should only called at maximum once per channel";
        hasConnectExceptionMap.add(context);
        super.connectException(context, e);
    }
}
