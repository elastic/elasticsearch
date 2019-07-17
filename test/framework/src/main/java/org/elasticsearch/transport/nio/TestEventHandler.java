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

import org.elasticsearch.nio.ChannelContext;
import org.elasticsearch.nio.EventHandler;
import org.elasticsearch.nio.NioSelector;
import org.elasticsearch.nio.ServerChannelContext;
import org.elasticsearch.nio.SocketChannelContext;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class TestEventHandler extends EventHandler {

    private final Set<SocketChannelContext> hasConnectedMap = Collections.newSetFromMap(new WeakHashMap<>());
    private final Set<SocketChannelContext> hasConnectExceptionMap = Collections.newSetFromMap(new WeakHashMap<>());
    private final MockNioTransport.TransportThreadWatchdog transportThreadWatchdog;

    TestEventHandler(Consumer<Exception> exceptionHandler, Supplier<NioSelector> selectorSupplier,
                     MockNioTransport.TransportThreadWatchdog transportThreadWatchdog) {
        super(exceptionHandler, selectorSupplier);
        this.transportThreadWatchdog = transportThreadWatchdog;
    }

    @Override
    protected void acceptChannel(ServerChannelContext context) throws IOException {
        final boolean registered = transportThreadWatchdog.register();
        try {
            super.acceptChannel(context);
        } finally {
            if (registered) {
                transportThreadWatchdog.unregister();
            }
        }
    }

    @Override
    protected void acceptException(ServerChannelContext context, Exception exception) {
        final boolean registered = transportThreadWatchdog.register();
        try {
            super.acceptException(context, exception);
        } finally {
            if (registered) {
                transportThreadWatchdog.unregister();
            }
        }
    }

    @Override
    protected void handleRegistration(ChannelContext<?> context) throws IOException {
        final boolean registered = transportThreadWatchdog.register();
        try {
            super.handleRegistration(context);
        } finally {
            if (registered) {
                transportThreadWatchdog.unregister();
            }
        }
    }

    @Override
    protected void registrationException(ChannelContext<?> context, Exception exception) {
        final boolean registered = transportThreadWatchdog.register();
        try {
            super.registrationException(context, exception);
        } finally {
            if (registered) {
                transportThreadWatchdog.unregister();
            }
        }
    }

    @Override
    protected void handleActive(ChannelContext<?> context) throws IOException {
        final boolean registered = transportThreadWatchdog.register();
        try {
            super.handleActive(context);
        } finally {
            if (registered) {
                transportThreadWatchdog.unregister();
            }
        }
    }

    @Override
    protected void activeException(ChannelContext<?> context, Exception exception) {
        final boolean registered = transportThreadWatchdog.register();
        try {
            super.activeException(context, exception);
        } finally {
            if (registered) {
                transportThreadWatchdog.unregister();
            }
        }
    }

    public void handleConnect(SocketChannelContext context) throws IOException {
        assert hasConnectedMap.contains(context) == false : "handleConnect should only be called is a channel is not yet connected";
        final boolean registered = transportThreadWatchdog.register();
        try {
            super.handleConnect(context);
            if (context.isConnectComplete()) {
                hasConnectedMap.add(context);
            }
        } finally {
            if (registered) {
                transportThreadWatchdog.unregister();
            }
        }
    }

    public void connectException(SocketChannelContext context, Exception e) {
        assert hasConnectExceptionMap.contains(context) == false : "connectException should only called at maximum once per channel";
        final boolean registered = transportThreadWatchdog.register();
        hasConnectExceptionMap.add(context);
        try {
            super.connectException(context, e);
        } finally {
            if (registered) {
                transportThreadWatchdog.unregister();
            }
        }
    }

    @Override
    protected void handleRead(SocketChannelContext context) throws IOException {
        final boolean registered = transportThreadWatchdog.register();
        try {
            super.handleRead(context);
        } finally {
            if (registered) {
                transportThreadWatchdog.unregister();
            }
        }
    }

    @Override
    protected void readException(SocketChannelContext context, Exception exception) {
        final boolean registered = transportThreadWatchdog.register();
        try {
            super.readException(context, exception);
        } finally {
            if (registered) {
                transportThreadWatchdog.unregister();
            }
        }
    }

    @Override
    protected void handleWrite(SocketChannelContext context) throws IOException {
        final boolean registered = transportThreadWatchdog.register();
        try {
            super.handleWrite(context);
        } finally {
            if (registered) {
                transportThreadWatchdog.unregister();
            }
        }
    }

    @Override
    protected void writeException(SocketChannelContext context, Exception exception) {
        final boolean registered = transportThreadWatchdog.register();
        try {
            super.writeException(context, exception);
        } finally {
            if (registered) {
                transportThreadWatchdog.unregister();
            }
        }
    }

    @Override
    protected void handleTask(Runnable task) {
        final boolean registered = transportThreadWatchdog.register();
        try {
            super.handleTask(task);
        } finally {
            if (registered) {
                transportThreadWatchdog.unregister();
            }
        }
    }

    @Override
    protected void taskException(Exception exception) {
        final boolean registered = transportThreadWatchdog.register();
        try {
            super.taskException(exception);
        } finally {
            if (registered) {
                transportThreadWatchdog.unregister();
            }
        }
    }

    @Override
    protected void handleClose(ChannelContext<?> context) throws IOException {
        final boolean registered = transportThreadWatchdog.register();
        try {
            super.handleClose(context);
        } finally {
            if (registered) {
                transportThreadWatchdog.unregister();
            }
        }
    }

    @Override
    protected void closeException(ChannelContext<?> context, Exception exception) {
        final boolean registered = transportThreadWatchdog.register();
        try {
            super.closeException(context, exception);
        } finally {
            if (registered) {
                transportThreadWatchdog.unregister();
            }
        }
    }

    @Override
    protected void genericChannelException(ChannelContext<?> context, Exception exception) {
        final boolean registered = transportThreadWatchdog.register();
        try {
            super.genericChannelException(context, exception);
        } finally {
            if (registered) {
                transportThreadWatchdog.unregister();
            }
        }
    }
}
