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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.nio.ChannelContext;
import org.elasticsearch.nio.EventHandler;
import org.elasticsearch.nio.NioSelector;
import org.elasticsearch.nio.ServerChannelContext;
import org.elasticsearch.nio.SocketChannelContext;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

public class TestEventHandler extends EventHandler {

    private static final Logger logger = LogManager.getLogger(TestEventHandler.class);

    private final Set<SocketChannelContext> hasConnectedMap = Collections.newSetFromMap(new WeakHashMap<>());
    private final Set<SocketChannelContext> hasConnectExceptionMap = Collections.newSetFromMap(new WeakHashMap<>());
    private final LongSupplier relativeNanosSupplier;

    TestEventHandler(Consumer<Exception> exceptionHandler, Supplier<NioSelector> selectorSupplier, LongSupplier relativeNanosSupplier) {
        super(exceptionHandler, selectorSupplier);
        this.relativeNanosSupplier = relativeNanosSupplier;
    }

    @Override
    protected void acceptChannel(ServerChannelContext context) throws IOException {
        long startTime = relativeNanosSupplier.getAsLong();
        try {
            super.acceptChannel(context);
        } finally {
            maybeLogElapsedTime(startTime);
        }
    }

    @Override
    protected void acceptException(ServerChannelContext context, Exception exception) {
        long startTime = relativeNanosSupplier.getAsLong();
        try {
            super.acceptException(context, exception);
        } finally {
            maybeLogElapsedTime(startTime);
        }
    }

    @Override
    protected void handleRegistration(ChannelContext<?> context) throws IOException {
        long startTime = relativeNanosSupplier.getAsLong();
        try {
            super.handleRegistration(context);
        } finally {
            maybeLogElapsedTime(startTime);
        }
    }

    @Override
    protected void registrationException(ChannelContext<?> context, Exception exception) {
        long startTime = relativeNanosSupplier.getAsLong();
        try {
            super.registrationException(context, exception);
        } finally {
            maybeLogElapsedTime(startTime);
        }
    }

    public void handleConnect(SocketChannelContext context) throws IOException {
        assert hasConnectedMap.contains(context) == false : "handleConnect should only be called is a channel is not yet connected";
        long startTime = relativeNanosSupplier.getAsLong();
        try {
            super.handleConnect(context);
            if (context.isConnectComplete()) {
                hasConnectedMap.add(context);
            }
        } finally {
            maybeLogElapsedTime(startTime);
        }
    }

    public void connectException(SocketChannelContext context, Exception e) {
        assert hasConnectExceptionMap.contains(context) == false : "connectException should only called at maximum once per channel";
        hasConnectExceptionMap.add(context);
        long startTime = relativeNanosSupplier.getAsLong();
        try {
            super.connectException(context, e);
        } finally {
            maybeLogElapsedTime(startTime);
        }
    }

    @Override
    protected void handleRead(SocketChannelContext context) throws IOException {
        long startTime = relativeNanosSupplier.getAsLong();
        try {
            super.handleRead(context);
        } finally {
            maybeLogElapsedTime(startTime);
        }
    }

    @Override
    protected void readException(SocketChannelContext context, Exception exception) {
        long startTime = relativeNanosSupplier.getAsLong();
        try {
            super.readException(context, exception);
        } finally {
            maybeLogElapsedTime(startTime);
        }
    }

    @Override
    protected void handleWrite(SocketChannelContext context) throws IOException {
        long startTime = relativeNanosSupplier.getAsLong();
        try {
            super.handleWrite(context);
        } finally {
            maybeLogElapsedTime(startTime);
        }
    }

    @Override
    protected void writeException(SocketChannelContext context, Exception exception) {
        long startTime = relativeNanosSupplier.getAsLong();
        try {
            super.writeException(context, exception);
        } finally {
            maybeLogElapsedTime(startTime);
        }
    }

    @Override
    protected void handleTask(Runnable task) {
        long startTime = relativeNanosSupplier.getAsLong();
        try {
            super.handleTask(task);
        } finally {
            maybeLogElapsedTime(startTime);
        }
    }

    @Override
    protected void taskException(Exception exception) {
        long startTime = relativeNanosSupplier.getAsLong();
        try {
            super.taskException(exception);
        } finally {
            maybeLogElapsedTime(startTime);
        }
    }

    @Override
    protected void handleClose(ChannelContext<?> context) throws IOException {
        long startTime = relativeNanosSupplier.getAsLong();
        try {
            super.handleClose(context);
        } finally {
            maybeLogElapsedTime(startTime);
        }
    }

    @Override
    protected void closeException(ChannelContext<?> context, Exception exception) {
        long startTime = relativeNanosSupplier.getAsLong();
        try {
            super.closeException(context, exception);
        } finally {
            maybeLogElapsedTime(startTime);
        }
    }

    @Override
    protected void genericChannelException(ChannelContext<?> context, Exception exception) {
        long startTime = relativeNanosSupplier.getAsLong();
        try {
            super.genericChannelException(context, exception);
        } finally {
            maybeLogElapsedTime(startTime);
        }
    }

    private static final long WARN_THRESHOLD = 150;

    private void maybeLogElapsedTime(long startTime) {
        long elapsedTime = TimeUnit.NANOSECONDS.toMillis(relativeNanosSupplier.getAsLong() - startTime);
        if (elapsedTime > WARN_THRESHOLD) {
            logger.warn(new ParameterizedMessage("Slow execution on network thread [{} milliseconds]", elapsedTime),
                new RuntimeException("Slow exception on network thread"));
        }
    }
}
