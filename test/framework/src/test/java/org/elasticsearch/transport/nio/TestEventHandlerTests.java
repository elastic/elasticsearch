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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.CheckedRunnable;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.nio.ServerChannelContext;
import org.elasticsearch.nio.SocketChannelContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class TestEventHandlerTests extends ESTestCase {

    private MockLogAppender appender;

    public void setUp() throws Exception {
        super.setUp();
        appender = new MockLogAppender();
        Loggers.addAppender(LogManager.getLogger(MockNioTransport.class), appender);
        appender.start();
    }

    public void tearDown() throws Exception {
        Loggers.removeAppender(LogManager.getLogger(MockNioTransport.class), appender);
        appender.stop();
        super.tearDown();
    }

    public void testLogOnElapsedTime() throws Exception {
        long start = System.nanoTime();
        long end = start + TimeUnit.MILLISECONDS.toNanos(400);
        AtomicBoolean isStart = new AtomicBoolean(true);
        LongSupplier timeSupplier = () -> {
            if (isStart.compareAndSet(true, false)) {
                return start;
            } else if (isStart.compareAndSet(false, true)) {
                return end;
            }
            throw new IllegalStateException("Cannot update isStart");
        };
        final ThreadPool threadPool = mock(ThreadPool.class);
        doAnswer(i -> timeSupplier.getAsLong()).when(threadPool).relativeTimeInNanos();
        TestEventHandler eventHandler =
            new TestEventHandler(e -> {}, () -> null, new MockNioTransport.TransportThreadWatchdog(threadPool, Settings.EMPTY));

        ServerChannelContext serverChannelContext = mock(ServerChannelContext.class);
        SocketChannelContext socketChannelContext = mock(SocketChannelContext.class);
        RuntimeException exception = new RuntimeException("boom");

        Map<String, CheckedRunnable<Exception>> tests = new HashMap<>();

        tests.put("acceptChannel", () -> eventHandler.acceptChannel(serverChannelContext));
        tests.put("acceptException", () -> eventHandler.acceptException(serverChannelContext, exception));
        tests.put("registrationException", () -> eventHandler.registrationException(socketChannelContext, exception));
        tests.put("handleConnect", () -> eventHandler.handleConnect(socketChannelContext));
        tests.put("connectException", () -> eventHandler.connectException(socketChannelContext, exception));
        tests.put("handleRead", () -> eventHandler.handleRead(socketChannelContext));
        tests.put("readException", () -> eventHandler.readException(socketChannelContext, exception));
        tests.put("handleWrite", () -> eventHandler.handleWrite(socketChannelContext));
        tests.put("writeException", () -> eventHandler.writeException(socketChannelContext, exception));
        tests.put("handleTask", () -> eventHandler.handleTask(mock(Runnable.class)));
        tests.put("taskException", () -> eventHandler.taskException(exception));
        tests.put("handleClose", () -> eventHandler.handleClose(socketChannelContext));
        tests.put("closeException", () -> eventHandler.closeException(socketChannelContext, exception));
        tests.put("genericChannelException", () -> eventHandler.genericChannelException(socketChannelContext, exception));

        for (Map.Entry<String, CheckedRunnable<Exception>> entry : tests.entrySet()) {
            String message = "*Slow execution on network thread*";
            MockLogAppender.LoggingExpectation slowExpectation =
                new MockLogAppender.SeenEventExpectation(entry.getKey(), MockNioTransport.class.getCanonicalName(), Level.WARN, message);
            appender.addExpectation(slowExpectation);
            entry.getValue().run();
            appender.assertAllExpectationsMatched();
        }
    }
}
