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
package org.elasticsearch.grok;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.elasticsearch.test.ESTestCase;
import org.joni.Matcher;
import org.mockito.Mockito;

import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;

public class MatcherWatchdogTests extends ESTestCase {

    public void testInterrupt() throws Exception {
        AtomicBoolean run = new AtomicBoolean(true); // to avoid a lingering thread when test has completed
        MatcherWatchdog watchdog = MatcherWatchdog.newInstance(10, 100, System::currentTimeMillis, (delay, command) -> {
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
            Thread thread = new Thread(() -> {
                if (run.get()) {
                    command.run();
                }
            });
            thread.start();
        });

        Map<?, ?> registry = ((MatcherWatchdog.Default) watchdog).registry;
        assertThat(registry.size(), is(0));
        // need to call #register() method on a different thread, assertBusy() fails if current thread gets interrupted
        AtomicBoolean interrupted = new AtomicBoolean(false);
        Thread thread = new Thread(() -> {
            Matcher matcher = mock(Matcher.class);
            watchdog.register(matcher);
            verify(matcher, timeout(9999).atLeastOnce()).interrupt();
            interrupted.set(true);
            while (run.get()) {} // wait here so that the size of the registry can be asserted
            watchdog.unregister(matcher);
        });
        thread.start();
        assertBusy(() -> {
            assertThat(interrupted.get(), is(true));
            assertThat(registry.size(), is(1));
        });
        run.set(false);
        assertBusy(() -> {
            assertThat(registry.size(), is(0));
        });
    }

    public void testIdleIfNothingRegistered() throws Exception {
        long interval = 1L;
        ScheduledExecutorService threadPool = mock(ScheduledExecutorService.class);
        MatcherWatchdog watchdog = MatcherWatchdog.newInstance(interval, Long.MAX_VALUE, System::currentTimeMillis,
            (delay, command) -> threadPool.schedule(command, delay, TimeUnit.MILLISECONDS));
        // Periodic action is not scheduled because no thread is registered
        verifyZeroInteractions(threadPool);
        CompletableFuture<Runnable> commandFuture = new CompletableFuture<>();
        // Periodic action is scheduled because a thread is registered
        doAnswer(invocationOnMock -> {
            commandFuture.complete((Runnable) invocationOnMock.getArguments()[0]);
            return null;
        }).when(threadPool).schedule(
            any(Runnable.class), eq(interval), eq(TimeUnit.MILLISECONDS)
        );
        Matcher matcher = mock(Matcher.class);
        watchdog.register(matcher);
        // Registering the first thread should have caused the command to get scheduled again
        Runnable command = commandFuture.get(1L, TimeUnit.MILLISECONDS);
        Mockito.reset(threadPool);
        watchdog.unregister(matcher);
        command.run();
        // Periodic action is not scheduled again because no thread is registered
        verifyZeroInteractions(threadPool);
        watchdog.register(matcher);
        Thread otherThread = new Thread(() -> {
            Matcher otherMatcher = mock(Matcher.class);
            watchdog.register(otherMatcher);
        });
        try {
            verify(threadPool).schedule(any(Runnable.class), eq(interval), eq(TimeUnit.MILLISECONDS));
            // Registering a second thread does not cause the command to get scheduled twice
            verifyNoMoreInteractions(threadPool);
            otherThread.start();
        } finally {
            otherThread.join();
        }
    }
}
