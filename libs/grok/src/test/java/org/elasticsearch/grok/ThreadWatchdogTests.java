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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.is;

public class ThreadWatchdogTests extends ESTestCase {

    public void testInterrupt() throws Exception {
        AtomicBoolean run = new AtomicBoolean(true); // to avoid a lingering thread when test has completed
        ThreadWatchdog watchdog = ThreadWatchdog.newInstance(10, 100, System::currentTimeMillis, (delay, command) -> {
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
            return null;
        });

        Map<?, ?> registry = ((ThreadWatchdog.Default) watchdog).registry;
        assertThat(registry.size(), is(0));
        // need to call #register() method on a different thread, assertBusy() fails if current thread gets interrupted
        AtomicBoolean interrupted = new AtomicBoolean(false);
        Thread thread = new Thread(() -> {
            Thread currentThread = Thread.currentThread();
            watchdog.register();
            while (currentThread.isInterrupted() == false) {}
            interrupted.set(true);
            while (run.get()) {} // wait here so that the size of the registry can be asserted
            watchdog.unregister();
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
        AtomicInteger counter = new AtomicInteger(0);
        ScheduledExecutorService threadPool = Executors.newSingleThreadScheduledExecutor();
        try {
            ThreadWatchdog watchdog = ThreadWatchdog.newInstance(interval, Long.MAX_VALUE, System::currentTimeMillis, (delay, command) -> {
                counter.incrementAndGet();
                return threadPool.schedule(command, delay, TimeUnit.MILLISECONDS);
            });
            // Counter was not incremented since watchdog does not run without a registered thread
            assertEquals(0, counter.get());
            watchdog.register();
            Thread.sleep(5L * interval);
            // Counter is incremented because thread is registered and it runs every ms
            assertTrue(counter.get() > 0);
            watchdog.unregister();
            Thread.sleep(5L * interval);
            // Wait for counter to stop incrementing because no thread is registered
            int beginningCount = counter.get();
            Thread.sleep(5L * interval);
            // Without a registered thread counter again does not increment
            assertEquals(beginningCount, counter.get());
            watchdog.register();
            Thread.sleep(5L * interval);
            // Registering the thread again makes the counter increment again
            assertTrue(counter.get() > beginningCount);
        } finally {
            threadPool.shutdownNow();
            if (!threadPool.awaitTermination(2L, TimeUnit.SECONDS)) {
                fail("Threadpool failed to shut down.");
            }
        }
    }
}
