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

import org.elasticsearch.test.ESTestCase;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

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
    
}
