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

public class ThreadInterrupterTests extends ESTestCase {
    
    public void testInterrupt() throws Exception {
        AtomicBoolean run = new AtomicBoolean(true); // to avoid a lingering thread when test has completed
        ThreadInterrupter guard = ThreadInterrupter.newInstance(10, 100, System::currentTimeMillis, (delay, command) -> {
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
            }
            Thread thread = new Thread(() -> {
                if (run.get()) {
                    command.run();
                }
            });
            thread.start();
            return null;
        });
    
        Map<?, ?> registry = ((ThreadInterrupter.Default) guard).registry;
        assertThat(registry.size(), is(0));
        // need to call #register() method on a different thread, assertBusy() fails if current thread gets interrupted
        Thread thread = new Thread(() -> {
            guard.register();
            while (run.get()) {
            }
            guard.deregister();
        });
        thread.start();
        assertBusy(() -> {
            assertThat(thread.isInterrupted(), is(true));
            assertThat(registry.size(), is(1));
        });
        run.set(false);
        assertBusy(() -> {
            assertThat(registry.size(), is(0));
        });
    }
    
}
