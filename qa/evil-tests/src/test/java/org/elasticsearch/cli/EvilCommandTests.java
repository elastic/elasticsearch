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

package org.elasticsearch.cli;

import joptsimple.OptionSet;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.isEmptyString;

public class EvilCommandTests extends ESTestCase {

    public void testCommandShutdownHook() throws Exception {
        final AtomicBoolean closed = new AtomicBoolean();
        final boolean shouldThrow = randomBoolean();
        final Command command = new Command("test-command-shutdown-hook", () -> {}) {
            @Override
            protected void execute(Terminal terminal, OptionSet options) throws Exception {

            }

            @Override
            public void close() throws IOException {
                closed.set(true);
                if (shouldThrow) {
                    throw new IOException("fail");
                }
            }
        };
        final MockTerminal terminal = new MockTerminal();
        command.main(new String[0], terminal);
        assertNotNull(command.getShutdownHookThread());
        // successful removal here asserts that the runtime hook was installed in Command#main
        assertTrue(Runtime.getRuntime().removeShutdownHook(command.getShutdownHookThread()));
        command.getShutdownHookThread().run();
        command.getShutdownHookThread().join();
        assertTrue(closed.get());
        final String output = terminal.getOutput();
        if (shouldThrow) {
            // ensure that we dump the exception
            assertThat(output, containsString("java.io.IOException: fail"));
            // ensure that we dump the stack trace too
            assertThat(output, containsString("\tat org.elasticsearch.cli.EvilCommandTests$1.close"));
        } else {
            assertThat(output, isEmptyString());
        }
    }

}
