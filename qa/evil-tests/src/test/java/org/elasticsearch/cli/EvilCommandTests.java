/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cli;

import joptsimple.OptionSet;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.is;

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
        final String output = terminal.getErrorOutput();
        if (shouldThrow) {
            // ensure that we dump the exception
            assertThat(output, containsString("java.io.IOException: fail"));
            // ensure that we dump the stack trace too
            assertThat(output, containsString("\tat org.elasticsearch.cli.EvilCommandTests$1.close"));
        } else {
            assertThat(output, is(emptyString()));
        }
    }

}
