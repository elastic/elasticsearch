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

import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

/**
 * A base test case for cli tools.
 */
public abstract class CommandTestCase extends ESTestCase {

    /** The terminal that execute uses. */
    protected final MockTerminal terminal = new MockTerminal();

    @Before
    public void resetTerminal() {
        terminal.reset();
        terminal.setVerbosity(Terminal.Verbosity.NORMAL);
    }

    /** Creates a Command to test execution. */
    protected abstract Command newCommand();

    /**
     * Runs a command with the given args.
     *
     * Output can be found in {@link #terminal}.
     */
    public String execute(String... args) throws Exception {
        return execute(newCommand(), args);
    }

    /**
     * Runs the specified command with the given args.
     * <p>
     * Output can be found in {@link #terminal}.
     */
    public String execute(Command command, String... args) throws Exception {
        command.mainWithoutErrorHandling(args, terminal);
        return terminal.getOutput();
    }
}
