/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
