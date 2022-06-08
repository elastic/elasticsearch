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

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * A base test case for cli tools.
 */
public abstract class CommandTestCase extends ESTestCase {

    /** The terminal that execute uses */
    protected final MockTerminal terminal = MockTerminal.create();

    /** The system properties that execute uses */
    protected final Map<String, String> sysprops = new HashMap<>();

    /** The environment variables that execute uses */
    protected final Map<String, String> envVars = new HashMap<>();

    /** The working directory that execute uses */
    protected Path esHomeDir;

    @Before
    public void resetTerminal() {
        terminal.reset();
        terminal.setSupportsBinary(false);
        terminal.setVerbosity(Terminal.Verbosity.NORMAL);
        esHomeDir = createTempDir();
        sysprops.clear();
        sysprops.put("es.path.home", esHomeDir.toString());
        sysprops.put("es.path.conf", esHomeDir.resolve("config").toString());
        sysprops.put("os.name", "Linux"); // default to linux, tests can override to check specific OS behavior
        envVars.clear();
    }

    protected static Map<String, String> mockSystemProperties(Path homeDir) {
        return Map.of("es.path.home", homeDir.toString(), "es.path.conf", homeDir.resolve("config").toString());
    }

    /** Creates a Command to test execution. */
    protected abstract Command newCommand();

    /**
     * Run the main method of a new command with the given args.
     *
     * Output can be found in {@link #terminal}.
     */
    public int executeMain(String... args) throws Exception {
        return newCommand().main(args, terminal, new ProcessInfo(sysprops, envVars, esHomeDir));
    }

    /**
     * Runs a new command with the given args.
     *
     * Output can be found in {@link #terminal}.
     */
    public String execute(String... args) throws Exception {
        return execute(newCommand(), args);
    }

    /**
     * Runs the specified command with the given args, throwing exceptions on errors.
     * <p>
     * Output can be found in {@link #terminal}.
     */
    public String execute(Command command, String... args) throws Exception {
        command.mainWithoutErrorHandling(args, terminal, new ProcessInfo(sysprops, envVars, esHomeDir));
        return terminal.getOutput();
    }
}
