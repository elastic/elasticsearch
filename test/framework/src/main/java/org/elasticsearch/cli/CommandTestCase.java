/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cli;

import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matcher;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.emptyString;

/**
 * A base test case for cli tools.
 */
@ESTestCase.WithoutSecurityManager
public abstract class CommandTestCase extends ESTestCase {

    /** The terminal that execute uses */
    protected final MockTerminal terminal = MockTerminal.create();

    /** The system properties that execute uses */
    protected final Map<String, String> sysprops = new HashMap<>();

    /** The environment variables that execute uses */
    protected final Map<String, String> envVars = new HashMap<>();

    /** The working directory that execute uses */
    protected Path esHomeDir;

    /** The ES config dir */
    protected Path configDir;

    /** Whether to include a whitespace in the file-system path. */
    private final boolean spaceInPath;

    protected CommandTestCase() {
        this(false);
    }

    protected CommandTestCase(boolean spaceInPath) {
        this.spaceInPath = spaceInPath;
    }

    @Before
    public void resetTerminal() throws IOException {
        terminal.reset();
        terminal.setSupportsBinary(false);
        terminal.setVerbosity(Terminal.Verbosity.NORMAL);
        if (spaceInPath) {
            esHomeDir = createTempDir("a b"); // contains a whitespace
        } else {
            esHomeDir = createTempDir();
        }
        configDir = esHomeDir.resolve("config");
        Files.createDirectory(configDir);
        sysprops.clear();
        sysprops.put("es.path.home", esHomeDir.toString());
        sysprops.put("es.path.conf", esHomeDir.resolve("config").toString());
        sysprops.put("os.name", "Linux"); // default to linux, tests can override to check specific OS behavior
        envVars.clear();
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

    protected void assertOk(String... args) throws Exception {
        assertOkWithOutput(emptyString(), emptyString(), args);
    }

    protected void assertOkWithOutput(Matcher<String> outMatcher, Matcher<String> errMatcher, String... args) throws Exception {
        int status = executeMain(args);
        assertThat(status, equalTo(ExitCodes.OK));
        assertThat(terminal.getErrorOutput(), errMatcher);
        assertThat(terminal.getOutput(), outMatcher);
    }

    protected void assertUsage(Matcher<String> matcher, String... args) throws Exception {
        terminal.reset();
        int status = executeMain(args);
        assertThat(status, equalTo(ExitCodes.USAGE));
        assertThat(terminal.getErrorOutput(), matcher);
    }
}
