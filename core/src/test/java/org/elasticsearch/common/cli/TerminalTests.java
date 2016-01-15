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

package org.elasticsearch.common.cli;

import java.nio.file.NoSuchFileException;
import java.util.List;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;

/**
 *
 */
public class TerminalTests extends CliToolTestCase {
    public void testVerbosity() throws Exception {
        CaptureOutputTerminal terminal = new CaptureOutputTerminal(Terminal.Verbosity.SILENT);
        assertPrinted(terminal, Terminal.Verbosity.SILENT, "text");
        assertNotPrinted(terminal, Terminal.Verbosity.NORMAL, "text");
        assertNotPrinted(terminal, Terminal.Verbosity.VERBOSE, "text");

        terminal = new CaptureOutputTerminal(Terminal.Verbosity.NORMAL);
        assertPrinted(terminal, Terminal.Verbosity.SILENT, "text");
        assertPrinted(terminal, Terminal.Verbosity.NORMAL, "text");
        assertNotPrinted(terminal, Terminal.Verbosity.VERBOSE, "text");

        terminal = new CaptureOutputTerminal(Terminal.Verbosity.VERBOSE);
        assertPrinted(terminal, Terminal.Verbosity.SILENT, "text");
        assertPrinted(terminal, Terminal.Verbosity.NORMAL, "text");
        assertPrinted(terminal, Terminal.Verbosity.VERBOSE, "text");
    }

    public void testError() throws Exception {
        try {
            // actually throw so we have a stacktrace
            throw new NoSuchFileException("/path/to/some/file");
        } catch (NoSuchFileException e) {
            CaptureOutputTerminal terminal = new CaptureOutputTerminal(Terminal.Verbosity.NORMAL);
            terminal.printError(e);
            List<String> output = terminal.getTerminalOutput();
            assertFalse(output.isEmpty());
            assertTrue(output.get(0), output.get(0).contains("NoSuchFileException")); // exception class
            assertTrue(output.get(0), output.get(0).contains("/path/to/some/file")); // message
            assertEquals(1, output.size());

            // TODO: we should test stack trace is printed in debug mode...except debug is a sysprop instead of
            // a command line param...maybe it should be VERBOSE instead of a separate debug prop?
        }
    }

    private void assertPrinted(CaptureOutputTerminal logTerminal, Terminal.Verbosity verbosity, String text) {
        logTerminal.print(verbosity, text);
        assertThat(logTerminal.getTerminalOutput(), hasSize(1));
        assertThat(logTerminal.getTerminalOutput(), hasItem(text));
        logTerminal.terminalOutput.clear();
    }

    private void assertNotPrinted(CaptureOutputTerminal logTerminal, Terminal.Verbosity verbosity, String text) {
        logTerminal.print(verbosity, text);
        assertThat(logTerminal.getTerminalOutput(), hasSize(0));
    }
}
