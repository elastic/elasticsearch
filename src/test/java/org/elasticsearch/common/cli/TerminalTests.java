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

import org.junit.Test;

import static org.hamcrest.Matchers.*;

/**
 *
 */
public class TerminalTests extends CliToolTestCase {

    @Test
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

    private void assertPrinted(CaptureOutputTerminal logTerminal, Terminal.Verbosity verbosity, String text) {
        logTerminal.print(verbosity, text);
        assertThat(logTerminal.getTerminalOutput(), hasSize(1));
        assertThat(logTerminal.getTerminalOutput(), hasItem(is("text")));
        logTerminal.terminalOutput.clear();
    }

    private void assertNotPrinted(CaptureOutputTerminal logTerminal, Terminal.Verbosity verbosity, String text) {
        logTerminal.print(verbosity, text);
        assertThat(logTerminal.getTerminalOutput(), hasSize(0));
    }
}
