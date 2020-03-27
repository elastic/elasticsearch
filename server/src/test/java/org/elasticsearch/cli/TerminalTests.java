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

import java.io.BufferedReader;
import java.io.StringReader;

import static org.elasticsearch.cli.Terminal.readLineToCharArray;
import static org.hamcrest.Matchers.equalTo;

public class TerminalTests extends ESTestCase {

    public void testVerbosity() throws Exception {
        MockTerminal terminal = new MockTerminal();
        terminal.setVerbosity(Terminal.Verbosity.SILENT);
        assertPrinted(terminal, Terminal.Verbosity.SILENT, "text");
        assertNotPrinted(terminal, Terminal.Verbosity.NORMAL, "text");
        assertNotPrinted(terminal, Terminal.Verbosity.VERBOSE, "text");

        terminal = new MockTerminal();
        assertPrinted(terminal, Terminal.Verbosity.SILENT, "text");
        assertPrinted(terminal, Terminal.Verbosity.NORMAL, "text");
        assertNotPrinted(terminal, Terminal.Verbosity.VERBOSE, "text");

        terminal = new MockTerminal();
        terminal.setVerbosity(Terminal.Verbosity.VERBOSE);
        assertPrinted(terminal, Terminal.Verbosity.SILENT, "text");
        assertPrinted(terminal, Terminal.Verbosity.NORMAL, "text");
        assertPrinted(terminal, Terminal.Verbosity.VERBOSE, "text");
    }

    public void testErrorVerbosity() throws Exception {
        MockTerminal terminal = new MockTerminal();
        terminal.setVerbosity(Terminal.Verbosity.SILENT);
        assertErrorPrinted(terminal, Terminal.Verbosity.SILENT, "text");
        assertErrorNotPrinted(terminal, Terminal.Verbosity.NORMAL, "text");
        assertErrorNotPrinted(terminal, Terminal.Verbosity.VERBOSE, "text");

        terminal = new MockTerminal();
        assertErrorPrinted(terminal, Terminal.Verbosity.SILENT, "text");
        assertErrorPrinted(terminal, Terminal.Verbosity.NORMAL, "text");
        assertErrorNotPrinted(terminal, Terminal.Verbosity.VERBOSE, "text");

        terminal = new MockTerminal();
        terminal.setVerbosity(Terminal.Verbosity.VERBOSE);
        assertErrorPrinted(terminal, Terminal.Verbosity.SILENT, "text");
        assertErrorPrinted(terminal, Terminal.Verbosity.NORMAL, "text");
        assertErrorPrinted(terminal, Terminal.Verbosity.VERBOSE, "text");
    }


    public void testEscaping() throws Exception {
        MockTerminal terminal = new MockTerminal();
        assertPrinted(terminal, Terminal.Verbosity.NORMAL, "This message contains percent like %20n");
    }

    public void testPromptYesNoDefault() throws Exception {
        MockTerminal terminal = new MockTerminal();
        terminal.addTextInput("");
        assertTrue(terminal.promptYesNo("Answer?", true));
        terminal.addTextInput("");
        assertFalse(terminal.promptYesNo("Answer?", false));
        terminal.addTextInput(null);
        assertFalse(terminal.promptYesNo("Answer?", false));
    }

    public void testPromptYesNoReprompt() throws Exception {
        MockTerminal terminal = new MockTerminal();
        terminal.addTextInput("blah");
        terminal.addTextInput("y");
        assertTrue(terminal.promptYesNo("Answer? [Y/n]\nDid not understand answer 'blah'\nAnswer? [Y/n]", true));
    }

    public void testPromptYesNoCase() throws Exception {
        MockTerminal terminal = new MockTerminal();
        terminal.addTextInput("Y");
        assertTrue(terminal.promptYesNo("Answer?", false));
        terminal.addTextInput("y");
        assertTrue(terminal.promptYesNo("Answer?", false));
        terminal.addTextInput("N");
        assertFalse(terminal.promptYesNo("Answer?", true));
        terminal.addTextInput("n");
        assertFalse(terminal.promptYesNo("Answer?", true));
    }

    public void testMaxSecretLength() throws Exception {
        MockTerminal terminal = new MockTerminal();
        String secret = "A very long secret, too long in fact for our purposes.";
        terminal.addSecretInput(secret);

        expectThrows(IllegalStateException.class, "Secret exceeded maximum length of ",
            () -> terminal.readSecret("Secret? ", secret.length() - 1));
    }

    public void testTerminalReusesBufferedReaders() throws Exception {
        Terminal.SystemTerminal terminal = new Terminal.SystemTerminal();
        BufferedReader reader1 = terminal.getReader();
        BufferedReader reader2 = terminal.getReader();
        assertSame("System terminal should not create multiple buffered readers", reader1, reader2);
    }

    private void assertPrinted(MockTerminal logTerminal, Terminal.Verbosity verbosity, String text) throws Exception {
        logTerminal.println(verbosity, text);
        String output = logTerminal.getOutput();
        assertTrue(output, output.contains(text));
        logTerminal.reset();
    }

    private void assertNotPrinted(MockTerminal logTerminal, Terminal.Verbosity verbosity, String text) throws Exception {
        logTerminal.println(verbosity, text);
        String output = logTerminal.getOutput();
        assertTrue(output, output.isEmpty());
    }

    private void assertErrorPrinted(MockTerminal logTerminal, Terminal.Verbosity verbosity, String text) throws Exception {
        logTerminal.errorPrintln(verbosity, text);
        String output = logTerminal.getErrorOutput();
        assertTrue(output, output.contains(text));
        logTerminal.reset();
    }

    private void assertErrorNotPrinted(MockTerminal logTerminal, Terminal.Verbosity verbosity, String text) throws Exception {
        logTerminal.errorPrintln(verbosity, text);
        String output = logTerminal.getErrorOutput();
        assertTrue(output, output.isEmpty());
    }

    public void testSystemTerminalReadsSingleLines() throws Exception {
        assertRead("\n", "");
        assertRead("\r\n", "");

        assertRead("hello\n", "hello");
        assertRead("hello\r\n", "hello");

        assertRead("hellohello\n", "hellohello");
        assertRead("hellohello\r\n", "hellohello");
    }

    public void testSystemTerminalReadsMultipleLines() throws Exception {
        assertReadLines("hello\nhello\n", "hello", "hello");
        assertReadLines("hello\r\nhello\r\n", "hello", "hello");

        assertReadLines("one\ntwo\n\nthree", "one", "two", "", "three");
        assertReadLines("one\r\ntwo\r\n\r\nthree", "one", "two", "", "three");
    }

    public void testSystemTerminalLineExceedsMaxCharacters() throws Exception {
        try (StringReader reader = new StringReader("hellohellohello!\n")) {
            expectThrows(RuntimeException.class, "Input exceeded maximum length of 10",
                () -> readLineToCharArray(reader, 10));
        }
    }

    private void assertRead(String source, String expected) {
        try (StringReader reader = new StringReader(source)) {
            char[] result = readLineToCharArray(reader, 10);
            assertThat(result, equalTo(expected.toCharArray()));
        }
    }

    private void assertReadLines(String source, String... expected) {
        try (StringReader reader = new StringReader(source)) {
            char[] result;
            for (String exp : expected) {
                result = readLineToCharArray(reader, 10);
                assertThat(result, equalTo(exp.toCharArray()));
            }
        }
    }

}
