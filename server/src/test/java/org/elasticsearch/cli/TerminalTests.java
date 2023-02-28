/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cli;

import org.elasticsearch.test.ESTestCase;

import java.io.StringReader;

import static org.elasticsearch.cli.Terminal.readLineToCharArray;
import static org.hamcrest.Matchers.equalTo;

public class TerminalTests extends ESTestCase {

    public void testVerbosity() throws Exception {
        MockTerminal terminal = MockTerminal.create();
        terminal.setVerbosity(Terminal.Verbosity.SILENT);
        assertPrinted(terminal, Terminal.Verbosity.SILENT, "text");
        assertNotPrinted(terminal, Terminal.Verbosity.NORMAL, "text");
        assertNotPrinted(terminal, Terminal.Verbosity.VERBOSE, "text");

        terminal = MockTerminal.create();
        assertPrinted(terminal, Terminal.Verbosity.SILENT, "text");
        assertPrinted(terminal, Terminal.Verbosity.NORMAL, "text");
        assertNotPrinted(terminal, Terminal.Verbosity.VERBOSE, "text");

        terminal = MockTerminal.create();
        terminal.setVerbosity(Terminal.Verbosity.VERBOSE);
        assertPrinted(terminal, Terminal.Verbosity.SILENT, "text");
        assertPrinted(terminal, Terminal.Verbosity.NORMAL, "text");
        assertPrinted(terminal, Terminal.Verbosity.VERBOSE, "text");
    }

    public void testErrorVerbosity() throws Exception {
        MockTerminal terminal = MockTerminal.create();
        terminal.setVerbosity(Terminal.Verbosity.SILENT);
        assertErrorPrinted(terminal, Terminal.Verbosity.SILENT, "text");
        assertErrorNotPrinted(terminal, Terminal.Verbosity.NORMAL, "text");
        assertErrorNotPrinted(terminal, Terminal.Verbosity.VERBOSE, "text");

        terminal = MockTerminal.create();
        assertErrorPrinted(terminal, Terminal.Verbosity.SILENT, "text");
        assertErrorPrinted(terminal, Terminal.Verbosity.NORMAL, "text");
        assertErrorNotPrinted(terminal, Terminal.Verbosity.VERBOSE, "text");

        terminal = MockTerminal.create();
        terminal.setVerbosity(Terminal.Verbosity.VERBOSE);
        assertErrorPrinted(terminal, Terminal.Verbosity.SILENT, "text");
        assertErrorPrinted(terminal, Terminal.Verbosity.NORMAL, "text");
        assertErrorPrinted(terminal, Terminal.Verbosity.VERBOSE, "text");
    }

    public void testEscaping() throws Exception {
        MockTerminal terminal = MockTerminal.create();
        assertPrinted(terminal, Terminal.Verbosity.NORMAL, "This message contains percent like %20n");
    }

    public void testPromptYesNoDefault() throws Exception {
        MockTerminal terminal = MockTerminal.create();
        terminal.addTextInput("");
        assertTrue(terminal.promptYesNo("Answer?", true));
        terminal.addTextInput("");
        assertFalse(terminal.promptYesNo("Answer?", false));
    }

    public void testPromptYesNoReprompt() throws Exception {
        MockTerminal terminal = MockTerminal.create();
        terminal.addTextInput("blah");
        terminal.addTextInput("y");
        assertTrue(terminal.promptYesNo("Answer? [Y/n]\nDid not understand answer 'blah'\nAnswer? [Y/n]", true));
    }

    public void testPromptYesNoCase() throws Exception {
        MockTerminal terminal = MockTerminal.create();
        terminal.addTextInput("Y");
        assertTrue(terminal.promptYesNo("Answer?", false));
        terminal.addTextInput("y");
        assertTrue(terminal.promptYesNo("Answer?", false));
        terminal.addTextInput("N");
        assertFalse(terminal.promptYesNo("Answer?", true));
        terminal.addTextInput("n");
        assertFalse(terminal.promptYesNo("Answer?", true));
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

    public void testReadLineToCharArrayBufferExpansion() throws Exception {
        String passphrase = randomAlphaOfLength(128);
        assertRead(passphrase + "\n", passphrase);
        assertRead(passphrase + "\r\n", passphrase);
    }

    /**
     * Tests an edge case when read buffer gets completely filled (up to 128 chars) with the last character being carriage return
     * and asserts that this last CR character is properly removed.
     */
    public void testReadLineToCharArrayBufferWithCarriageReturnRemoval() throws Exception {
        String passphrase = randomAlphaOfLength(127);
        assertRead(passphrase + "\n", passphrase);
        assertRead(passphrase + "\r\n", passphrase);
    }

    private void assertRead(String source, String expected) {
        try (StringReader reader = new StringReader(source)) {
            char[] result = readLineToCharArray(reader);
            assertThat(result, equalTo(expected.toCharArray()));
        }
    }

    private void assertReadLines(String source, String... expected) {
        try (StringReader reader = new StringReader(source)) {
            char[] result;
            for (String exp : expected) {
                result = readLineToCharArray(reader);
                assertThat(result, equalTo(exp.toCharArray()));
            }
        }
    }

}
