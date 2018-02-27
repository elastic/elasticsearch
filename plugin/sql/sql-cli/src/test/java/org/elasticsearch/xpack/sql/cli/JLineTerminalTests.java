/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli;

import org.elasticsearch.test.ESTestCase;
import org.jline.reader.LineReader;
import org.jline.terminal.Terminal;
import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

public class JLineTerminalTests extends ESTestCase {
    public void testReadPasswordSuccess() throws IOException {
        Terminal wrapped = mock(Terminal.class);
        LineReader reader = mock(LineReader.class);

        try (JLineTerminal terminal = new JLineTerminal(wrapped, reader, randomBoolean())) {
            String prompt = randomAlphaOfLength(5);

            String expected = randomAlphaOfLength(5);
            when(reader.readLine(prompt, null, (char) 0, null)).thenReturn(expected);
            String actual = terminal.readPassword(prompt);

            assertEquals(expected, actual);
            verify(reader).readLine(prompt, null, (char) 0, null);
        }
    }

    public void testReadPasswordEof() throws IOException {
        Terminal wrapped = mock(Terminal.class);
        LineReader reader = mock(LineReader.class);

        try (JLineTerminal terminal = new JLineTerminal(wrapped, reader, randomBoolean())) {
            String prompt = randomAlphaOfLength(5);

            Exception e = expectThrows(FatalCliException.class, () -> terminal.readPassword(prompt));
            assertEquals("Error reading password, terminal is closed", e.getMessage());

            verify(reader).readLine(prompt, null, (char) 0, null);
        }
    }

    public void testDisableMatchBracket() throws IOException {
        Terminal wrapped = mock(Terminal.class);
        LineReader reader = mock(LineReader.class);
        new JLineTerminal(wrapped, reader, false).close();
        verify(reader).setVariable(LineReader.BLINK_MATCHING_PAREN, 0L);
    }
}
