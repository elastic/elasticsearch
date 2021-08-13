/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.cli;

import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.UserException;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.UserInterruptException;
import org.jline.terminal.Terminal;

import java.io.IOException;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class JLineTerminalTests extends SqlCliTestCase {
    private final Terminal wrapped = mock(Terminal.class);
    private final LineReader reader = mock(LineReader.class);

    public void testDisableMatchBracket() throws IOException {
        new JLineTerminal(wrapped, reader, false).close();
        verify(reader).setVariable(LineReader.BLINK_MATCHING_PAREN, 0L);
    }

    public void testReadPasswordSuccess() throws IOException, UserException {
        String prompt = randomAlphaOfLength(5);
        String expected = randomAlphaOfLength(5);
        when(reader.readLine(prompt, (char) 0)).thenReturn(expected);

        try (JLineTerminal terminal = new JLineTerminal(wrapped, reader, randomBoolean())) {
            String actual = terminal.readPassword(prompt);

            assertEquals(expected, actual);
        }
    }

    public void testReadPasswordNull() throws IOException {
        String prompt = randomAlphaOfLength(5);
        /*
         * jLine documents readLine as not being able to return null but
         * LineReader totally does sometimes. We should interpret that as
         * "user hit ctrl-d on the password prompt" because that is similar
         * to the situations where this comes up.
         */
        when(reader.readLine(prompt, (char) 0)).thenReturn(null);

        try (JLineTerminal terminal = new JLineTerminal(wrapped, reader, randomBoolean())) {
            UserException e = expectThrows(UserException.class, () -> terminal.readPassword(prompt));
            assertEquals(ExitCodes.NOPERM, e.exitCode);
            assertEquals("password required", e.getMessage());
        }
    }

    public void testReadPasswordInterrupted() throws IOException {
        String prompt = randomAlphaOfLength(5);
        when(reader.readLine(prompt, (char) 0)).thenThrow(new UserInterruptException(""));

        try (JLineTerminal terminal = new JLineTerminal(wrapped, reader, randomBoolean())) {
            UserException e = expectThrows(UserException.class, () -> terminal.readPassword(prompt));
            assertEquals(ExitCodes.NOPERM, e.exitCode);
            assertEquals("password required", e.getMessage());
        }
    }

    public void testReadPasswordClosed() throws IOException {
        String prompt = randomAlphaOfLength(5);
        when(reader.readLine(prompt, (char) 0)).thenThrow(new EndOfFileException(""));

        try (JLineTerminal terminal = new JLineTerminal(wrapped, reader, randomBoolean())) {
            UserException e = expectThrows(UserException.class, () -> terminal.readPassword(prompt));
            assertEquals(ExitCodes.NOPERM, e.exitCode);
            assertEquals("password required", e.getMessage());
        }
    }

    public void testReadLineSuccess() throws IOException {
        String prompt = randomAlphaOfLength(5);
        String expected = randomAlphaOfLength(5);
        when(reader.readLine(any(String.class))).thenReturn(expected);

        try (JLineTerminal terminal = new JLineTerminal(wrapped, reader, randomBoolean())) {
            String actual = terminal.readLine(prompt);

            assertEquals(expected, actual);
        }
    }

    public void testReadLineInterrupted() throws IOException {
        String prompt = randomAlphaOfLength(5);
        when(reader.readLine(any(String.class))).thenThrow(new UserInterruptException(""));

        try (JLineTerminal terminal = new JLineTerminal(wrapped, reader, randomBoolean())) {
            assertEquals("", terminal.readLine(prompt));
        }
    }

    public void testReadLineClosed() throws IOException {
        String prompt = randomAlphaOfLength(5);
        when(reader.readLine(any(String.class))).thenThrow(new EndOfFileException(""));

        try (JLineTerminal terminal = new JLineTerminal(wrapped, reader, randomBoolean())) {
            assertEquals(null, terminal.readLine(prompt));
        }
    }
}
