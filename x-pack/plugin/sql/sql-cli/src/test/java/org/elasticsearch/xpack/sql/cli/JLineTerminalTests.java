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
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.terminal.Terminal;
import org.jline.terminal.impl.DumbTerminal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

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

    /**
     * A dumb terminal cannot hide input. The password must still be read, stay out of the history,
     * and leave no jLine thread behind that keeps writing the prompt.
     */
    public void testReadPasswordOnDumbTerminal() throws IOException, UserException {
        String password = randomAlphaOfLength(8);
        Terminal dumb = dumbTerminal(password + "\nSELECT 1;\n");
        LineReader dumbReader = LineReaderBuilder.builder().terminal(dumb).build();

        try (JLineTerminal terminal = new JLineTerminal(dumb, dumbReader, randomBoolean())) {
            assertEquals(password, terminal.readPassword("password: "));
            assertEquals(0, dumbReader.getHistory().size());
            assertFalse(maskThreadIsRunning());

            assertEquals("SELECT 1;", terminal.readLine("sql> "));
            assertEquals(1, dumbReader.getHistory().size());
        }
    }

    /**
     * A script written with CRLF line ends must read like one written with LF line ends.
     */
    public void testReadLineOnDumbTerminalIgnoresCarriageReturn() throws IOException {
        Terminal dumb = dumbTerminal("SELECT 1;\r\nquit;\r\n");
        LineReader dumbReader = LineReaderBuilder.builder().terminal(dumb).build();

        try (JLineTerminal terminal = new JLineTerminal(dumb, dumbReader, randomBoolean())) {
            assertEquals("SELECT 1;", terminal.readLine("sql> "));
            assertEquals("quit;", terminal.readLine("sql> "));
            assertNull(terminal.readLine("sql> "));
        }
    }

    /**
     * The terminal the CLI gets when its input does not come from a keyboard.
     */
    private Terminal dumbTerminal(String input) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
        String type = randomBoolean() ? Terminal.TYPE_DUMB : Terminal.TYPE_DUMB_COLOR;
        return new DumbTerminal("test", type, in, new ByteArrayOutputStream(), StandardCharsets.UTF_8);
    }

    private static boolean maskThreadIsRunning() {
        // jLine starts its mask thread from the reading thread, so it lands in this thread's group
        ThreadGroup group = Thread.currentThread().getThreadGroup();
        Thread[] threads = new Thread[group.activeCount() + 8];
        int count = group.enumerate(threads);
        return Arrays.stream(threads, 0, count).anyMatch(t -> t.getName().equals("JLine Mask Thread"));
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
