/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cli;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;

import java.io.Console;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Locale;

/**
 * A Terminal wraps access to reading input and writing output for a cli.
 *
 * The available methods are similar to those of {@link Console}, with the ability
 * to read either normal text or a password, and the ability to print a line
 * of text. Printing is also gated by the {@link Verbosity} of the terminal,
 * which allows {@link #println(Verbosity,CharSequence)} calls which act like a logger,
 * only actually printing if the verbosity level of the terminal is above
 * the verbosity of the message.
*/
public abstract class Terminal {

    // Writer to standard error - not supplied by the {@link Console} API, so we share with subclasses
    private static final PrintWriter ERROR_WRITER = newErrorWriter();

    /** The default terminal implementation, which will be a console if available, or stdout/stderr if not. */
    public static final Terminal DEFAULT = ConsoleTerminal.isSupported() ? new ConsoleTerminal() : new SystemTerminal();

    @SuppressForbidden(reason = "Writer for System.err")
    private static PrintWriter newErrorWriter() {
        return new PrintWriter(System.err, true);
    }

    /** Defines the available verbosity levels of messages to be printed. */
    public enum Verbosity {
        SILENT, /* always printed */
        NORMAL, /* printed when no options are given to cli */
        VERBOSE /* printed only when cli is passed verbose option */
    }

    private final Reader reader;
    private final PrintWriter outWriter;
    private final PrintWriter errWriter;
    private Verbosity currentVerbosity = Verbosity.NORMAL;

    /**
     * Constructs a terminal instance.
     *
     * @param reader A character-based reader over the input of this terminal
     * @param outWriter A character-based writer for the output of this terminal
     * @param errWriter A character-based writer for the error stream of this terminal
     */
    protected Terminal(Reader reader, PrintWriter outWriter, PrintWriter errWriter) {
        this.reader = reader;
        this.outWriter = outWriter;
        this.errWriter = errWriter;
    }

    /**
     * Sets the verbosity of the terminal.
     *
     * <p> Defaults to {@link Verbosity#NORMAL}.
     */
    public void setVerbosity(Verbosity verbosity) {
        this.currentVerbosity = verbosity;
    }

    /**
     * Return the current verbosity level of this terminal.
     */
    public Verbosity getVerbosity() {
        return currentVerbosity;
    }

    private char[] read(String prompt) {
        errWriter.print(prompt); // prompts should go to standard error to avoid mixing with list output
        errWriter.flush(); // flush to ensure it is seen
        final char[] line = readLineToCharArray(reader);
        if (line == null) {
            throw new IllegalStateException("unable to read from standard input; is standard input open and a tty attached?");
        }
        return line;
    }

    /** Reads clear text from the terminal input. See {@link Console#readLine()}. */
    public String readText(String prompt) {
        return new String(read(prompt));
    }

    /** Reads password text from the terminal input. See {@link Console#readPassword()}}. */
    public char[] readSecret(String prompt) {
        return read(prompt);
    }

    /** Returns a Reader which can be used to read directly from the terminal using standard input. */
    public final Reader getReader() {
        return reader;
    }

    /** Returns a Writer which can be used to write to the terminal directly using standard output. */
    public final PrintWriter getWriter() {
        return outWriter;
    }

    /** Returns a Writer which can be used to write to the terminal directly using standard error. */
    public final PrintWriter getErrorWriter() {
        return errWriter;
    }

    /**
     * Returns an InputStream which can be used to read from the terminal directly using standard input.
     *
     * <p> May return {@code null} if this Terminal is not capable of binary input.
     * This corresponds with the underlying stream of bytes read by {@link #reader}.
     */
    @Nullable
    public InputStream getInputStream() {
        return null;
    }

    /**
     * Returns an OutputStream which can be used to write to the terminal directly using standard output.
     *
     * <p> May return {@code null} if this Terminal is not capable of binary output.
     * This corresponds with the underlying stream of bytes written to by {@link #getWriter()}.
      */
    @Nullable
    public OutputStream getOutputStream() {
        return null;
    }

    /** Prints a line to the terminal at {@link Verbosity#NORMAL} verbosity level. */
    public final void println(CharSequence msg) {
        println(Verbosity.NORMAL, msg);
    }

    /** Prints a line to the terminal at {@code verbosity} level. */
    public final void println(Verbosity verbosity, CharSequence msg) {
        print(verbosity, outWriter, msg, true);
    }

    /** Prints message to the terminal's standard output at {@code verbosity} level, without a newline. */
    public final void print(Verbosity verbosity, String msg) {
        print(verbosity, outWriter, msg, false);
    }

    /**
     * Prints message to the terminal at {@code verbosity} level.
     *
     * Subclasses may override if the writers are not implemented.
     */
    protected void print(Verbosity verbosity, PrintWriter writer, CharSequence msg, boolean newline) {
        if (isPrintable(verbosity)) {
            if (newline) {
                writer.println(msg);
            } else {
                writer.print(msg);
            }
            writer.flush();
        }
    }

    /** Prints a line to the terminal's standard error at {@link Verbosity#NORMAL} verbosity level, without a newline. */
    public final void errorPrint(Verbosity verbosity, String msg) {
        print(verbosity, errWriter, msg, false);
    }

    /** Prints a line to the terminal's standard error at {@link Verbosity#NORMAL} verbosity level. */
    public final void errorPrintln(String msg) {
        errorPrintln(Verbosity.NORMAL, msg);
    }

    /** Prints a line to the terminal's standard error at {@code verbosity} level. */
    public final void errorPrintln(Verbosity verbosity, String msg) {
        print(verbosity, errWriter, msg, true);
    }

    /** Checks if is enough {@code verbosity} level to be printed */
    public final boolean isPrintable(Verbosity verbosity) {
        return this.currentVerbosity.ordinal() >= verbosity.ordinal();
    }

    /**
     * Prompt for a yes or no answer from the user. This method will loop until 'y' or 'n'
     * (or the default empty value) is entered.
     */
    public final boolean promptYesNo(String prompt, boolean defaultYes) {
        String answerPrompt = defaultYes ? " [Y/n]" : " [y/N]";
        while (true) {
            String answer = readText(prompt + answerPrompt);
            if (answer == null || answer.isEmpty()) {
                return defaultYes;
            }
            answer = answer.toLowerCase(Locale.ROOT);
            boolean answerYes = answer.equals("y");
            if (answerYes == false && answer.equals("n") == false) {
                errorPrintln("Did not understand answer '" + answer + "'");
                continue;
            }
            return answerYes;
        }
    }

    /**
     * Read from the reader until we find a newline. If that newline
     * character is immediately preceded by a carriage return, we have
     * a Windows-style newline, so we discard the carriage return as well
     * as the newline.
     */
    public static char[] readLineToCharArray(Reader reader) {
        char[] buf = new char[128];
        try {
            int len = 0;
            int next;
            while ((next = reader.read()) != -1) {
                char nextChar = (char) next;
                if (nextChar == '\n') {
                    break;
                }
                if (len >= buf.length) {
                    char[] newbuf = new char[buf.length * 2];
                    System.arraycopy(buf, 0, newbuf, 0, buf.length);
                    Arrays.fill(buf, '\0');
                    buf = newbuf;
                }
                buf[len++] = nextChar;
            }
            if (len == 0 && next == -1) {
                return null;
            }

            if (len > 0 && len <= buf.length && buf[len - 1] == '\r') {
                len--;
            }

            char[] shortResult = Arrays.copyOf(buf, len);
            Arrays.fill(buf, '\0');
            return shortResult;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Flush the outputs of this terminal.
     */
    public final void flush() {
        outWriter.flush();
        errWriter.flush();
    }

    /**
     * Indicates whether this terminal is for a headless system i.e. is not interactive. If an instances answers
     * {@code false}, interactive operations can be attempted, but it is not guaranteed that they will succeed.
     *
     * @return if this terminal is headless.
     */
    public boolean isHeadless() {
        return false;
    }

    private static class ConsoleTerminal extends Terminal {

        private static final Console CONSOLE = System.console();

        ConsoleTerminal() {
            super(CONSOLE.reader(), CONSOLE.writer(), ERROR_WRITER);
        }

        static boolean isSupported() {
            return CONSOLE != null;
        }

        @Override
        public String readText(String prompt) {
            return CONSOLE.readLine("%s", prompt);
        }

        @Override
        public char[] readSecret(String prompt) {
            return CONSOLE.readPassword("%s", prompt);
        }
    }

    /** visible for testing */
    @SuppressForbidden(reason = "Access streams for construction")
    static class SystemTerminal extends Terminal {
        SystemTerminal() {
            super(
                // TODO: InputStreamReader can advance stdin past what it decodes. We need a way to buffer this and put it back
                // at the end of each character based read, so that switching to using getInputStream() returns binary data
                // right after the last character based input (newline)
                new InputStreamReader(System.in, Charset.defaultCharset()),
                new PrintWriter(System.out, true),
                ERROR_WRITER
            );
        }

        @Override
        public InputStream getInputStream() {
            return System.in;
        }

        @Override
        public OutputStream getOutputStream() {
            return System.out;
        }
    }
}
