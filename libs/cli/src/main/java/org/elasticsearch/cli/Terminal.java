/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cli;

import org.elasticsearch.core.Nullable;

import java.io.BufferedReader;
import java.io.Console;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Reader;
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

    /** Writer to standard error - not supplied by the {@link Console} API, so we share with subclasses */
    private static final PrintWriter ERROR_WRITER = newErrorWriter();

    /** The default terminal implementation, which will be a console if available, or stdout/stderr if not. */
    public static final Terminal DEFAULT = ConsoleTerminal.isSupported() ? new ConsoleTerminal() : new SystemTerminal();

    @SuppressForbidden(reason = "Writer for System.err")
    private static PrintWriter newErrorWriter() {
        return new PrintWriter(System.err);
    }

    /** Defines the available verbosity levels of messages to be printed. */
    public enum Verbosity {
        SILENT, /* always printed */
        NORMAL, /* printed when no options are given to cli */
        VERBOSE /* printed only when cli is passed verbose option */
    }

    /** The current verbosity for the terminal, defaulting to {@link Verbosity#NORMAL}. */
    private Verbosity currentVerbosity = Verbosity.NORMAL;

    private final BufferedReader reader;

    private final PrintWriter outWriter;

    private final PrintWriter errWriter;

    /** The newline used when calling println. */
    private final String lineSeparator;

    protected Terminal(Reader reader, PrintWriter outWriter, PrintWriter errWriter, String lineSeparator) {
        this.reader = reader == null ? null : new BufferedReader(reader);
        this.outWriter = outWriter;
        this.errWriter = errWriter;
        this.lineSeparator = lineSeparator;
    }

    /** Sets the verbosity of the terminal. */
    public void setVerbosity(Verbosity verbosity) {
        this.currentVerbosity = verbosity;
    }

    /** Reads clear text from the terminal input. See {@link Console#readLine()}. */
    public String readText(String prompt) {
        errWriter.print(prompt); // prompts should go to standard error to avoid mixing with list output
        try {
            final String line = reader.readLine();
            if (line == null) {
                throw new IllegalStateException("unable to read from standard input; is standard input open and a tty attached?");
            }
            return line;
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    /** Reads password text from the terminal input. See {@link Console#readPassword()}}. */
    public char[] readSecret(String prompt) {
        return readText(prompt).toCharArray();
    }

    /** Returns the separate used for a new line when writing to either output or error writers. */
    public String getLineSeparator() {
        return lineSeparator;
    }

    /** Returns a Writer which can be used to write to the terminal directly using standard output. */
    public PrintWriter getWriter() {
        return outWriter;
    }

    /** Returns a Writer which can be used to write to the terminal directly using standard error. */
    public PrintWriter getErrorWriter() {
        return errWriter;
    }

    /** Returns an OutputStream for writing bytes directly, or null if not supported */
    @Nullable
    public abstract OutputStream getOutputStream();

    /** Prints a line to the terminal at {@link Verbosity#NORMAL} verbosity level. */
    public final void println(CharSequence msg) {
        println(Verbosity.NORMAL, msg);
    }

    /** Prints a line to the terminal at {@code verbosity} level. */
    public final void println(Verbosity verbosity, CharSequence msg) {
        print(verbosity, msg + lineSeparator);
    }

    /** Prints message to the terminal's standard output at {@code verbosity} level, without a newline. */
    public final void print(Verbosity verbosity, String msg) {
        print(verbosity, msg, false);
    }

    /** Prints message to the terminal at {@code verbosity} level, without a newline. */
    protected void print(Verbosity verbosity, String msg, boolean isError) {
        if (isPrintable(verbosity)) {
            PrintWriter writer = isError ? getErrorWriter() : getWriter();
            writer.print(msg);
            writer.flush();
        }
    }

    /** Prints a line to the terminal's standard error at {@link Verbosity#NORMAL} verbosity level, without a newline. */
    public final void errorPrint(Verbosity verbosity, String msg) {
        print(verbosity, msg, true);
    }

    /** Prints a line to the terminal's standard error at {@link Verbosity#NORMAL} verbosity level. */
    public final void errorPrintln(String msg) {
        errorPrintln(Verbosity.NORMAL, msg);
    }

    /** Prints a line to the terminal's standard error at {@code verbosity} level. */
    public final void errorPrintln(Verbosity verbosity, String msg) {
        errorPrint(verbosity, msg + lineSeparator);
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

            if (len > 0 && len < buf.length && buf[len - 1] == '\r') {
                len--;
            }

            char[] shortResult = Arrays.copyOf(buf, len);
            Arrays.fill(buf, '\0');
            return shortResult;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void flush() {
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
            super(CONSOLE.reader(), CONSOLE.writer(), ERROR_WRITER, System.lineSeparator());
        }

        static boolean isSupported() {
            return CONSOLE != null;
        }

        @Override
        public OutputStream getOutputStream() {
            return null;
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
    static class SystemTerminal extends Terminal {

        SystemTerminal() {
            super(newReader(), newWriter(), ERROR_WRITER, System.lineSeparator());
        }

        @SuppressForbidden(reason = "Writing to System.out")
        @Override
        public OutputStream getOutputStream() {
            return System.out;
        }

        @SuppressForbidden(reason = "Reader for System.in")
        private static Reader newReader() {
            return new InputStreamReader(System.in);
        }

        @SuppressForbidden(reason = "Writer for System.out")
        private static PrintWriter newWriter() {
            return new PrintWriter(System.out);
        }
    }
}
