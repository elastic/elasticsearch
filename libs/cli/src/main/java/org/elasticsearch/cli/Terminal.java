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
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Locale;

/**
 * A Terminal wraps access to reading input and writing output for a cli.
 *
 * The available methods are similar to those of {@link Console}, with the ability
 * to read either normal text or a password, and the ability to print a line
 * of text. Printing is also gated by the {@link Verbosity} of the terminal,
 * which allows {@link #println(Verbosity,String)} calls which act like a logger,
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
    private Verbosity verbosity = Verbosity.NORMAL;

    /** The newline used when calling println. */
    private final String lineSeparator;

    protected Terminal(String lineSeparator) {
        this.lineSeparator = lineSeparator;
    }

    /** Sets the verbosity of the terminal. */
    public void setVerbosity(Verbosity verbosity) {
        this.verbosity = verbosity;
    }

    /** Reads clear text from the terminal input. See {@link Console#readLine()}. */
    public abstract String readText(String prompt);

    /** Reads password text from the terminal input. See {@link Console#readPassword()}}. */
    public abstract char[] readSecret(String prompt);

    /** Read password text form terminal input up to a maximum length. */
    public char[] readSecret(String prompt, int maxLength) {
        char[] result = readSecret(prompt);
        if (result.length > maxLength) {
            Arrays.fill(result, '\0');
            throw new IllegalStateException("Secret exceeded maximum length of " + maxLength);
        }
        return result;
    }

    /** Returns a Writer which can be used to write to the terminal directly using standard output. */
    public abstract PrintWriter getWriter();

    /**
     * Returns an OutputStream which can be used to write to the terminal directly using standard output.
     * May return {@code null} if this Terminal is not capable of binary output
      */
    @Nullable
    public abstract OutputStream getOutputStream();

    /** Returns a Writer which can be used to write to the terminal directly using standard error. */
    public PrintWriter getErrorWriter() {
        return ERROR_WRITER;
    }

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
    private void print(Verbosity verbosity, String msg, boolean isError) {
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
        return this.verbosity.ordinal() >= verbosity.ordinal();
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
    public static char[] readLineToCharArray(Reader reader, int maxLength) {
        char[] buf = new char[maxLength + 2];
        try {
            int len = 0;
            int next;
            while ((next = reader.read()) != -1) {
                char nextChar = (char) next;
                if (nextChar == '\n') {
                    break;
                }
                if (len < buf.length) {
                    buf[len] = nextChar;
                }
                len++;
            }

            if (len > 0 && len < buf.length && buf[len-1] == '\r') {
                len--;
            }

            if (len > maxLength) {
                Arrays.fill(buf, '\0');
                throw new RuntimeException("Input exceeded maximum length of " + maxLength);
            }

            char[] shortResult = Arrays.copyOf(buf, len);
            Arrays.fill(buf, '\0');
            return shortResult;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void flush() {
        this.getWriter().flush();
        this.getErrorWriter().flush();
    }

    private static class ConsoleTerminal extends Terminal {

        private static final Console CONSOLE = System.console();

        ConsoleTerminal() {
            super(System.lineSeparator());
        }

        static boolean isSupported() {
            return CONSOLE != null;
        }

        @Override
        public PrintWriter getWriter() {
            return CONSOLE.writer();
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

        private static final PrintWriter WRITER = newWriter();

        private BufferedReader reader;

        SystemTerminal() {
            super(System.lineSeparator());
        }

        @SuppressForbidden(reason = "Writer for System.out")
        private static PrintWriter newWriter() {
            return new PrintWriter(System.out);
        }

        /** visible for testing */
        BufferedReader getReader() {
            if (reader == null) {
                reader = new BufferedReader(new InputStreamReader(System.in, Charset.defaultCharset()));
            }
            return reader;
        }

        @Override
        public PrintWriter getWriter() {
            return WRITER;
        }

        @Override
        public OutputStream getOutputStream() {
            return System.out;
        }

        @Override
        public String readText(String text) {
            getErrorWriter().print(text); // prompts should go to standard error to avoid mixing with list output
            try {
                final String line = getReader().readLine();
                if (line == null) {
                    throw new IllegalStateException("unable to read from standard input; is standard input open and a tty attached?");
                }
                return line;
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }

        @Override
        public char[] readSecret(String text) {
            return readText(text).toCharArray();
        }

        @Override
        public char[] readSecret(String text, int maxLength) {
            getErrorWriter().println(text);
            return readLineToCharArray(getReader(), maxLength);
        }
    }
}
