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

import java.io.BufferedReader;
import java.io.Console;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.charset.Charset;

import org.elasticsearch.common.SuppressForbidden;

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

    /** The default terminal implementation, which will be a console if available, or stdout/stderr if not. */
    public static final Terminal DEFAULT = ConsoleTerminal.isSupported() ? new ConsoleTerminal() : new SystemTerminal();

    /** Defines the available verbosity levels of messages to be printed. */
    public enum Verbosity {
        SILENT, /* always printed */
        NORMAL, /* printed when no options are given to cli */
        VERBOSE /* printed only when cli is passed verbose option */
    }

    /** The current verbosity for the terminal, defaulting to {@link Verbosity#NORMAL}. */
    private Verbosity verbosity = Verbosity.NORMAL;

    /** Sets the verbosity of the terminal. */
    public void setVerbosity(Verbosity verbosity) {
        this.verbosity = verbosity;
    }

    /** Reads clear text from the terminal input. See {@link Console#readLine()}. */
    public abstract String readText(String prompt);

    /** Reads password text from the terminal input. See {@link Console#readPassword()}}. */
    public abstract char[] readSecret(String prompt);

    /** Returns a Writer which can be used to write to the terminal directly. */
    public abstract PrintWriter getWriter();

    /** Print a message directly to the terminal. */
    protected abstract void doPrint(String msg);

    /** Prints a line to the terminal at {@link Verbosity#NORMAL} verbosity level. */
    public final void println(String msg) {
        println(Verbosity.NORMAL, msg);
    }

    /** Prints a line to the terminal at {@code verbosity} level. */
    public final void println(Verbosity verbosity, String msg) {
        if (this.verbosity.ordinal() >= verbosity.ordinal()) {
            doPrint(msg + System.lineSeparator());
        }
    }

    private static class ConsoleTerminal extends Terminal {

        private static final Console console = System.console();

        static boolean isSupported() {
            return console != null;
        }

        @Override
        public PrintWriter getWriter() {
            return console.writer();
        }

        @Override
        public void doPrint(String msg) {
            console.printf("%s", msg);
            console.flush();
        }

        @Override
        public String readText(String prompt) {
            return console.readLine("%s", prompt);
        }

        @Override
        public char[] readSecret(String prompt) {
            return console.readPassword("%s", prompt);
        }
    }

    private static class SystemTerminal extends Terminal {

        private static final PrintWriter writer = new PrintWriter(System.out);

        @Override
        @SuppressForbidden(reason = "System#out")
        public void doPrint(String msg) {
            System.out.print(msg);
            System.out.flush();
        }

        @Override
        public PrintWriter getWriter() {
            return writer;
        }

        @Override
        public String readText(String text) {
            doPrint(text);
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in, Charset.defaultCharset()));
            try {
                return reader.readLine();
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }

        @Override
        public char[] readSecret(String text) {
            return readText(text).toCharArray();
        }
    }
}
