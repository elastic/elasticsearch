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

import org.apache.commons.cli.CommandLine;
import org.elasticsearch.common.SuppressForbidden;

import java.io.BufferedReader;
import java.io.Console;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Locale;

/**
*
*/
@SuppressForbidden(reason = "System#out")
public abstract class Terminal {

    public static final Terminal DEFAULT = ConsoleTerminal.supported() ? new ConsoleTerminal() : new SystemTerminal();

    public static enum Verbosity {
        SILENT(0), NORMAL(1), VERBOSE(2);

        private final int level;

        private Verbosity(int level) {
            this.level = level;
        }

        public boolean enabled(Verbosity verbosity) {
            return level >= verbosity.level;
        }

        public static Verbosity resolve(CommandLine cli) {
            if (cli.hasOption("s")) {
                return SILENT;
            }
            if (cli.hasOption("v")) {
                return VERBOSE;
            }
            return NORMAL;
        }
    }

    private Verbosity verbosity = Verbosity.NORMAL;

    public Terminal() {
        this(Verbosity.NORMAL);
    }

    public Terminal(Verbosity verbosity) {
        this.verbosity = verbosity;
    }

    public void verbosity(Verbosity verbosity) {
        this.verbosity = verbosity;
    }

    public Verbosity verbosity() {
        return verbosity;
    }

    public abstract String readText(String text, Object... args);

    public abstract char[] readSecret(String text, Object... args);

    protected abstract void printStackTrace(Throwable t);

    public void println() {
        println(Verbosity.NORMAL);
    }

    public void println(String msg) {
        println(Verbosity.NORMAL, msg);
    }

    public void print(String msg) {
        print(Verbosity.NORMAL, msg);
    }

    public void println(Verbosity verbosity) {
        println(verbosity, "");
    }

    public void println(Verbosity verbosity, String msg) {
        print(verbosity, msg + System.lineSeparator());
    }

    public void print(Verbosity verbosity, String msg) {
        if (this.verbosity.enabled(verbosity)) {
            doPrint(msg);
        }
    }

    public void printError(String msg) {
        println(Verbosity.SILENT, "ERROR: " + msg);
    }

    public void printWarn(String msg) {
        println(Verbosity.SILENT, "WARN: " + msg);
    }

    protected abstract void doPrint(String msg);

    private static class ConsoleTerminal extends Terminal {

        final Console console = System.console();

        static boolean supported() {
            return System.console() != null;
        }

        @Override
        public void doPrint(String msg) {
            console.printf("%s", msg);
            console.flush();
        }

        @Override
        public String readText(String text, Object... args) {
            return console.readLine(text, args);
        }

        @Override
        public char[] readSecret(String text, Object... args) {
            return console.readPassword(text, args);
        }

        @Override
        public void printStackTrace(Throwable t) {
            t.printStackTrace(console.writer());
        }
    }

    @SuppressForbidden(reason = "System#out")
    private static class SystemTerminal extends Terminal {

        private final PrintWriter printWriter = new PrintWriter(System.out);

        @Override
        public void doPrint(String msg) {
            System.out.print(msg);
        }

        @Override
        public String readText(String text, Object... args) {
            print(text);
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            try {
                return reader.readLine();
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }

        @Override
        public char[] readSecret(String text, Object... args) {
            return readText(text, args).toCharArray();
        }

        @Override
        public void printStackTrace(Throwable t) {
            t.printStackTrace(printWriter);
        }
    }
}
