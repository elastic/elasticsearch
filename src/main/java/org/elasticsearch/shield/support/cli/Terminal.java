/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.support.cli;

import java.io.*;
import java.util.Locale;

/**
*
*/
public abstract class Terminal {

    public static final Terminal INSTANCE = ConsoleTerminal.supported() ? new ConsoleTerminal() : new SystemTerminal();

    public abstract String readText(String text, Object... args);

    public abstract char[] readSecret(String text, Object... args);

    public abstract void println();

    public abstract void println(String msg, Object... args);

    public abstract void print(String msg, Object... args);

    public abstract PrintWriter writer();

    public static abstract class Base extends Terminal {

        @Override
        public void println() {
            println("");
        }

        @Override
        public void println(String msg, Object... args) {
            print(msg + System.lineSeparator(), args);
        }

    }

    private static class ConsoleTerminal extends Base {

        final Console console = System.console();

        static boolean supported() {
            return System.console() != null;
        }

        private ConsoleTerminal() {
        }

        @Override
        public void print(String msg, Object... args) {
            console.printf(msg, args);
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
        public PrintWriter writer() {
            return console.writer();
        }

    }

    private static class SystemTerminal extends Base {

        private final PrintWriter printWriter = new PrintWriter(System.out);

        @Override
        public void print(String msg, Object... args) {
            System.out.print(String.format(Locale.ROOT, msg, args));
        }

        @Override
        public String readText(String text, Object... args) {
            print(text, args);
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
        public PrintWriter writer() {
            return printWriter;
        }
    }
}
