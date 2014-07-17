/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.support;

import org.apache.commons.cli.*;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.internal.InternalSettingsPreparer;

import java.io.*;
import java.util.Locale;

import static org.elasticsearch.common.settings.ImmutableSettings.Builder.EMPTY_SETTINGS;

/**
 *
 */
public abstract class CmdLineTool {

    protected enum ExitStatus {
        OK(0),
        USAGE(64),
        IO_ERROR(74),
        CODE_ERROR(70);

        private final int status;

        private ExitStatus(int status) {
            this.status = status;
        }
    }

    protected static final Terminal terminal = ConsoleTerminal.supported() ? new ConsoleTerminal() : new SystemTerminal();
    protected static final Environment env;
    protected static final Settings settings;

    static {
        Tuple<Settings, Environment> tuple = InternalSettingsPreparer.prepareSettings(EMPTY_SETTINGS, true);
        settings = tuple.v1();
        env = tuple.v2();
    }

    private final Options options;
    private final String cmd;
    private final HelpFormatter helpFormatter = new HelpFormatter();

    protected CmdLineTool(String cmd, OptionBuilder... options) {
        this.cmd = cmd;
        this.options = new Options();
        for (int i = 0; i < options.length; i++) {
            this.options.addOption(options[i].option);
        }
    }

    protected final void execute(String[] args) {
        CommandLineParser parser = new GnuParser();
        try {
            CommandLine cli = parser.parse(options, args);
            run(cli);
        } catch (ParseException pe) {
            printUsage();
        } catch (Exception e) {
            terminal.println("Error: %s", e.getMessage());
            printUsage();
        }

    }

    protected void printUsage() {
        helpFormatter.printUsage(terminal.printWriter(), HelpFormatter.DEFAULT_WIDTH, cmd, options);
    }

    protected void exit(ExitStatus status) {
        System.exit(status.status);
    }

    protected abstract void run(CommandLine cli) throws Exception;

    protected static OptionBuilder option(String shortName, String longName, String description) {
        return new OptionBuilder(shortName, longName, description);
    }

    protected static class OptionBuilder {

        private final Option option;

        private OptionBuilder(String shortName, String longName, String description) {
            option = new Option(shortName, description);
            option.setLongOpt(longName);
        }

        public OptionBuilder required(boolean required) {
            option.setRequired(required);
            return this;
        }

        public OptionBuilder hasArg(boolean hasArg) {
            if (hasArg) {
                option.setArgs(1);
            }
            return this;
        }

    }

    protected static abstract class Terminal {

        public abstract void print(String msg, Object... args);

        public void println(String msg, Object... args) {
            print(msg + System.lineSeparator(), args);
        }

        public abstract void print(Throwable t);

        public void newLine() {
            println("");
        }

        public abstract String readString(String msg, Object... args);

        public abstract char[] readPassword(String msg, Object... args);

        public abstract PrintWriter printWriter();
    }

    private static class ConsoleTerminal extends Terminal {

        final Console console = System.console();

        static boolean supported() {
            return System.console() != null;
        }

        @Override
        public void print(String msg, Object... args) {
            console.printf(msg, args);
            console.flush();
        }

        @Override
        public void print(Throwable t) {
            t.printStackTrace(console.writer());
            console.flush();
        }

        @Override
        public String readString(String msg, Object... args) {
            return console.readLine(msg, args);
        }

        @Override
        public char[] readPassword(String msg, Object... args) {
            return console.readPassword(msg, args);
        }

        @Override
        public PrintWriter printWriter() {
            return console.writer();
        }
    }

    private static class SystemTerminal extends Terminal {

        private final PrintWriter printWriter = new PrintWriter(System.out);

        @Override
        public void print(String msg, Object... args) {
            System.out.print(String.format(Locale.ROOT, msg, args));
        }

        @Override
        public void print(Throwable t) {
            t.printStackTrace(System.err);
        }

        @Override
        public String readString(String msg, Object... args) {
            print(msg, args);
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            try {
                return reader.readLine();
            } catch (IOException ioe) {
                System.err.println("Could not read input");
                ioe.printStackTrace();
                System.exit(1);
            }
            return null;
        }

        @Override
        public char[] readPassword(String msg, Object... args) {
            return readString(msg, args).toCharArray();
        }

        @Override
        public PrintWriter printWriter() {
            return printWriter;
        }
    }

}

