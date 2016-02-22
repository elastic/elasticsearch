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

package org.elasticsearch.bootstrap;

import org.elasticsearch.Build;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.cli.Terminal;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.OptionDef;
import org.kohsuke.args4j.spi.MapOptionHandler;
import org.kohsuke.args4j.spi.Setter;
import org.kohsuke.args4j.spi.SubCommand;
import org.kohsuke.args4j.spi.SubCommandHandler;
import org.kohsuke.args4j.spi.SubCommands;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * This class starts elasticsearch.
 */
public final class Elasticsearch {

    /** no instantiation */
    private Elasticsearch() {}

    /**
     * Main entry point for starting elasticsearch
     */
    public static void main(String[] args) {
        int status = main(args, Terminal.DEFAULT);
        exit(status);
    }

    // visible for testing
    static int main(String[] args, Terminal terminal) {
        Command command = parse(args);
        command.execute(terminal);
        return command.status;
    }

    // visible for testing
    static Command parse(String[] args) {
        Elasticsearch elasticsearch = new Elasticsearch();
        CmdLineParser parser = new CmdLineParser(elasticsearch);
        boolean help;
        int status = 0;
        String message = "";
        try {
            parser.parseArgument(args);
            help = elasticsearch.command.help;
        } catch (CmdLineException e) {
            help = true;
            status = 1;
            message = e.getMessage() + "\n";
        }

        if (help) {
            if ("version".equals(args[0])) {
                message += printHelp(new CmdLineParser(new VersionCommand()));
            } else if ("start".equals(args[0])) {
                message += printHelp(new CmdLineParser(new StartCommand()));
            } else {
                message += "command must be one of \"start\" or \"version\" but was \"" + args[0] + "\"";
            }
            HelpCommand command = new HelpCommand();
            command.message = message;
            command.status = status;
            return command;
        } else {
            return elasticsearch.command;
        }
    }

    private static String printHelp(CmdLineParser parser) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        parser.printUsage(stream);
        return new String(stream.toByteArray(), StandardCharsets.UTF_8);
    }

    @SuppressForbidden(reason = "Allowed to exit explicitly in bootstrap phase")
    private static void exit(int status) {
        System.exit(status);
    }

    /**
     * Required method that's called by Apache Commons procrun when
     * running as a service on Windows, when the service is stopped.
     *
     * http://commons.apache.org/proper/commons-daemon/procrun.html
     *
     * NOTE: If this method is renamed and/or moved, make sure to update service.bat!
     */
    static void close(String[] args) throws IOException {
        Bootstrap.stop();
    }

    @Argument(handler = SubCommandHandler.class, metaVar = "command")
    @SubCommands({
        @SubCommand(name = "start", impl = StartCommand.class),
        @SubCommand(name = "version", impl = VersionCommand.class)
    })
    Command command;

    static abstract class Command {

        @Option(name = "-h", aliases = "--help", usage = "print this message")
        boolean help;

        int status;

        abstract void execute(Terminal terminal);
    }

    public static class StartCommand extends Command {

        @Option(name = "-H", aliases = "--path.home", usage = "Elasticsearch path.home")
        String pathHome;

        @Option(name = "-d", aliases = "--daemonize", usage = "daemonize Elasticsearch")
        boolean daemonize;

        @Option(name = "-p", aliases = "--pidfile", usage = "pid file location")
        String pidFile;

        @Option(name = "-E", handler = EsMapOptionHandler.class, usage = "configure an Elasticsearch property")
        Map<String, String> properties = new HashMap<>();

        @Override
        void execute(Terminal terminal) {
            try {
                Bootstrap.init(daemonize, pathHome, pidFile);
            } catch (Throwable t) {
                // format exceptions to the console in a special way
                // to avoid 2MB stacktraces from guice, etc.
                throw new StartupError(t);
            }
        }

        public static class EsMapOptionHandler extends MapOptionHandler {

            public EsMapOptionHandler(CmdLineParser parser, OptionDef option, Setter<? super Map<?, ?>> setter) {
                super(parser, option, setter);
            }

            @Override
            protected void addToMap(String argument, Map m) throws CmdLineException {
                try {
                    super.addToMap(argument, m);
                } catch (CmdLineException e) {
                    throw new CmdLineException(this.owner, e.getMessage() + " for parameter [" + argument + "]", e);
                }
            }

            @SuppressForbidden(reason = "Sets system properties passed as CLI parameters")
            @Override
            protected void addToMap(Map m, String key, String value) {
                if (!key.startsWith("es.")) {
                    throw new IllegalArgumentException("Elasticsearch properties must be prefixed with \"es.\" but was \"" + key + "\"");
                }
                System.setProperty(key, value);
                super.addToMap(m, key, value);
            }

        }

    }

    public static class VersionCommand extends Command {

        @Override
        void execute(Terminal terminal) {
            terminal.println("Version: " + org.elasticsearch.Version.CURRENT
                + ", Build: " + Build.CURRENT.shortHash() + "/" + Build.CURRENT.date()
                + ", JVM: " + JvmInfo.jvmInfo().version());
        }

    }

    public static class HelpCommand extends Command {

        String message;

        @Override
        void execute(Terminal terminal) {
            terminal.println(message);
        }

    }
}
