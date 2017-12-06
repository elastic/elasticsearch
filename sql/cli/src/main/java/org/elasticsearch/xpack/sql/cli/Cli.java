/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.xpack.sql.cli.command.ClearScreenCliCommand;
import org.elasticsearch.xpack.sql.cli.command.CliCommand;
import org.elasticsearch.xpack.sql.cli.command.CliCommands;
import org.elasticsearch.xpack.sql.cli.command.CliSession;
import org.elasticsearch.xpack.sql.cli.command.FetchSeparatorCliCommand;
import org.elasticsearch.xpack.sql.cli.command.FetchSizeCliCommand;
import org.elasticsearch.xpack.sql.cli.command.PrintLogoCommand;
import org.elasticsearch.xpack.sql.cli.command.ServerInfoCliCommand;
import org.elasticsearch.xpack.sql.cli.command.ServerQueryCliCommand;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.logging.LogManager;

public class Cli extends Command {
    private final OptionSpec<Boolean> debugOption;
    private final OptionSpec<String> connectionString;

    public Cli() {
        super("Elasticsearch SQL CLI", Cli::configureLogging);
        this.debugOption = parser.acceptsAll(Arrays.asList("d", "debug"),
                "Enable debug logging")
                .withRequiredArg().ofType(Boolean.class)
                .defaultsTo(Boolean.parseBoolean(System.getProperty("cli.debug", "false")));
        this.connectionString = parser.nonOptions("uri");
    }

    public static void main(String[] args) throws Exception {
        final Cli cli = new Cli();
        int status = cli.main(args, Terminal.DEFAULT);
        if (status != ExitCodes.OK) {
            exit(status);
        }
    }

    private static void configureLogging() {
        try {
            /* Initialize the logger from the a properties file we bundle. This makes sure
             * we get useful error messages from jLine. */
            LogManager.getLogManager().readConfiguration(Cli.class.getResourceAsStream("/logging.properties"));
        } catch (IOException ex) {
            throw new RuntimeException("cannot setup logging", ex);
        }
    }

    @Override
    protected void execute(org.elasticsearch.cli.Terminal terminal, OptionSet options) throws Exception {
        boolean debug = debugOption.value(options);
        List<String> args = connectionString.values(options);
        if (args.size() > 1) {
            throw new UserException(ExitCodes.USAGE, "expecting a single uri");
        }
        execute(args.size() == 1 ? args.get(0) : null, debug);
    }

    private void execute(String uri, boolean debug) throws Exception {
        CliCommand cliCommand = new CliCommands(
                new PrintLogoCommand(),
                new ClearScreenCliCommand(),
                new FetchSizeCliCommand(),
                new FetchSeparatorCliCommand(),
                new ServerInfoCliCommand(),
                new ServerQueryCliCommand()
        );
        try (CliTerminal cliTerminal = new JLineTerminal()) {
            ConnectionBuilder connectionBuilder = new ConnectionBuilder(cliTerminal);
            CliSession cliSession = new CliSession(new CliHttpClient(connectionBuilder.buildConnection(uri)));
            cliSession.setDebug(debug);
            new CliRepl(cliTerminal, cliSession, cliCommand).execute();
        }
    }
}
