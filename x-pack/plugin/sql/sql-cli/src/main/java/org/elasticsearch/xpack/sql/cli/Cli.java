/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.cli;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.cli.terminal.Terminal;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.xpack.sql.cli.command.AllowPartialResultsCliCommand;
import org.elasticsearch.xpack.sql.cli.command.ClearScreenCliCommand;
import org.elasticsearch.xpack.sql.cli.command.CliCommand;
import org.elasticsearch.xpack.sql.cli.command.CliCommands;
import org.elasticsearch.xpack.sql.cli.command.CliSession;
import org.elasticsearch.xpack.sql.cli.command.FetchSeparatorCliCommand;
import org.elasticsearch.xpack.sql.cli.command.FetchSizeCliCommand;
import org.elasticsearch.xpack.sql.cli.command.LenientCliCommand;
import org.elasticsearch.xpack.sql.cli.command.PrintLogoCommand;
import org.elasticsearch.xpack.sql.cli.command.ProjectRoutingCliCommand;
import org.elasticsearch.xpack.sql.cli.command.ServerInfoCliCommand;
import org.elasticsearch.xpack.sql.cli.command.ServerQueryCliCommand;
import org.elasticsearch.xpack.sql.client.ClientException;
import org.elasticsearch.xpack.sql.client.ClientVersion;
import org.elasticsearch.xpack.sql.client.ConnectionConfiguration;
import org.elasticsearch.xpack.sql.client.HttpClient;
import org.jline.terminal.TerminalBuilder;
import org.jline.utils.InfoCmp;

import java.io.IOException;
import java.net.ConnectException;
import java.nio.charset.Charset;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.util.Arrays;
import java.util.List;
import java.util.logging.LogManager;

public class Cli extends Command {
    private final OptionSpec<String> keystoreLocation;
    private final OptionSpec<String> apiKeyOption;
    private final OptionSpec<Boolean> checkOption;
    private final OptionSpec<String> connectionString;
    private final OptionSpec<Boolean> binaryCommunication;

    /**
     * Use this VM Options to run in IntelliJ or Eclipse:
     * -Dorg.jline.terminal.type=xterm-256color
     * -Dorg.jline.terminal.jna=false
     * -Dorg.jline.terminal.jansi=false
     * -Dorg.jline.terminal.exec=false
     * -Dorg.jline.terminal.dumb=true
     */
    public static void main(String[] args) throws Exception {
        configureJLineLogging();
        silenceJnaDeprecationNotice();
        preferBundledTerminalDescriptions();
        final Cli cli = new Cli(
            new JLineTerminal(
                TerminalBuilder.builder()
                    .name("Elasticsearch SQL CLI")
                    // remove jansi since it has issues on Windows in closing terminals
                    // the CLI uses JNA anyway
                    .jansi(false)
                    // jline would otherwise replace the JVM's signal handling even while no line is being read
                    .nativeSignals(false)
                    // jline would otherwise move to stderr when stdout is redirected, leaving the redirected output empty
                    .systemOutput(TerminalBuilder.SystemOutput.SysOut)
                    // jline would otherwise follow the locale, and a C locale turns non-ASCII input and output into '?'
                    .stdinEncoding(Charset.defaultCharset())
                    .stdoutEncoding(Charset.defaultCharset())
                    .build(),
                true
            )
        );
        int status = cli.main(args, Terminal.DEFAULT, ProcessInfo.fromSystem());
        if (status != ExitCodes.OK) {
            exit(status);
        }
    }

    @SuppressForbidden(reason = "a system property is the only switch jline offers for this notice")
    private static void silenceJnaDeprecationNotice() {
        // the CLI ships the JNA provider on purpose, so the notice is nothing a user can act on
        System.setProperty(TerminalBuilder.PROP_DISABLE_DEPRECATED_PROVIDER_WARNING, "true");
    }

    /**
     * jLine asks the infocmp command for a terminal's description before using the one it bundles, and it reads
     * infocmp's backspace key in a way that breaks backspace on Windows shells that come with infocmp.
     */
    private static void preferBundledTerminalDescriptions() {
        // jLine swaps xterm-256color in for xterm under Cygwin and MSYS
        for (String type : Arrays.asList(TerminalBuilder.builder().computeType(), "xterm-256color")) {
            String bundled = InfoCmp.getDefaultInfoCmp(type);
            if (bundled != null) {
                InfoCmp.setLoadedInfoCmp(type, bundled);
            }
        }
    }

    private static void configureJLineLogging() {
        try {
            /* Initialize the logger from the a properties file we bundle. This makes sure
             * we get useful error messages from jLine. */
            LogManager.getLogManager().readConfiguration(Cli.class.getResourceAsStream("/logging.properties"));
        } catch (IOException ex) {
            throw new RuntimeException("cannot setup logging", ex);
        }
    }

    private final CliTerminal cliTerminal;

    /**
     * Build the CLI.
     */
    public Cli(CliTerminal cliTerminal) {
        super("Elasticsearch SQL CLI");

        this.cliTerminal = cliTerminal;
        parser.acceptsAll(Arrays.asList("d", "debug"), "Enable debug logging");
        this.binaryCommunication = parser.acceptsAll(
            Arrays.asList("b", "binary"),
            "Disable binary communication. " + "Enabled by default. Accepts 'true' or 'false' values."
        ).withRequiredArg().ofType(Boolean.class).defaultsTo(Boolean.parseBoolean(System.getProperty("binary", "true")));
        this.keystoreLocation = parser.acceptsAll(
            Arrays.asList("k", "keystore_location"),
            "Location of a keystore to use when setting up SSL. "
                + "If specified then the CLI will prompt for a keystore password. "
                + "If specified when the uri isn't https then an error is thrown."
        ).withRequiredArg().ofType(String.class);
        this.apiKeyOption = parser.acceptsAll(
            Arrays.asList("apikey"),
            "API key to use for authentication. "
                + "The API key should be in the encoded format (base64 of id:api_key). "
                + "Cannot be used together with basic authentication (user in the URI)."
        ).withRequiredArg().ofType(String.class);
        this.checkOption = parser.acceptsAll(Arrays.asList("c", "check"), "Enable initial connection check on startup")
            .withRequiredArg()
            .ofType(Boolean.class)
            .defaultsTo(Boolean.parseBoolean(System.getProperty("cli.check", "true")));
        this.connectionString = parser.nonOptions("uri");
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, ProcessInfo processInfo) throws Exception {
        boolean debug = options.has("d") || options.has("debug");
        boolean binary = binaryCommunication.value(options);
        boolean checkConnection = checkOption.value(options);
        List<String> args = connectionString.values(options);
        if (args.size() > 1) {
            throw new UserException(ExitCodes.USAGE, "expecting a single uri");
        }
        String uri = args.size() == 1 ? args.get(0) : null;
        args = keystoreLocation.values(options);
        if (args.size() > 1) {
            throw new UserException(ExitCodes.USAGE, "expecting a single keystore file");
        }
        String keystoreLocationValue = args.size() == 1 ? args.get(0) : null;
        args = apiKeyOption.values(options);
        if (args.size() > 1) {
            throw new UserException(ExitCodes.USAGE, "expecting a single API key");
        }
        String apiKey = args.size() == 1 ? args.get(0) : null;
        execute(uri, debug, binary, keystoreLocationValue, checkConnection, apiKey);
    }

    private void execute(String uri, boolean debug, boolean binary, String keystoreLocation, boolean checkConnection, String apiKey)
        throws Exception {
        CliCommand cliCommand = new CliCommands(
            new PrintLogoCommand(),
            new ClearScreenCliCommand(),
            new FetchSizeCliCommand(),
            new LenientCliCommand(),
            new AllowPartialResultsCliCommand(),
            new ProjectRoutingCliCommand(),
            new FetchSeparatorCliCommand(),
            new ServerInfoCliCommand(),
            new ServerQueryCliCommand()
        );
        try {
            ConnectionBuilder connectionBuilder = new ConnectionBuilder(cliTerminal);
            ConnectionConfiguration con = connectionBuilder.buildConnection(uri, keystoreLocation, binary, apiKey);
            CliSession cliSession = new CliSession(new HttpClient(con));
            cliSession.cfg().setDebug(debug);
            if (checkConnection) {
                checkConnection(cliSession, cliTerminal, con);
            }
            new CliRepl(cliTerminal, cliSession, cliCommand).execute();
        } finally {
            cliTerminal.close();
        }
    }

    private static void checkConnection(CliSession cliSession, CliTerminal cliTerminal, ConnectionConfiguration con) throws UserException {
        try {
            cliSession.checkConnection();
        } catch (ClientException ex) {
            if (cliSession.cfg().isDebug()) {
                cliTerminal.error("Client Exception", ex.getMessage());
                cliTerminal.println();
                cliTerminal.printStackTrace(ex);
                cliTerminal.flush();
            }
            if (ex.getCause() != null && ex.getCause() instanceof ConnectException) {
                // Most likely Elasticsearch is not running
                throw new UserException(
                    ExitCodes.IO_ERROR,
                    "Cannot connect to the server " + con.connectionString() + " - " + ex.getCause().getMessage()
                );
            } else if (ex.getCause() != null && ex.getCause() instanceof SQLInvalidAuthorizationSpecException) {
                throw new UserException(
                    ExitCodes.NOPERM,
                    "Cannot establish a secure connection to the server " + con.connectionString() + " - " + ex.getCause().getMessage()
                );
            } else {
                // Most likely we connected to something other than Elasticsearch
                throw new UserException(
                    ExitCodes.DATA_ERROR,
                    "Cannot communicate with the server "
                        + con.connectionString()
                        + ". This version of CLI only works with Elasticsearch version "
                        + ClientVersion.CURRENT.toString()
                );
            }
        }
    }
}
