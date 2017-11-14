/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli;

import org.elasticsearch.xpack.sql.cli.net.protocol.Proto.ResponseType;
import org.elasticsearch.xpack.sql.cli.net.protocol.QueryResponse;
import org.elasticsearch.xpack.sql.client.shared.IOUtils;
import org.elasticsearch.xpack.sql.client.shared.StringUtils;
import org.elasticsearch.xpack.sql.client.shared.SuppressForbidden;
import org.elasticsearch.xpack.sql.protocol.shared.AbstractQueryInitRequest;
import org.elasticsearch.xpack.sql.protocol.shared.Response;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.InfoCmp.Capability;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;
import java.util.Properties;
import java.util.logging.LogManager;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.jline.utils.AttributedStyle.BOLD;
import static org.jline.utils.AttributedStyle.BRIGHT;
import static org.jline.utils.AttributedStyle.DEFAULT;
import static org.jline.utils.AttributedStyle.RED;
import static org.jline.utils.AttributedStyle.YELLOW;

public class Cli {
    public static void main(String... args) throws Exception {
        /* Initialize the logger from the a properties file we bundle. This makes sure
         * we get useful error messages from jLine. */
        LogManager.getLogManager().readConfiguration(Cli.class.getResourceAsStream("/logging.properties"));
        String hostAndPort = "localhost:9200";
        Properties properties = new Properties();
        String user = null;
        String password = null;
        if (args.length > 0) {
            hostAndPort = args[0];
            if (false == hostAndPort.contains("://")) {
                // Default to http
                hostAndPort = "http://" + hostAndPort;
            }
            URI parsed;
            try {
                parsed = new URI(hostAndPort);
            } catch (URISyntaxException e) {
                exit("Invalid connection configuration [" + hostAndPort + "]: " + e.getMessage(), 1);
                return;
            }
            if (false == "".equals(parsed.getPath())) {
                exit("Invalid connection configuration [" + hostAndPort + "]: Path not allowed", 1);
                return;
            }
            user = parsed.getUserInfo();
            if (user != null) {
                // NOCOMMIT just use a URI the whole time
                hostAndPort = parsed.getScheme() + "://" + parsed.getHost() + ":" + parsed.getPort();
                int colonIndex = user.indexOf(':');
                if (colonIndex >= 0) {
                    password = user.substring(colonIndex + 1);
                    user = user.substring(0, colonIndex);
                }
            }
        }
        try (Terminal term = TerminalBuilder.builder().build()) {
            try {
                if (user != null) {
                    if (password == null) {
                        term.writer().print("password: ");
                        term.writer().flush();
                        term.echo(false);
                        password = new BufferedReader(term.reader()).readLine();
                        term.echo(true);
                    }
                    properties.setProperty(CliConfiguration.AUTH_USER, user);
                    properties.setProperty(CliConfiguration.AUTH_PASS, password);
                }

                boolean debug = StringUtils.parseBoolean(System.getProperty("cli.debug", "false"));
                Cli console = new Cli(debug, new CliConfiguration(hostAndPort + "/_sql/cli", properties), term);
                console.run();
            } catch (FatalException e) {
                term.writer().println(e.getMessage());
            }
        }
    }

    private final boolean debug;
    private final Terminal term;
    private final CliHttpClient cliClient;
    private int fetchSize = AbstractQueryInitRequest.DEFAULT_FETCH_SIZE;
    private String fetchSeparator = "";

    Cli(boolean debug, CliConfiguration cfg, Terminal terminal) {
        this.debug = debug;
        term = terminal;
        cliClient = new CliHttpClient(cfg);
    }

    void run() throws IOException {
        PrintWriter out = term.writer();

        LineReader reader = LineReaderBuilder.builder()
                .terminal(term)
                .completer(Completers.INSTANCE)
                .build();
        
        String DEFAULT_PROMPT = new AttributedString("sql> ", DEFAULT.foreground(YELLOW)).toAnsi(term);
        String MULTI_LINE_PROMPT = new AttributedString("   | ", DEFAULT.foreground(YELLOW)).toAnsi(term);

        StringBuilder multiLine = new StringBuilder();
        String prompt = DEFAULT_PROMPT;

        out.flush();
        printLogo();

        while (true) {
            String line = null;
            try {
                line = reader.readLine(prompt);
            } catch (UserInterruptException ex) {
                // ignore
            } catch (EndOfFileException ex) {
                return;
            }

            if (line == null) {
                continue;
            }
            line = line.trim();

            if (!line.endsWith(";")) {
                multiLine.append(" ");
                multiLine.append(line);
                prompt = MULTI_LINE_PROMPT;
                continue;
            }

            line = line.substring(0, line.length() - 1);

            prompt = DEFAULT_PROMPT;
            if (multiLine.length() > 0) {
                // append the line without trailing ;
                multiLine.append(line);
                line = multiLine.toString().trim();
                multiLine.setLength(0);
            }

            // special case to handle exit
            if (isExit(line)) {
                out.println(new AttributedString("Bye!", DEFAULT.foreground(BRIGHT)).toAnsi(term));
                out.flush();
                return;
            }
            boolean wasLocal = handleLocalCommand(line);
            if (false == wasLocal) {
                try {
                    if (isServerInfo(line)) {
                        executeServerInfo();
                    } else {
                        executeQuery(line);
                    }
                } catch (RuntimeException e) {
                    handleExceptionWhileCommunicatingWithServer(e);
                }
                out.println();
            }

            out.flush();
        }
    }

    /**
     * Handle an exception while communication with the server. Extracted
     * into a method so that tests can bubble the failure. 
     */
    protected void handleExceptionWhileCommunicatingWithServer(RuntimeException e) {
        AttributedStringBuilder asb = new AttributedStringBuilder();
        asb.append("Communication error [", BOLD.foreground(RED));
        asb.append(e.getMessage(), DEFAULT.boldOff().italic().foreground(YELLOW));
        asb.append("]", BOLD.underlineOff().foreground(RED));
        term.writer().println(asb.toAnsi(term));
        if (debug) {
            e.printStackTrace(term.writer());
        }
    }

    private static String logo() {
        try (InputStream io = Cli.class.getResourceAsStream("/logo.txt")) {
            if (io == null) {
                throw new FatalException("Could not find logo!");
            }
            return IOUtils.asBytes(io).toString();
        } catch (IOException e) {
            throw new FatalException("Could not load logo!", e);
        }
    }

    private void printLogo() {
        term.puts(Capability.clear_screen);
        term.writer().println(logo());
        term.writer().println();
    }

    private static final Pattern LOGO_PATTERN = Pattern.compile("logo", Pattern.CASE_INSENSITIVE);
    private static final Pattern CLEAR_PATTERN = Pattern.compile("cls", Pattern.CASE_INSENSITIVE);
    private static final Pattern FETCH_SIZE_PATTERN = Pattern.compile("fetch(?: |_)size *= *(.+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern FETCH_SEPARATOR_PATTERN = Pattern.compile("fetch(?: |_)separator *= *\"(.+)\"", Pattern.CASE_INSENSITIVE);
    private boolean handleLocalCommand(String line) {
        Matcher m = LOGO_PATTERN.matcher(line);
        if (m.matches()) {
            printLogo();
            return true;
        }
        m = CLEAR_PATTERN.matcher(line);
        if (m.matches()) {
            term.puts(Capability.clear_screen);
            return true;
        }
        m = FETCH_SIZE_PATTERN.matcher(line);
        if (m.matches()) {
            int proposedFetchSize;
            try {
                proposedFetchSize = fetchSize = Integer.parseInt(m.group(1));
            } catch (NumberFormatException e) {
                AttributedStringBuilder asb = new AttributedStringBuilder();
                asb.append("Invalid fetch size [", BOLD.foreground(RED));
                asb.append(m.group(1), DEFAULT.boldOff().italic().foreground(YELLOW));
                asb.append("]", BOLD.underlineOff().foreground(RED));
                term.writer().println(asb.toAnsi(term));
                return true;
            }
            if (proposedFetchSize <= 0) {
                AttributedStringBuilder asb = new AttributedStringBuilder();
                asb.append("Invalid fetch size [", BOLD.foreground(RED));
                asb.append(m.group(1), DEFAULT.boldOff().italic().foreground(YELLOW));
                asb.append("]. Must be > 0.", BOLD.underlineOff().foreground(RED));
                term.writer().println(asb.toAnsi(term));
                return true;
            }
            this.fetchSize = proposedFetchSize;
            AttributedStringBuilder asb = new AttributedStringBuilder();
            asb.append("fetch size set to ", DEFAULT);
            asb.append(Integer.toString(fetchSize), DEFAULT.foreground(BRIGHT));
            term.writer().println(asb.toAnsi(term));
            return true;
        }
        m = FETCH_SEPARATOR_PATTERN.matcher(line);
        if (m.matches()) {
            fetchSeparator = m.group(1);
            AttributedStringBuilder asb = new AttributedStringBuilder();
            asb.append("fetch separator set to \"", DEFAULT);
            asb.append(fetchSeparator, DEFAULT.foreground(BRIGHT));
            asb.append("\"", DEFAULT);
            term.writer().println(asb.toAnsi(term));
            return true;
        }

        return false;
    }
    
    private boolean isServerInfo(String line) {
        line = line.toLowerCase(Locale.ROOT);
        return line.equals("info");
    }

    private void executeServerInfo() {
        term.writer().println(ResponseToString.toAnsi(cliClient.serverInfo()).toAnsi(term));
    }

    private static boolean isExit(String line) {
        line = line.toLowerCase(Locale.ROOT);
        return line.equals("exit") || line.equals("quit");
    }

    private void executeQuery(String line) throws IOException {
        Response response = cliClient.queryInit(line, fetchSize);
        while (true) {
            term.writer().print(ResponseToString.toAnsi(response).toAnsi(term));
            term.writer().flush();
            if (response.responseType() == ResponseType.ERROR || response.responseType() == ResponseType.EXCEPTION) {
                return;
            }
            QueryResponse queryResponse = (QueryResponse) response;
            if (queryResponse.cursor().length == 0) {
                return;
            }
            if (false == fetchSeparator.equals("")) {
                term.writer().println(fetchSeparator);
            }
            response = cliClient.nextPage(queryResponse.cursor());
        }
    }

    static class FatalException extends RuntimeException {
        FatalException(String message, Throwable cause) {
            super(message, cause);
        }

        FatalException(String message) {
            super(message);
        }
    }

    @SuppressForbidden(reason = "CLI application")
    private static void exit(String message, int code) {
        System.err.println(message);
        System.exit(code);
    }
}