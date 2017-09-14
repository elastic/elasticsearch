/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli;

import org.elasticsearch.xpack.sql.cli.net.protocol.QueryResponse;
import org.elasticsearch.xpack.sql.net.client.SuppressForbidden;
import org.elasticsearch.xpack.sql.net.client.util.IOUtils;
import org.elasticsearch.xpack.sql.protocol.shared.AbstractQueryInitRequest;
import org.jline.keymap.BindingReader;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.InfoCmp.Capability;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
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
         * we get useful error messages. */
        LogManager.getLogManager().readConfiguration(Cli.class.getResourceAsStream("/logging.properties"));
        try (Terminal term = TerminalBuilder.builder().build()) {
            try {
                Cli console = new Cli(new CliConfiguration("localhost:9200/_sql/cli", new Properties()), term);
                console.run();
            } catch (FatalException e) {
                term.writer().println(e.getMessage());
                
            }
        }
    }

    @SuppressForbidden(reason = "CLI application")
    private static void terminateWithError() {
        System.exit(1);
    }

    private final Terminal term;
    private final BindingReader bindingReader;
    private final Keys keys;
    private final CliConfiguration cfg;
    private final CliHttpClient cliClient;
    private int fetchSize = AbstractQueryInitRequest.DEFAULT_FETCH_SIZE;
    private String fetchSeparator = "";

    Cli(CliConfiguration cfg, Terminal terminal) {
        term = terminal;
        // NOCOMMIT figure out if we need to build these for side effects or not. We don't currently use them.
        bindingReader = new BindingReader(term.reader());
        keys = new Keys(term);
        
        this.cfg = cfg;
        cliClient = new CliHttpClient(cfg);
    }

    void run() throws IOException {
        PrintWriter out = term.writer();

        LineReader reader = LineReaderBuilder.builder()
                .terminal(term)
                .completer(Completers.INSTANCE)
                .build();
        
        String prompt = null;

        String DEFAULT_PROMPT = new AttributedString("sql> ", DEFAULT.foreground(YELLOW)).toAnsi(term);
        String MULTI_LINE_PROMPT = new AttributedString("   | ", DEFAULT.foreground(YELLOW)).toAnsi(term);

        StringBuilder multiLine = new StringBuilder();
        prompt = DEFAULT_PROMPT;

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
        QueryResponse response = cliClient.queryInit(line, fetchSize);
        while (true) {
            term.writer().print(ResponseToString.toAnsi(response).toAnsi(term));
            term.writer().flush();
            if (response.cursor().length == 0) {
                return;
            }
            if (false == fetchSeparator.equals("")) {
                term.writer().println(fetchSeparator);
            }
            response = cliClient.nextPage(response.cursor());
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
}