/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.Locale;
import java.util.Properties;

import org.elasticsearch.xpack.sql.cli.net.client.CliHttpClient;
import org.elasticsearch.xpack.sql.net.client.util.IOUtils;
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

import static org.jline.utils.AttributedStyle.BOLD;
import static org.jline.utils.AttributedStyle.BRIGHT;
import static org.jline.utils.AttributedStyle.DEFAULT;
import static org.jline.utils.AttributedStyle.RED;
import static org.jline.utils.AttributedStyle.YELLOW;

public class Cli {

    private final Terminal term;
    private final BindingReader bindingReader;
    private final Keys keys;
    private final CliConfiguration cfg;
    private final CliHttpClient cliClient;

    public static void main(String... args) throws Exception {
        try (Terminal term = TerminalBuilder.builder().build()) {
            Cli console = new Cli(term);
            console.run();
        }
    }

    private Cli(Terminal terminal) {
        term = terminal;
        bindingReader = new BindingReader(term.reader());
        keys = new Keys(term);
        
        cfg = new CliConfiguration("localhost:9200/_cli", new Properties());
        cliClient = new CliHttpClient(cfg);
    }


    private void run() throws Exception {
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
        printLogo(out);

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
            //
            // local commands
            //

            // special case to handle exit
            if (isExit(line)) {
                out.println(new AttributedString("Bye!", DEFAULT.foreground(BRIGHT)).toAnsi(term));
                out.flush();
                return;
            }
            if (isClear(line)) {
                term.puts(Capability.clear_screen);
            }
            else if (isLogo(line)) {
                printLogo(out);
            }

            else {
                try {
                    if (isServerInfo(line)) {
                        executeServerInfo(out);
                    }
                    else {
                        executeCommand(line, out);
                    }
                } catch (RuntimeException ex) {
                    AttributedStringBuilder asb = new AttributedStringBuilder();
                    asb.append("Communication error [", BOLD.foreground(RED));
                    asb.append(ex.getMessage(), DEFAULT.boldOff().italic().foreground(YELLOW));
                    asb.append("]", BOLD.underlineOff().foreground(RED));
                    out.println(asb.toAnsi(term));
                }
                out.println();
            }

            out.flush();
        }
    }

    private static String logo() {
        try (InputStream io = Cli.class.getResourceAsStream("logo.txt")) {
            if (io != null) {
                return IOUtils.asBytes(io).toString();
            }
        } catch (IOException io) {
        }
        return "Could not load logo...";
    }

    private void printLogo(PrintWriter out) {
        term.puts(Capability.clear_screen);
        out.println(logo());
        out.println();
    }
    
    private static boolean isClear(String line) {
        line = line.toLowerCase(Locale.ROOT);
        return (line.equals("cls"));
    }

    private boolean isServerInfo(String line) {
        line = line.toLowerCase(Locale.ROOT);
        return (line.equals("info"));
    }

    private boolean isLogo(String line) {
        line = line.toLowerCase(Locale.ROOT);
        return (line.equals("logo"));
    }

    private void executeServerInfo(PrintWriter out) {
        out.println(ResponseToString.toAnsi(cliClient.serverInfo()).toAnsi(term));
    }

    private static boolean isExit(String line) {
        line = line.toLowerCase(Locale.ROOT);
        return (line.equals("exit") || line.equals("quit"));
    }

    protected void executeCommand(String line, PrintWriter out) throws IOException {
        out.print(ResponseToString.toAnsi(cliClient.command(line, null)).toAnsi(term));
    }
}