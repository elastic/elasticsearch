/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli;

import org.elasticsearch.xpack.sql.cli.net.client.HttpCliClient;
import org.elasticsearch.xpack.sql.cli.net.protocol.CommandResponse;
import org.elasticsearch.xpack.sql.cli.net.protocol.InfoResponse;
import org.elasticsearch.xpack.sql.net.client.util.StringUtils;
import org.jline.keymap.BindingReader;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.utils.InfoCmp.Capability;

import java.awt.Desktop;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Properties;

import static java.lang.String.format;

public class Cli {

    private final Terminal term;
    private final BindingReader bindingReader;
    private final Keys keys;
    private final CliConfiguration cfg;
    private final HttpCliClient cliClient;

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
        
        cfg = new CliConfiguration("localhost:9200", new Properties());
        cliClient = new HttpCliClient(cfg);
    }


    private void run() throws Exception {
        PrintWriter out = term.writer();

        LineReader reader = LineReaderBuilder.builder()
                .terminal(term)
                .completer(Completers.INSTANCE)
                .build();
        
        String DEFAULT_PROMPT = "sql> ";
        String MULTI_LINE_PROMPT = "   | ";

        StringBuilder multiLine = new StringBuilder();
        String prompt = DEFAULT_PROMPT;

        out.flush();

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
                out.println("Bye!");
                out.flush();
                return;
            }
            if (isClear(line)) {
                term.puts(Capability.clear_screen);
            }
            else if (isServerInfo(line)) {
                executeServerInfo(out);
            }
            else {
                executeCommand(line, out);
            }

            out.flush();
        }
    }

    private static boolean isClear(String line) {
        line = line.toLowerCase(Locale.ROOT);
        return (line.equals("cls"));
    }

    private boolean isServerInfo(String line) {
        line = line.toLowerCase(Locale.ROOT);
        return (line.equals("connect"));
    }

    private void executeServerInfo(PrintWriter out) {
        InfoResponse info = cliClient.serverInfo();
        out.println(format(Locale.ROOT, "Node:%s, Cluster:%s, Version:%s", info.node, info.cluster, info.versionString));
    }

    private static boolean isExit(String line) {
        line = line.toLowerCase(Locale.ROOT);
        return (line.equals("exit") || line.equals("quit"));
    }

    protected void executeCommand(String line, PrintWriter out) throws IOException {
        // remove trailing
        CommandResponse cmd = cliClient.command(line, null);

        String result = StringUtils.EMPTY;
        if (cmd.data != null) {
            result = cmd.data.toString();
            // TODO: handle graphviz
        }
        out.println(result);
    }

    private String displayGraphviz(String str) throws IOException {
        // save the content to a temp file
        Path dotTempFile = Files.createTempFile("sql-gv", ".dot2img");
        Files.write(dotTempFile, str.getBytes(StandardCharsets.UTF_8));
        // run graphviz on it (dot needs to be on the file path)
        Desktop desktop = Desktop.getDesktop();
        desktop.open(dotTempFile.toFile());
        return "";
    }
}